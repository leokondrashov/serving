/*
Copyright 2019 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package net

import (
	"context"
	"strconv"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"

	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/ptr"
	"knative.dev/pkg/system"
)

const (
	// reservationLabelKey marks every Pod this mechanism owns, regardless of
	// which node it currently reserves -- used to list the full owned set
	// for garbage collection.
	reservationLabelKey = "serving.knative.dev/escrow-reservation"
	// reservationNodeLabelKey maps a listed reservation Pod back to the node
	// it reserves capacity on, without needing to parse the Pod name.
	reservationNodeLabelKey = "serving.knative.dev/escrow-reservation-node"

	// reservationImage is a minimal do-nothing container: the reservation
	// Pod exists only to hold its requests.cpu against the node.
	reservationImage = "registry.k8s.io/pause:3.9"
)

// reservationPodName is deterministic from both the node name and the
// desired core count -- NOT just the node name. A Pod's requests.cpu is
// immutable, and a real Delete doesn't remove the object synchronously (it
// lingers Terminating until the kubelet on that node acknowledges it, which
// can take up to its terminationGracePeriodSeconds). Encoding cores into the
// name means "go from 2 cores to 4 cores" creates a *different* object
// instead of racing the old one's deletion: Create either succeeds outright
// or, if a previous reconcile already got there, returns AlreadyExists --
// both are the correct end state, with no dependency on how fast the old
// (differently-named) Pod actually finishes terminating.
func reservationPodName(nodeName string, cores int32) string {
	return kmeta.ChildName("escrow-reservation", "-"+nodeName+"-"+strconv.Itoa(int(cores)))
}

// reconcileReservations idempotently syncs one placeholder Pod per node in
// desired (keyed by node name, valued by whole cores to reserve) so that
// kube-scheduler treats that CPU quantity as unavailable to any other pod on
// the node. A desired value of 0 means "reserve nothing on this node" (see
// desiredReservations) and any existing reservation Pod(s) there are
// removed. Nodes no longer present in desired at all (label removed, node
// deleted) are also garbage collected.
//
// This is called independently, at startup, by every activator replica --
// not by a single leader. It is idempotent and safe to race: the Pod name
// for a given (node, cores) pair is deterministic, so concurrent replicas
// converge on the same object rather than creating duplicates. Every
// mutation tolerates "someone else already got there first" as success.
//
// Failures are logged and swallowed, never fatal: the pre-existing in-memory
// fallback-dispatch throttling this activator performs does not depend on
// these Pods existing.
func reconcileReservations(ctx context.Context, clientset kubernetes.Interface, desired map[string]int32) {
	logger := logging.FromContext(ctx)
	ns := system.Namespace()
	pods := clientset.CoreV1().Pods(ns)

	selector := labels.SelectorFromSet(labels.Set{reservationLabelKey: "true"})
	existing, err := pods.List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		logger.Errorw("Error listing escrow reservation pods", "error", err)
		return
	}

	// Group by node -- there can legitimately be more than one Pod per node
	// at once transiently (e.g. an old value's Pod still Terminating after
	// a previous reconcile moved it to a new name).
	byNode := make(map[string][]corev1.Pod, len(existing.Items))
	for _, p := range existing.Items {
		node := p.Labels[reservationNodeLabelKey]
		byNode[node] = append(byNode[node], p)
	}

	for nodeName, cores := range desired {
		podsForNode := byNode[nodeName]
		delete(byNode, nodeName) // accounted for, whatever we do with it below

		if cores <= 0 {
			for _, p := range podsForNode {
				deleteReservationPod(ctx, pods, logger, p.Name)
			}
			continue
		}

		wantName := reservationPodName(nodeName, cores)
		haveWant := false
		for _, p := range podsForNode {
			if p.Name == wantName {
				haveWant = true
				continue
			}
			// Stale: reserves this node under a previous cores value (or a
			// duplicate). Delete it -- harmless if it's already Terminating.
			logger.Infof("Escrow reservation on node %s changed (pod %s no longer matches desired %d cores): deleting", nodeName, p.Name, cores)
			deleteReservationPod(ctx, pods, logger, p.Name)
		}
		if !haveWant {
			logger.Infof("Escrow reservation on node %s: creating %s requesting %d cores", nodeName, wantName, cores)
			createReservationPod(ctx, pods, logger, nodeName, cores)
		}
	}

	// Anything left in byNode belongs to a node no longer in the desired
	// set at all -- garbage collect it.
	for nodeName, podsForNode := range byNode {
		for _, p := range podsForNode {
			logger.Infof("Escrow reservation on node %s no longer desired: deleting %s", nodeName, p.Name)
			deleteReservationPod(ctx, pods, logger, p.Name)
		}
	}
}

func requestedCores(pod corev1.Pod) int32 {
	if len(pod.Spec.Containers) == 0 {
		return 0
	}
	q := pod.Spec.Containers[0].Resources.Requests.Cpu()
	return int32(q.Value())
}

func createReservationPod(ctx context.Context, pods corev1client.PodInterface, logger *zap.SugaredLogger, nodeName string, cores int32) {
	quantity := *resource.NewQuantity(int64(cores), resource.DecimalSI)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      reservationPodName(nodeName, cores),
			Namespace: system.Namespace(),
			Labels: map[string]string{
				reservationLabelKey:     "true",
				reservationNodeLabelKey: nodeName,
			},
		},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
			Containers: []corev1.Container{{
				Name:  "reservation",
				Image: reservationImage,
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: quantity},
					Limits:   corev1.ResourceList{corev1.ResourceCPU: quantity},
				},
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: ptr.Bool(false),
					ReadOnlyRootFilesystem:   ptr.Bool(true),
					RunAsNonRoot:             ptr.Bool(true),
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
					SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
				},
			}},
		},
	}

	if _, err := pods.Create(ctx, pod, metav1.CreateOptions{}); err != nil && !k8serrors.IsAlreadyExists(err) {
		logger.Errorw("Error creating escrow reservation pod", "node", nodeName, "error", err)
	}
}

func deleteReservationPod(ctx context.Context, pods corev1client.PodInterface, logger *zap.SugaredLogger, name string) {
	if err := pods.Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
		logger.Errorw("Error deleting escrow reservation pod", "pod", name, "error", err)
	}
}
