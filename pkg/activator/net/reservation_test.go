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
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	fakekubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	rtesting "knative.dev/pkg/reconciler/testing"
	"knative.dev/pkg/system"
)

func TestDesiredReservations(t *testing.T) {
	nodes := []nodeInfo{
		{name: "node-a", cores: 4},
		{name: "node-b", cores: 8},
	}

	tests := []struct {
		name     string
		cpuShare float64
		want     map[string]int32
	}{{
		name:     "unset/default 1.0 means reserve nothing",
		cpuShare: 1.0,
		want:     map[string]int32{"node-a": 0, "node-b": 0},
	}, {
		name:     "fractional share reserves a floor of each node's cores",
		cpuShare: 0.5,
		want:     map[string]int32{"node-a": 2, "node-b": 4},
	}, {
		name:     "zero share reserves nothing",
		cpuShare: 0,
		want:     map[string]int32{"node-a": 0, "node-b": 0},
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := desiredReservations(nodes, tc.cpuShare)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("desiredReservations() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestListWorkerNodesAndBuildNodeTrackers(t *testing.T) {
	ctx, cancel, _ := rtesting.SetupFakeContextWithCancel(t)
	defer cancel()

	fake := fakekubeclient.Get(ctx)
	nodeSpecs := []struct {
		name     string
		nodeType string
		cores    int64
		ip       string
	}{
		{name: "worker-1", nodeType: "worker", cores: 4, ip: "10.0.0.1"},
		{name: "worker-2", nodeType: "worker", cores: 8, ip: "10.0.0.2"},
		{name: "single-1", nodeType: "singlenode", cores: 2, ip: "10.0.0.3"},
		{name: "other", nodeType: "control-plane", cores: 16, ip: "10.0.0.4"},
	}
	for _, n := range nodeSpecs {
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   n.name,
				Labels: map[string]string{"loader-nodetype": n.nodeType},
			},
			Status: corev1.NodeStatus{
				Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: n.ip}},
				Allocatable: corev1.ResourceList{
					corev1.ResourceCPU: *resource.NewQuantity(n.cores, resource.DecimalSI),
				},
			},
		}
		if _, err := fake.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil {
			t.Fatalf("failed to create fake node %s: %v", n.name, err)
		}
	}

	nodes, err := listWorkerNodes(ctx, fake)
	if err != nil {
		t.Fatalf("listWorkerNodes() error = %v", err)
	}
	if len(nodes) != 3 {
		t.Fatalf("listWorkerNodes() returned %d nodes, want 3 (excluding non-worker labeled node): %+v", len(nodes), nodes)
	}

	var totalCores int64
	for _, n := range nodes {
		totalCores += n.cores
	}
	if totalCores != 14 {
		t.Errorf("total worker cores = %d, want 14", totalCores)
	}

	trackers := buildNodeTrackers(nodes, 0.5)
	var totalLimit int32
	for _, tr := range trackers {
		totalLimit += tr.limit
	}
	// floor(14 * 0.5) == 7, spread across 3 nodes.
	if totalLimit != 7 {
		t.Errorf("total nodeTracker limit = %d, want 7", totalLimit)
	}
}

func TestReconcileReservations(t *testing.T) {
	ctx, cancel, _ := rtesting.SetupFakeContextWithCancel(t)
	defer cancel()
	fake := fakekubeclient.Get(ctx)

	// Pre-seed a stale reservation (for a node no longer desired) and a
	// reservation with an out-of-date CPU value (for a node whose desired
	// value changed).
	mustCreatePod(t, ctx, fake, "stale-node", 3)
	mustCreatePod(t, ctx, fake, "changed-node", 1)

	desired := map[string]int32{
		"changed-node": 2, // exists with old value 1 -- must be recreated with 2.
		"new-node":     4, // doesn't exist yet -- must be created.
		"zero-node":    0, // desired zero, doesn't exist -- must stay absent.
	}

	reconcileReservations(ctx, fake, desired)

	pods, err := fake.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("failed to list pods: %v", err)
	}

	byNode := map[string]corev1.Pod{}
	for _, p := range pods.Items {
		byNode[p.Labels[reservationNodeLabelKey]] = p
	}

	if _, ok := byNode["stale-node"]; ok {
		t.Error("expected stale-node's reservation pod to be garbage collected")
	}
	if _, ok := byNode["zero-node"]; ok {
		t.Error("expected zero-node to have no reservation pod")
	}
	if pod, ok := byNode["new-node"]; !ok {
		t.Error("expected new-node to have a reservation pod created")
	} else if got := requestedCores(pod); got != 4 {
		t.Errorf("new-node reservation cpu = %d, want 4", got)
	}
	if pod, ok := byNode["changed-node"]; !ok {
		t.Error("expected changed-node to still have a reservation pod")
	} else if got := requestedCores(pod); got != 2 {
		t.Errorf("changed-node reservation cpu = %d, want 2 (recreated with new value)", got)
	}

	// Re-running with the same desired state should be a no-op (idempotent),
	// simulating a second activator replica racing the same reconcile.
	reconcileReservations(ctx, fake, desired)
	podsAgain, err := fake.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("failed to list pods: %v", err)
	}
	if len(podsAgain.Items) != len(pods.Items) {
		t.Errorf("re-reconciling with unchanged desired state changed pod count: got %d, want %d", len(podsAgain.Items), len(pods.Items))
	}
}

func mustCreatePod(t *testing.T, ctx context.Context, fake kubernetes.Interface, nodeName string, cores int32) {
	t.Helper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "escrow-reservation-" + nodeName,
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
					Requests: corev1.ResourceList{
						corev1.ResourceCPU: *resource.NewQuantity(int64(cores), resource.DecimalSI),
					},
				},
			}},
		},
	}
	if _, err := fake.CoreV1().Pods(system.Namespace()).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to pre-seed reservation pod for %s: %v", nodeName, err)
	}
}
