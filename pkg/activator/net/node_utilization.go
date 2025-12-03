/*
Copyright 2025 The Knative Authors

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
	"net"
	"sync"
	"time"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	corev1listers "k8s.io/client-go/listers/core/v1"
	metricsclientset "k8s.io/metrics/pkg/client/clientset/versioned"
)

// NodeUtilizationTracker tracks the CPU utilization of nodes.
type NodeUtilizationTracker struct {
	nodeLister    corev1listers.NodeLister
	metricsClient metricsclientset.Interface
	logger        *zap.SugaredLogger

	mu sync.RWMutex
	// map node name -> cpu usage (0.0 to 1.0)
	nodeCPUUsage map[string]float64
}

// NewNodeUtilizationTracker creates a new NodeUtilizationTracker.
func NewNodeUtilizationTracker(nodeLister corev1listers.NodeLister, metricsClient metricsclientset.Interface, logger *zap.SugaredLogger) *NodeUtilizationTracker {
	t := &NodeUtilizationTracker{
		nodeLister:    nodeLister,
		metricsClient: metricsClient,
		logger:        logger,
		nodeCPUUsage:  make(map[string]float64),
	}
	go t.run()
	return t
}

func (t *NodeUtilizationTracker) run() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		t.updateMetrics()
	}
}

func (t *NodeUtilizationTracker) updateMetrics() {
	if t.metricsClient == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	metricsList, err := t.metricsClient.MetricsV1beta1().NodeMetricses().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.logger.Errorw("Failed to list node metrics", zap.Error(err))
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	for _, m := range metricsList.Items {
		nodeName := m.Name
		cpuUsage := m.Usage.Cpu().MilliValue()

		node, err := t.nodeLister.Get(nodeName)
		if err != nil {
			continue
		}
		cpuCap := node.Status.Allocatable.Cpu().MilliValue()
		if cpuCap == 0 {
			continue
		}

		utilization := float64(cpuUsage) / float64(cpuCap)
		t.nodeCPUUsage[nodeName] = utilization
	}
}

// GetUtilization returns the CPU utilization for the node hosting the given IP.
func (t *NodeUtilizationTracker) GetUtilization(ipPort string) float64 {
	host, _, err := net.SplitHostPort(ipPort)
	if err != nil {
		host = ipPort // Fallback if no port
	}

	nodeName := t.getNodeNameForIP(host)
	if nodeName == "" {
		return 1.0 // Penalize unknown nodes
	}

	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.nodeCPUUsage[nodeName]
}

func (t *NodeUtilizationTracker) getNodeNameForIP(ip string) string {
	nodes, err := t.nodeLister.List(labels.Everything())
	if err != nil {
		return ""
	}

	targetIP := net.ParseIP(ip)
	if targetIP == nil {
		return ""
	}

	for _, node := range nodes {
		if matchesCIDR(node.Spec.PodCIDR, targetIP) {
			return node.Name
		}
		for _, cidr := range node.Spec.PodCIDRs {
			if matchesCIDR(cidr, targetIP) {
				return node.Name
			}
		}
	}
	return ""
}

func matchesCIDR(cidrStr string, ip net.IP) bool {
	if cidrStr == "" {
		return false
	}
	_, cidr, err := net.ParseCIDR(cidrStr)
	if err != nil {
		return false
	}
	return cidr.Contains(ip)
}
