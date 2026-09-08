/*
** Karpenter Provider OCI
**
** Copyright (c) 2026 Oracle and/or its affiliates.
** Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
 */

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	autoscalingv1beta1 "sigs.k8s.io/karpenter/pkg/apis/autoscaling/v1beta1"
	karpenterv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	ociv1beta1 "github.com/oracle/karpenter-provider-oci/pkg/apis/v1beta1"
)

func capacityBuffer(config *KarpenterE2ETestConfig) *autoscalingv1beta1.CapacityBuffer {
	strategy := autoscalingv1beta1.ActiveProvisioningStrategy
	replicas := config.CapacityBufferTest.BufferReplicas
	return &autoscalingv1beta1.CapacityBuffer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      config.CapacityBufferTest.Name,
			Namespace: config.Namespace,
		},
		Spec: autoscalingv1beta1.CapacityBufferSpec{
			ProvisioningStrategy: &strategy,
			ScalableRef: &autoscalingv1beta1.ScalableRef{
				APIGroup: "apps",
				Kind:     "Deployment",
				Name:     config.TestDeployment.Name,
			},
			Replicas: &replicas,
		},
	}
}

func (s *E2ETestSuite) createCapacityBuffer() error {
	buffer := capacityBuffer(s.testConfig)
	s.t.Logf("Creating CapacityBuffer %s/%s", buffer.Namespace, buffer.Name)
	return s.ctrlClient.Create(s.ctx, buffer)
}

func (s *E2ETestSuite) deleteCapacityBuffer() error {
	key := client.ObjectKey{
		Name:      s.testConfig.CapacityBufferTest.Name,
		Namespace: s.testConfig.Namespace,
	}
	buffer := &autoscalingv1beta1.CapacityBuffer{}
	if err := s.ctrlClient.Get(s.ctx, key, buffer); err != nil {
		return err
	}

	s.t.Logf("Deleting CapacityBuffer %s/%s", key.Namespace, key.Name)
	if err := s.ctrlClient.Delete(s.ctx, buffer); err != nil {
		return err
	}
	return wait.PollUntilContextTimeout(s.ctx, PollInterval, ResourceCreationTimeout, true,
		func(context.Context) (bool, error) {
			err := s.ctrlClient.Get(s.ctx, key, &autoscalingv1beta1.CapacityBuffer{})
			return apierrors.IsNotFound(err), client.IgnoreNotFound(err)
		})
}

func (s *E2ETestSuite) scaleTestDeployment(replicas int32) error {
	deployment := &appsv1.Deployment{}
	key := client.ObjectKey{Name: s.testConfig.TestDeployment.Name, Namespace: s.testConfig.Namespace}
	if err := s.ctrlClient.Get(s.ctx, key, deployment); err != nil {
		return err
	}
	if deployment.Spec.Replicas != nil && *deployment.Spec.Replicas == replicas {
		return nil
	}
	deployment.Spec.Replicas = &replicas
	return s.ctrlClient.Update(s.ctx, deployment)
}

func (s *E2ETestSuite) waitForCapacityBufferCondition(
	conditionType string,
	expectedStatus metav1.ConditionStatus,
	transitionedAfter time.Time,
) (*metav1.Condition, error) {
	key := client.ObjectKey{
		Name:      s.testConfig.CapacityBufferTest.Name,
		Namespace: s.testConfig.Namespace,
	}
	var matched metav1.Condition
	err := wait.PollUntilContextTimeout(s.ctx, PollInterval, NodeProvisionTimeout, true,
		func(context.Context) (bool, error) {
			buffer := &autoscalingv1beta1.CapacityBuffer{}
			if err := s.ctrlClient.Get(s.ctx, key, buffer); err != nil {
				return false, err
			}
			for _, condition := range buffer.Status.Conditions {
				if condition.Type != conditionType {
					continue
				}
				s.t.Logf("CapacityBuffer %s condition=%s status=%s reason=%s message=%q",
					buffer.Name, condition.Type, condition.Status, condition.Reason, condition.Message)
				if condition.ObservedGeneration != buffer.Generation || condition.Status != expectedStatus {
					return false, nil
				}
				if !transitionedAfter.IsZero() && !condition.LastTransitionTime.Time.After(transitionedAfter) {
					return false, nil
				}
				matched = condition
				return true, nil
			}
			return false, nil
		})
	if err != nil {
		return nil, err
	}
	return &matched, nil
}

func (s *E2ETestSuite) waitForCapacityBufferReplicas(replicas int32) error {
	key := client.ObjectKey{
		Name:      s.testConfig.CapacityBufferTest.Name,
		Namespace: s.testConfig.Namespace,
	}
	return wait.PollUntilContextTimeout(s.ctx, PollInterval, ResourceCreationTimeout, true,
		func(context.Context) (bool, error) {
			buffer := &autoscalingv1beta1.CapacityBuffer{}
			if err := s.ctrlClient.Get(s.ctx, key, buffer); err != nil {
				return false, err
			}
			return buffer.Status.Replicas != nil && *buffer.Status.Replicas == replicas, nil
		})
}

func (s *E2ETestSuite) waitForReadyNodeCountGreaterThan(count int) (int, error) {
	readyNodeCount := 0
	err := wait.PollUntilContextTimeout(s.ctx, PollInterval, NodeProvisionTimeout, true,
		func(context.Context) (bool, error) {
			nodes, err := s.getReadyNodes(client.MatchingLabels{NodePoolLabel: s.testConfig.NodePool.Name})
			if err != nil {
				return false, err
			}
			readyNodeCount = len(nodes)
			return readyNodeCount > count, nil
		})
	return readyNodeCount, err
}

func (s *E2ETestSuite) waitForNoTestNodeClaims() error {
	return wait.PollUntilContextTimeout(s.ctx, PollInterval, TestTimeout, true,
		func(context.Context) (bool, error) {
			claims := &karpenterv1.NodeClaimList{}
			if err := s.ctrlClient.List(s.ctx, claims,
				client.MatchingLabels{NodePoolLabel: s.testConfig.NodePool.Name}); err != nil {
				return false, err
			}
			return len(claims.Items) == 0, nil
		})
}

func (s *E2ETestSuite) restoreNodePoolRequirements(requirements []karpenterv1.NodeSelectorRequirementWithMinValues) error {
	nodePool := &karpenterv1.NodePool{}
	if err := s.ctrlClient.Get(s.ctx, client.ObjectKey{Name: s.testConfig.NodePool.Name}, nodePool); err != nil {
		return err
	}
	nodePool.Spec.Template.Spec.Requirements = requirements
	return s.ctrlClient.Update(s.ctx, nodePool)
}

func (s *E2ETestSuite) TestCapacityBuffer() {
	s.t.Run("CapacityBuffer", func(t *testing.T) {
		config := s.testConfig.CapacityBufferTest
		if config.Name == "" {
			t.Skip("CapacityBuffer test configuration is not set")
		}

		t.Logf("Start CapacityBuffer testing: name=%s instanceType=%s bufferReplicas=%d workloadReplicas=%d",
			config.Name, config.InstanceType, config.BufferReplicas, config.WorkloadReplicas)
		require.NotEmpty(t, config.InstanceType, "instance type must be set")
		require.Positive(t, config.BufferReplicas, "buffer replicas must be positive")
		require.Positive(t, config.WorkloadReplicas, "workload replicas must be positive")

		cleanupComplete := false
		var originalRequirements []karpenterv1.NodeSelectorRequirementWithMinValues
		t.Cleanup(func() {
			if cleanupComplete {
				return
			}
			if err := s.scaleTestDeployment(0); err != nil && !apierrors.IsNotFound(err) {
				t.Logf("Warning: failed to scale test deployment down during CapacityBuffer cleanup: %v", err)
			}
			if err := s.deleteCapacityBuffer(); err != nil && !apierrors.IsNotFound(err) {
				t.Logf("Warning: failed to delete CapacityBuffer during cleanup: %v", err)
			}
			if err := s.waitForNoTestNodeClaims(); err != nil {
				t.Logf("Warning: NodeClaims remained after CapacityBuffer cleanup: %v", err)
			}
			if originalRequirements != nil {
				if err := s.restoreNodePoolRequirements(originalRequirements); err != nil {
					t.Logf("Warning: failed to restore NodePool requirements after CapacityBuffer cleanup: %v", err)
				}
			}
		})

		require.NoError(t, s.scaleTestDeployment(0), "failed to scale the test deployment down")
		require.NoError(t, s.waitForNoTestNodeClaims(), "test NodeClaims should be absent before creating the buffer")

		nodePool := &karpenterv1.NodePool{}
		require.NoError(t, s.ctrlClient.Get(s.ctx, client.ObjectKey{Name: s.testConfig.NodePool.Name}, nodePool),
			"failed to get NodePool for CapacityBuffer test")
		originalRequirements = append(
			[]karpenterv1.NodeSelectorRequirementWithMinValues(nil), nodePool.Spec.Template.Spec.Requirements...)
		require.NoError(t, s.PatchNodePoolRequirements(
			nodePool,
			[]string{corev1.LabelInstanceTypeStable, ociv1beta1.OciInstanceShape},
			map[string][]string{corev1.LabelInstanceTypeStable: {config.InstanceType}},
		), "failed to constrain the CapacityBuffer test to %s", config.InstanceType)

		require.NoError(t, s.createCapacityBuffer(), "failed to create CapacityBuffer")

		_, err := s.waitForCapacityBufferCondition(
			autoscalingv1beta1.ReadyForProvisioningCondition, metav1.ConditionTrue, time.Time{})
		require.NoError(t, err, "CapacityBuffer should become ready for provisioning")
		require.NoError(t, s.waitForCapacityBufferReplicas(config.BufferReplicas),
			"CapacityBuffer should resolve the requested buffer replicas")

		initialNodeCount, err := s.waitForReadyNodeCountGreaterThan(0)
		require.NoError(t, err, "CapacityBuffer should proactively launch a KPO node")
		initialProvisioning, err := s.waitForCapacityBufferCondition(
			autoscalingv1beta1.ProvisioningCondition, metav1.ConditionTrue, time.Time{})
		require.NoError(t, err, "CapacityBuffer should report that its initial capacity is provisioned")
		require.Equal(t, "FitsExistingCapacity", initialProvisioning.Reason,
			"initial buffer capacity should fit on the provisioned KPO node")
		t.Logf("CapacityBuffer provisioned %d initial ready KPO node(s)", initialNodeCount)

		require.NoError(t, s.scaleTestDeployment(config.WorkloadReplicas), "failed to consume buffered capacity")
		refillPending, err := s.waitForCapacityBufferCondition(
			autoscalingv1beta1.ProvisioningCondition,
			metav1.ConditionFalse,
			initialProvisioning.LastTransitionTime.Time,
		)
		require.NoError(t, err, "CapacityBuffer should report that refill capacity is required")
		require.Equal(t, "RequiresNewCapacity", refillPending.Reason,
			"consuming the buffer should require new capacity")
		refilledNodeCount, err := s.waitForReadyNodeCountGreaterThan(initialNodeCount)
		require.NoError(t, err, "Karpenter should launch another node to refill consumed buffer capacity")
		require.NoError(t, s.checkPodsReady(s.testConfig.TestDeployment.Name, int(config.WorkloadReplicas)),
			"all workload pods should be running after consuming buffered capacity")
		refilledProvisioning, err := s.waitForCapacityBufferCondition(
			autoscalingv1beta1.ProvisioningCondition,
			metav1.ConditionTrue,
			refillPending.LastTransitionTime.Time,
		)
		require.NoError(t, err, "CapacityBuffer should return to Provisioning=True after refill")
		require.Equal(t, "FitsExistingCapacity", refilledProvisioning.Reason,
			"the replenished buffer should fit on the ready KPO nodes")
		t.Logf("CapacityBuffer refill successful: ready KPO nodes increased from %d to %d",
			initialNodeCount, refilledNodeCount)

		require.NoError(t, s.scaleTestDeployment(0), "failed to scale workload down after CapacityBuffer test")
		require.NoError(t, s.deleteCapacityBuffer(), "failed to delete CapacityBuffer")
		require.NoError(t, s.waitForNoTestNodeClaims(), "KPO nodes should terminate after deleting the CapacityBuffer")
		require.NoError(t, s.restoreNodePoolRequirements(originalRequirements),
			"failed to restore NodePool requirements after CapacityBuffer test")
		cleanupComplete = true
		t.Log("CapacityBuffer test successful")
	})
}
