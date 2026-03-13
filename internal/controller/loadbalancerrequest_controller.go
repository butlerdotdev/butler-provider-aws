/*
Copyright 2026 The Butler Authors.

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

package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	elbv2 "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbv2types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	smithy "github.com/aws/smithy-go"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

const (
	lbrFinalizerName = "loadbalancerrequest.butler.butlerlabs.dev/aws-finalizer"
	lbrRequeueShort  = 10 * time.Second
	lbrRequeueLong   = 30 * time.Second

	// maxNLBNameLength is the AWS limit for NLB and target group names.
	maxNLBNameLength = 32
	// nlbSuffixLength is the length of the longest suffix we append ("-cp-nlb" = 7 chars).
	nlbSuffixLength = 7
	// maxClusterNameForNLB is the maximum cluster name length that fits within the NLB name limit.
	maxClusterNameForNLB = maxNLBNameLength - nlbSuffixLength // 25
)

// LoadBalancerRequestReconciler reconciles LoadBalancerRequest objects for AWS.
// It creates an AWS Network Load Balancer (NLB), target group, and listener,
// then registers backend instances as targets are added.
type LoadBalancerRequestReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=loadbalancerrequests,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=loadbalancerrequests/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=loadbalancerrequests/finalizers,verbs=update
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=providerconfigs,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile handles LoadBalancerRequest create/update/delete for AWS.
func (r *LoadBalancerRequestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	lbr := &butlerv1alpha1.LoadBalancerRequest{}
	if err := r.Get(ctx, req.NamespacedName, lbr); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	pc, err := r.getProviderConfig(ctx, lbr)
	if err != nil {
		return r.updateStatusError(ctx, lbr, butlerv1alpha1.ReasonProviderError, err.Error())
	}

	if pc.Spec.Provider != butlerv1alpha1.ProviderTypeAWS {
		log.V(1).Info("Skipping non-AWS LoadBalancerRequest")
		return ctrl.Result{}, nil
	}

	elbClient, err := r.createELBClient(ctx, pc)
	if err != nil {
		return r.updateStatusError(ctx, lbr, butlerv1alpha1.ReasonCredentialsInvalid, err.Error())
	}

	if !lbr.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, lbr, pc, elbClient)
	}

	if !controllerutil.ContainsFinalizer(lbr, lbrFinalizerName) {
		controllerutil.AddFinalizer(lbr, lbrFinalizerName)
		if err := r.Update(ctx, lbr); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	switch lbr.Status.Phase {
	case "", butlerv1alpha1.LoadBalancerPhasePending:
		return r.reconcileCreate(ctx, lbr, pc, elbClient)
	case butlerv1alpha1.LoadBalancerPhaseCreating:
		return r.reconcileCreate(ctx, lbr, pc, elbClient)
	case butlerv1alpha1.LoadBalancerPhaseReady:
		return r.reconcileTargets(ctx, lbr, pc, elbClient)
	case butlerv1alpha1.LoadBalancerPhaseFailed:
		return ctrl.Result{}, nil
	default:
		return r.updatePhase(ctx, lbr, butlerv1alpha1.LoadBalancerPhasePending)
	}
}

// reconcileCreate provisions the AWS NLB resources: load balancer, target group,
// and listener.
func (r *LoadBalancerRequestReconciler) reconcileCreate(ctx context.Context, lbr *butlerv1alpha1.LoadBalancerRequest, pc *butlerv1alpha1.ProviderConfig, elbClient *elbv2.Client) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	awsConfig := pc.Spec.AWS

	if awsConfig == nil {
		return r.updateStatusError(ctx, lbr, butlerv1alpha1.ReasonInvalidConfiguration,
			"ProviderConfig has no AWS configuration")
	}

	name := lbr.Spec.ClusterName
	port := lbr.Spec.Port
	if port == 0 {
		port = 6443
	}

	// Validate NLB name length. AWS NLB names are limited to 32 characters.
	// We append "-cp-nlb" (7 chars), so the cluster name must be at most 25 chars.
	if len(name)+nlbSuffixLength > maxNLBNameLength {
		return r.updateStatusError(ctx, lbr, butlerv1alpha1.ReasonInvalidConfiguration,
			fmt.Sprintf("clusterName %q is too long for AWS NLB naming: max %d characters (name + '-cp-nlb' suffix must fit within %d character AWS limit)",
				name, maxClusterNameForNLB, maxNLBNameLength))
	}

	// Validate subnets are configured.
	if len(awsConfig.SubnetIDs) == 0 {
		return r.updateStatusError(ctx, lbr, butlerv1alpha1.ReasonInvalidConfiguration,
			"ProviderConfig AWS configuration has no SubnetIDs configured")
	}

	if lbr.Status.Phase != butlerv1alpha1.LoadBalancerPhaseCreating {
		lbr.SetPhase(butlerv1alpha1.LoadBalancerPhaseCreating)
		meta.SetStatusCondition(&lbr.Status.Conditions, metav1.Condition{
			Type:               butlerv1alpha1.ConditionTypeProgressing,
			Status:             metav1.ConditionTrue,
			Reason:             butlerv1alpha1.ReasonCreating,
			Message:            "Provisioning AWS NLB resources",
			ObservedGeneration: lbr.Generation,
		})
		if err := r.Status().Update(ctx, lbr); err != nil {
			return ctrl.Result{}, err
		}
		r.Recorder.Event(lbr, corev1.EventTypeNormal, "Creating", "Starting AWS NLB provisioning")
	}

	// 1. Create NLB
	nlbName := name + "-cp-nlb"
	log.Info("Ensuring NLB", "name", nlbName)

	subnetIDs := awsConfig.SubnetIDs

	nlbARN, nlbDNS, err := r.ensureNLB(ctx, elbClient, nlbName, subnetIDs)
	if err != nil {
		return r.updateStatusRetryableError(ctx, lbr, butlerv1alpha1.ReasonProviderError, fmt.Sprintf("creating NLB: %v", err))
	}
	log.Info("NLB ready", "arn", nlbARN, "dns", nlbDNS)

	// 2. Create target group
	tgName := name + "-cp-tg"
	log.Info("Ensuring target group", "name", tgName)

	healthCheckPort := lbr.GetHealthCheckPort()
	tgARN, err := r.ensureTargetGroup(ctx, elbClient, tgName, awsConfig.VPCID, port, healthCheckPort)
	if err != nil {
		return r.updateStatusRetryableError(ctx, lbr, butlerv1alpha1.ReasonProviderError, fmt.Sprintf("creating target group: %v", err))
	}
	log.Info("Target group ready", "arn", tgARN)

	// 3. Create listener
	log.Info("Ensuring listener", "nlb", nlbARN, "targetGroup", tgARN)

	err = r.ensureListener(ctx, elbClient, nlbARN, tgARN, port)
	if err != nil {
		return r.updateStatusRetryableError(ctx, lbr, butlerv1alpha1.ReasonProviderError, fmt.Sprintf("creating listener: %v", err))
	}

	// Update status to Ready
	lbr.SetPhase(butlerv1alpha1.LoadBalancerPhaseReady)
	lbr.Status.Endpoint = nlbDNS
	lbr.Status.ResourceID = nlbARN
	lbr.Status.ObservedGeneration = lbr.Generation

	meta.SetStatusCondition(&lbr.Status.Conditions, metav1.Condition{
		Type:               butlerv1alpha1.ConditionTypeReady,
		Status:             metav1.ConditionTrue,
		Reason:             butlerv1alpha1.ReasonReady,
		Message:            fmt.Sprintf("AWS NLB ready at %s", nlbDNS),
		ObservedGeneration: lbr.Generation,
	})
	meta.SetStatusCondition(&lbr.Status.Conditions, metav1.Condition{
		Type:               butlerv1alpha1.ConditionTypeProgressing,
		Status:             metav1.ConditionFalse,
		Reason:             butlerv1alpha1.ReasonReady,
		Message:            "Provisioning complete",
		ObservedGeneration: lbr.Generation,
	})

	if err := r.Status().Update(ctx, lbr); err != nil {
		return ctrl.Result{}, err
	}

	r.Recorder.Eventf(lbr, corev1.EventTypeNormal, "Ready", "AWS NLB ready at %s", nlbDNS)
	return ctrl.Result{}, nil
}

// reconcileTargets synchronizes LoadBalancerRequest.Spec.Targets with the AWS
// NLB target group's registered instances.
func (r *LoadBalancerRequestReconciler) reconcileTargets(ctx context.Context, lbr *butlerv1alpha1.LoadBalancerRequest, pc *butlerv1alpha1.ProviderConfig, elbClient *elbv2.Client) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if pc.Spec.AWS == nil {
		return r.updateStatusError(ctx, lbr, butlerv1alpha1.ReasonInvalidConfiguration,
			"ProviderConfig has no AWS configuration")
	}

	tgName := lbr.Spec.ClusterName + "-cp-tg"

	// Look up target group ARN by name
	descResp, err := elbClient.DescribeTargetGroups(ctx, &elbv2.DescribeTargetGroupsInput{
		Names: []string{tgName},
	})
	if err != nil {
		log.Error(err, "Failed to describe target group for target sync")
		return ctrl.Result{RequeueAfter: lbrRequeueShort}, nil
	}
	if len(descResp.TargetGroups) == 0 {
		log.Info("Target group not found", "name", tgName)
		return ctrl.Result{RequeueAfter: lbrRequeueShort}, nil
	}

	tgARN := descResp.TargetGroups[0].TargetGroupArn

	// Build set of desired target instance IDs
	desiredTargets := make(map[string]struct{})
	for _, target := range lbr.Spec.Targets {
		instanceID := target.InstanceID
		if instanceID == "" {
			continue
		}
		desiredTargets[instanceID] = struct{}{}

		// RegisterTargets is idempotent -- re-registering an already-registered
		// target is a no-op, so we do not need to check for duplicates.
		_, regErr := elbClient.RegisterTargets(ctx, &elbv2.RegisterTargetsInput{
			TargetGroupArn: tgARN,
			Targets: []elbv2types.TargetDescription{
				{Id: aws.String(instanceID)},
			},
		})
		if regErr != nil {
			log.Info("Could not register target", "instanceID", instanceID, "error", regErr)
			continue
		}
		log.Info("Registered target with NLB target group", "instanceID", instanceID)
	}

	// De-register stale targets that are no longer in the spec
	healthResp, err := elbClient.DescribeTargetHealth(ctx, &elbv2.DescribeTargetHealthInput{
		TargetGroupArn: tgARN,
	})
	if err != nil {
		log.Error(err, "Failed to describe target health for de-registration")
		return ctrl.Result{RequeueAfter: lbrRequeueShort}, nil
	}

	var staleTargets []elbv2types.TargetDescription
	for _, thd := range healthResp.TargetHealthDescriptions {
		if thd.Target == nil || thd.Target.Id == nil {
			continue
		}
		targetID := aws.ToString(thd.Target.Id)
		if _, ok := desiredTargets[targetID]; !ok {
			staleTargets = append(staleTargets, elbv2types.TargetDescription{
				Id: aws.String(targetID),
			})
		}
	}

	if len(staleTargets) > 0 {
		log.Info("De-registering stale targets", "count", len(staleTargets))
		_, err := elbClient.DeregisterTargets(ctx, &elbv2.DeregisterTargetsInput{
			TargetGroupArn: tgARN,
			Targets:        staleTargets,
		})
		if err != nil {
			log.Error(err, "Failed to de-register stale targets")
			// Continue -- we will retry on next reconcile
		}
	}

	// Set RegisteredTargets based on actual target group membership.
	// Re-describe after registration/deregistration to get the accurate count.
	currentHealthResp, err := elbClient.DescribeTargetHealth(ctx, &elbv2.DescribeTargetHealthInput{
		TargetGroupArn: tgARN,
	})
	if err != nil {
		log.Error(err, "Failed to describe target health for status update")
		lbr.Status.RegisteredTargets = int32(len(desiredTargets))
	} else {
		lbr.Status.RegisteredTargets = int32(len(currentHealthResp.TargetHealthDescriptions))
	}

	now := metav1.Now()
	lbr.Status.LastUpdated = &now
	if err := r.Status().Update(ctx, lbr); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: lbrRequeueLong}, nil
}

// reconcileDelete tears down all AWS NLB resources in reverse creation order:
// listener, target group, then NLB.
func (r *LoadBalancerRequestReconciler) reconcileDelete(ctx context.Context, lbr *butlerv1alpha1.LoadBalancerRequest, pc *butlerv1alpha1.ProviderConfig, elbClient *elbv2.Client) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if pc.Spec.AWS == nil {
		log.Info("ProviderConfig has no AWS configuration, removing finalizer")
		controllerutil.RemoveFinalizer(lbr, lbrFinalizerName)
		if err := r.Update(ctx, lbr); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	if lbr.Status.Phase != butlerv1alpha1.LoadBalancerPhaseDeleting {
		lbr.SetPhase(butlerv1alpha1.LoadBalancerPhaseDeleting)
		if err := r.Status().Update(ctx, lbr); err != nil {
			return ctrl.Result{}, err
		}
	}

	name := lbr.Spec.ClusterName
	nlbName := name + "-cp-nlb"
	tgName := name + "-cp-tg"

	// Look up NLB ARN by name
	nlbARN := ""
	nlbResp, err := elbClient.DescribeLoadBalancers(ctx, &elbv2.DescribeLoadBalancersInput{
		Names: []string{nlbName},
	})
	if err == nil && len(nlbResp.LoadBalancers) > 0 {
		nlbARN = aws.ToString(nlbResp.LoadBalancers[0].LoadBalancerArn)
	}

	// 1. Delete listeners (must be removed before target group and NLB)
	if nlbARN != "" {
		listenerResp, err := elbClient.DescribeListeners(ctx, &elbv2.DescribeListenersInput{
			LoadBalancerArn: aws.String(nlbARN),
		})
		if err == nil {
			for _, listener := range listenerResp.Listeners {
				log.Info("Deleting listener", "arn", aws.ToString(listener.ListenerArn))
				_, err := elbClient.DeleteListener(ctx, &elbv2.DeleteListenerInput{
					ListenerArn: listener.ListenerArn,
				})
				if err != nil && !isAWSNotFound(err) {
					log.Error(err, "Failed to delete listener")
					return ctrl.Result{RequeueAfter: lbrRequeueShort}, nil
				}
			}
		}
	}

	// 2. Delete target group
	tgResp, err := elbClient.DescribeTargetGroups(ctx, &elbv2.DescribeTargetGroupsInput{
		Names: []string{tgName},
	})
	if err == nil && len(tgResp.TargetGroups) > 0 {
		tgARN := tgResp.TargetGroups[0].TargetGroupArn
		log.Info("Deleting target group", "name", tgName)
		_, err := elbClient.DeleteTargetGroup(ctx, &elbv2.DeleteTargetGroupInput{
			TargetGroupArn: tgARN,
		})
		if err != nil && !isAWSNotFound(err) {
			log.Error(err, "Failed to delete target group")
			return ctrl.Result{RequeueAfter: lbrRequeueShort}, nil
		}
	}

	// 3. Delete NLB
	if nlbARN != "" {
		log.Info("Deleting NLB", "name", nlbName)
		_, err := elbClient.DeleteLoadBalancer(ctx, &elbv2.DeleteLoadBalancerInput{
			LoadBalancerArn: aws.String(nlbARN),
		})
		if err != nil && !isAWSNotFound(err) {
			log.Error(err, "Failed to delete NLB")
			return ctrl.Result{RequeueAfter: lbrRequeueShort}, nil
		}

		// Wait for NLB to be fully deleted
		log.Info("Waiting for NLB deletion", "name", nlbName)
		waiter := elbv2.NewLoadBalancersDeletedWaiter(elbClient)
		if err := waiter.Wait(ctx, &elbv2.DescribeLoadBalancersInput{
			LoadBalancerArns: []string{nlbARN},
		}, 5*time.Minute); err != nil {
			log.Info("NLB deletion waiter returned error (may already be deleted)", "error", err)
		}
	}

	controllerutil.RemoveFinalizer(lbr, lbrFinalizerName)
	if err := r.Update(ctx, lbr); err != nil {
		return ctrl.Result{}, err
	}

	log.Info("AWS NLB resources deleted")
	r.Recorder.Event(lbr, corev1.EventTypeNormal, "Deleted", "AWS NLB resources cleaned up")
	return ctrl.Result{}, nil
}

// ensureNLB creates a Network Load Balancer if it doesn't exist and returns its ARN and DNS name.
func (r *LoadBalancerRequestReconciler) ensureNLB(ctx context.Context, elbClient *elbv2.Client, name string, subnetIDs []string) (string, string, error) {
	// Check if already exists
	descResp, err := elbClient.DescribeLoadBalancers(ctx, &elbv2.DescribeLoadBalancersInput{
		Names: []string{name},
	})
	if err == nil && len(descResp.LoadBalancers) > 0 {
		lb := descResp.LoadBalancers[0]
		return aws.ToString(lb.LoadBalancerArn), aws.ToString(lb.DNSName), nil
	}
	if err != nil && !isAWSNotFound(err) {
		return "", "", fmt.Errorf("checking NLB: %w", err)
	}

	// Create
	input := &elbv2.CreateLoadBalancerInput{
		Name:   aws.String(name),
		Type:   elbv2types.LoadBalancerTypeEnumNetwork,
		Scheme: elbv2types.LoadBalancerSchemeEnumInternetFacing,
		Tags: []elbv2types.Tag{
			{Key: aws.String("butler.butlerlabs.dev/managed-by"), Value: aws.String("butler")},
		},
	}
	if len(subnetIDs) > 0 {
		input.Subnets = subnetIDs
	}

	createResp, err := elbClient.CreateLoadBalancer(ctx, input)
	if err != nil {
		return "", "", fmt.Errorf("creating NLB: %w", err)
	}
	if len(createResp.LoadBalancers) == 0 {
		return "", "", fmt.Errorf("CreateLoadBalancer returned no load balancers")
	}

	lb := createResp.LoadBalancers[0]
	return aws.ToString(lb.LoadBalancerArn), aws.ToString(lb.DNSName), nil
}

// ensureTargetGroup creates a TCP target group if it doesn't exist and returns its ARN.
func (r *LoadBalancerRequestReconciler) ensureTargetGroup(ctx context.Context, elbClient *elbv2.Client, name, vpcID string, port, healthCheckPort int32) (string, error) {
	// Check if already exists
	descResp, err := elbClient.DescribeTargetGroups(ctx, &elbv2.DescribeTargetGroupsInput{
		Names: []string{name},
	})
	if err == nil && len(descResp.TargetGroups) > 0 {
		return aws.ToString(descResp.TargetGroups[0].TargetGroupArn), nil
	}
	if err != nil && !isAWSNotFound(err) {
		return "", fmt.Errorf("checking target group: %w", err)
	}

	// Create
	createResp, err := elbClient.CreateTargetGroup(ctx, &elbv2.CreateTargetGroupInput{
		Name:                       aws.String(name),
		Protocol:                   elbv2types.ProtocolEnumTcp,
		Port:                       aws.Int32(port),
		VpcId:                      aws.String(vpcID),
		TargetType:                 elbv2types.TargetTypeEnumInstance,
		HealthCheckProtocol:        elbv2types.ProtocolEnumTcp,
		HealthCheckPort:            aws.String(fmt.Sprintf("%d", healthCheckPort)),
		HealthCheckIntervalSeconds: aws.Int32(10),
		HealthyThresholdCount:      aws.Int32(2),
		UnhealthyThresholdCount:    aws.Int32(3),
		Tags: []elbv2types.Tag{
			{Key: aws.String("butler.butlerlabs.dev/managed-by"), Value: aws.String("butler")},
		},
	})
	if err != nil {
		return "", fmt.Errorf("creating target group: %w", err)
	}
	if len(createResp.TargetGroups) == 0 {
		return "", fmt.Errorf("CreateTargetGroup returned no target groups")
	}

	return aws.ToString(createResp.TargetGroups[0].TargetGroupArn), nil
}

// ensureListener creates a TCP listener on the NLB forwarding to the target group
// if one doesn't already exist with the expected port and target group.
func (r *LoadBalancerRequestReconciler) ensureListener(ctx context.Context, elbClient *elbv2.Client, nlbARN, tgARN string, port int32) error {
	// Check if a listener already exists for this NLB
	descResp, err := elbClient.DescribeListeners(ctx, &elbv2.DescribeListenersInput{
		LoadBalancerArn: aws.String(nlbARN),
	})
	if err != nil && !isAWSNotFound(err) {
		return fmt.Errorf("checking listeners: %w", err)
	}

	// Check if an existing listener matches the expected port and target group
	if err == nil {
		for _, listener := range descResp.Listeners {
			listenerPort := aws.ToInt32(listener.Port)
			if listenerPort != port {
				continue
			}
			// Verify the default action forwards to the expected target group
			for _, action := range listener.DefaultActions {
				if action.Type == elbv2types.ActionTypeEnumForward &&
					aws.ToString(action.TargetGroupArn) == tgARN {
					return nil // Listener matches expected configuration
				}
			}
		}
	}

	// Create listener (no matching listener found)
	_, err = elbClient.CreateListener(ctx, &elbv2.CreateListenerInput{
		LoadBalancerArn: aws.String(nlbARN),
		Protocol:        elbv2types.ProtocolEnumTcp,
		Port:            aws.Int32(port),
		DefaultActions: []elbv2types.Action{
			{
				Type:           elbv2types.ActionTypeEnumForward,
				TargetGroupArn: aws.String(tgARN),
			},
		},
	})
	if err != nil {
		// A DuplicateListener error means a listener exists on this port but
		// with a different target group. This is a configuration conflict.
		var dupErr *elbv2types.DuplicateListenerException
		if errors.As(err, &dupErr) {
			return fmt.Errorf("listener already exists on port %d with a different target group: %w", port, err)
		}
		return fmt.Errorf("creating listener: %w", err)
	}

	return nil
}

func (r *LoadBalancerRequestReconciler) getProviderConfig(ctx context.Context, lbr *butlerv1alpha1.LoadBalancerRequest) (*butlerv1alpha1.ProviderConfig, error) {
	pc := &butlerv1alpha1.ProviderConfig{}
	ns := lbr.Spec.ProviderConfigRef.Namespace
	if ns == "" {
		ns = lbr.Namespace
	}
	key := types.NamespacedName{Name: lbr.Spec.ProviderConfigRef.Name, Namespace: ns}
	if err := r.Get(ctx, key, pc); err != nil {
		return nil, fmt.Errorf("failed to get ProviderConfig %s: %w", key, err)
	}
	return pc, nil
}

func (r *LoadBalancerRequestReconciler) createELBClient(ctx context.Context, pc *butlerv1alpha1.ProviderConfig) (*elbv2.Client, error) {
	if pc.Spec.AWS == nil {
		return nil, fmt.Errorf("ProviderConfig %s has no AWS configuration", pc.Name)
	}

	// Get credentials secret
	secret := &corev1.Secret{}
	ns := pc.Spec.CredentialsRef.Namespace
	if ns == "" {
		ns = pc.Namespace
	}

	key := types.NamespacedName{
		Name:      pc.Spec.CredentialsRef.Name,
		Namespace: ns,
	}

	if err := r.Get(ctx, key, secret); err != nil {
		return nil, fmt.Errorf("failed to get credentials secret %s: %w", key, err)
	}

	accessKeyID := string(secret.Data["accessKeyID"])
	secretAccessKey := string(secret.Data["secretAccessKey"])

	if accessKeyID == "" || secretAccessKey == "" {
		return nil, fmt.Errorf("credentials secret %s missing required keys 'accessKeyID' and/or 'secretAccessKey'", key)
	}

	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(pc.Spec.AWS.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	return elbv2.NewFromConfig(awsCfg), nil
}

func (r *LoadBalancerRequestReconciler) updatePhase(ctx context.Context, lbr *butlerv1alpha1.LoadBalancerRequest, phase butlerv1alpha1.LoadBalancerPhase) (ctrl.Result, error) {
	lbr.SetPhase(phase)
	if err := r.Status().Update(ctx, lbr); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}

// updateStatusError sets the LBR to a terminal Failed state. Use for
// configuration errors and bad credentials that will not resolve on retry.
func (r *LoadBalancerRequestReconciler) updateStatusError(ctx context.Context, lbr *butlerv1alpha1.LoadBalancerRequest, reason, message string) (ctrl.Result, error) {
	lbr.SetFailure(reason, message)
	meta.SetStatusCondition(&lbr.Status.Conditions, metav1.Condition{
		Type:               butlerv1alpha1.ConditionTypeReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: lbr.Generation,
	})
	if err := r.Status().Update(ctx, lbr); err != nil {
		return ctrl.Result{}, err
	}
	r.Recorder.Event(lbr, corev1.EventTypeWarning, reason, message)
	return ctrl.Result{}, nil
}

// updateStatusRetryableError sets a Degraded condition and requeues for retry.
// Use for transient AWS API errors that may resolve on subsequent attempts.
// Unlike updateStatusError, this does NOT set the phase to Failed.
func (r *LoadBalancerRequestReconciler) updateStatusRetryableError(ctx context.Context, lbr *butlerv1alpha1.LoadBalancerRequest, reason, message string) (ctrl.Result, error) {
	meta.SetStatusCondition(&lbr.Status.Conditions, metav1.Condition{
		Type:               butlerv1alpha1.ConditionTypeDegraded,
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: lbr.Generation,
	})
	now := metav1.Now()
	lbr.Status.LastUpdated = &now
	if err := r.Status().Update(ctx, lbr); err != nil {
		return ctrl.Result{}, err
	}
	r.Recorder.Event(lbr, corev1.EventTypeWarning, reason, message)
	return ctrl.Result{RequeueAfter: lbrRequeueShort}, nil
}

// isAWSNotFound checks whether the error is an AWS "not found" exception using
// typed error checking with the AWS SDK v2 smithy error interface.
func isAWSNotFound(err error) bool {
	if err == nil {
		return false
	}
	var lbNotFound *elbv2types.LoadBalancerNotFoundException
	if errors.As(err, &lbNotFound) {
		return true
	}
	var tgNotFound *elbv2types.TargetGroupNotFoundException
	if errors.As(err, &tgNotFound) {
		return true
	}
	var listenerNotFound *elbv2types.ListenerNotFoundException
	if errors.As(err, &listenerNotFound) {
		return true
	}
	// Fallback: check the smithy API error code for any other not-found codes
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		return code == "LoadBalancerNotFound" ||
			code == "TargetGroupNotFound" ||
			code == "ListenerNotFound"
	}
	return false
}

// SetupWithManager registers the controller with the manager.
func (r *LoadBalancerRequestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&butlerv1alpha1.LoadBalancerRequest{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Named("loadbalancerrequest").
		Complete(r)
}
