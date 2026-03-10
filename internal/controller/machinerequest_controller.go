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
	"fmt"
	"time"

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
	"github.com/butlerdotdev/butler-provider-aws/internal/awsclient"
)

const (
	finalizerName = "machinerequest.butler.butlerlabs.dev/aws-finalizer"

	// Requeue intervals.
	requeueShort = 10 * time.Second
	requeueLong  = 30 * time.Second
)

// MachineRequestReconciler reconciles a MachineRequest object for AWS.
type MachineRequestReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=machinerequests,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=machinerequests/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=machinerequests/finalizers,verbs=update
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=providerconfigs,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile handles MachineRequest reconciliation for AWS.
func (r *MachineRequestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Fetch the MachineRequest
	machineRequest := &butlerv1alpha1.MachineRequest{}
	if err := r.Get(ctx, req.NamespacedName, machineRequest); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Get the ProviderConfig to check if this is an AWS request
	providerConfig, err := r.getProviderConfig(ctx, machineRequest)
	if err != nil {
		log.Error(err, "Failed to get ProviderConfig")
		return r.updateStatusError(ctx, machineRequest, "ProviderConfigError", err.Error())
	}

	// Only handle AWS provider requests
	if providerConfig.Spec.Provider != butlerv1alpha1.ProviderTypeAWS {
		log.V(1).Info("Skipping non-AWS MachineRequest", "provider", providerConfig.Spec.Provider)
		return ctrl.Result{}, nil
	}

	// Create AWS client
	ac, err := r.createAWSClient(ctx, providerConfig)
	if err != nil {
		log.Error(err, "Failed to create AWS client")
		return r.updateStatusError(ctx, machineRequest, "AWSClientError", err.Error())
	}

	// Handle deletion
	if !machineRequest.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, machineRequest, ac)
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(machineRequest, finalizerName) {
		controllerutil.AddFinalizer(machineRequest, finalizerName)
		if err := r.Update(ctx, machineRequest); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Reconcile based on current phase
	switch machineRequest.Status.Phase {
	case "", butlerv1alpha1.MachinePhasePending:
		return r.reconcilePending(ctx, machineRequest, ac)
	case butlerv1alpha1.MachinePhaseCreating:
		return r.reconcileCreating(ctx, machineRequest, ac)
	case butlerv1alpha1.MachinePhaseRunning:
		return r.reconcileRunning(ctx, machineRequest, ac)
	case butlerv1alpha1.MachinePhaseFailed:
		// Don't reconcile failed machines unless manually reset
		return ctrl.Result{}, nil
	default:
		log.Info("Unknown phase, resetting to Pending", "phase", machineRequest.Status.Phase)
		return r.updatePhase(ctx, machineRequest, butlerv1alpha1.MachinePhasePending)
	}
}

// reconcilePending handles the Pending phase - creates the EC2 instance.
func (r *MachineRequestReconciler) reconcilePending(
	ctx context.Context,
	mr *butlerv1alpha1.MachineRequest,
	ac *awsclient.Client,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Creating EC2 instance", "name", mr.Spec.MachineName)

	// Check if VM already exists (idempotency)
	status, err := ac.GetVMStatus(ctx, mr.Spec.MachineName)
	if err == nil && status.Exists {
		log.Info("EC2 instance already exists, moving to Creating phase", "name", mr.Spec.MachineName)
		return r.updatePhase(ctx, mr, butlerv1alpha1.MachinePhaseCreating)
	}

	opts := awsclient.VMCreateOptions{
		Name:     mr.Spec.MachineName,
		CPU:      mr.Spec.CPU,
		MemoryMB: mr.Spec.MemoryMB,
		DiskGB:   mr.Spec.DiskGB,
		UserData: mr.Spec.UserData,
		Labels:   mr.Spec.Labels,
	}

	instanceID, err := ac.CreateVM(ctx, opts)
	if err != nil {
		log.Error(err, "Failed to create EC2 instance")
		r.Recorder.Eventf(mr, corev1.EventTypeWarning, "CreateFailed", "Failed to create EC2 instance: %v", err)
		return r.updateStatusError(ctx, mr, butlerv1alpha1.ReasonProviderError, err.Error())
	}

	// Update status with provider ID and move to Creating phase
	mr.Status.ProviderID = instanceID
	mr.Status.Phase = butlerv1alpha1.MachinePhaseCreating
	mr.Status.FailureReason = ""
	mr.Status.FailureMessage = ""
	now := metav1.Now()
	mr.Status.LastUpdated = &now
	mr.Status.ObservedGeneration = mr.Generation

	meta.SetStatusCondition(&mr.Status.Conditions, metav1.Condition{
		Type:               butlerv1alpha1.ConditionTypeProgressing,
		Status:             metav1.ConditionTrue,
		Reason:             butlerv1alpha1.ReasonCreating,
		Message:            "EC2 instance is being created",
		ObservedGeneration: mr.Generation,
	})

	if err := r.Status().Update(ctx, mr); err != nil {
		return ctrl.Result{}, err
	}

	r.Recorder.Eventf(mr, corev1.EventTypeNormal, "Created", "EC2 instance creation initiated: %s", instanceID)
	return ctrl.Result{RequeueAfter: requeueShort}, nil
}

// reconcileCreating handles the Creating phase - waits for the instance to be running with an IP.
func (r *MachineRequestReconciler) reconcileCreating(
	ctx context.Context,
	mr *butlerv1alpha1.MachineRequest,
	ac *awsclient.Client,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Checking EC2 instance status", "name", mr.Spec.MachineName)

	status, err := ac.GetVMStatus(ctx, mr.Spec.MachineName)
	if err != nil {
		if awsclient.IsNotFound(err) {
			// VM doesn't exist, go back to Pending to recreate
			log.Info("EC2 instance not found, returning to Pending phase")
			return r.updatePhase(ctx, mr, butlerv1alpha1.MachinePhasePending)
		}
		log.Error(err, "Failed to get EC2 instance status")
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	log.V(1).Info("EC2 instance status", "status", status.Status, "ip", status.IPAddress)

	// Check if the instance is running and has an IP
	if status.Status == awsclient.InstanceStateRunning && status.IPAddress != "" {
		// Prefer external IP for bootstrap reachability (mgmt cluster -> cloud VM)
		ip := status.IPAddress
		if status.ExternalIP != "" {
			ip = status.ExternalIP
		}
		log.Info("EC2 instance is ready", "ip", ip, "internalIP", status.IPAddress, "externalIP", status.ExternalIP)

		mr.Status.Phase = butlerv1alpha1.MachinePhaseRunning
		mr.Status.IPAddress = ip
		now := metav1.Now()
		mr.Status.LastUpdated = &now

		meta.SetStatusCondition(&mr.Status.Conditions, metav1.Condition{
			Type:               butlerv1alpha1.ConditionTypeReady,
			Status:             metav1.ConditionTrue,
			Reason:             butlerv1alpha1.ReasonRunning,
			Message:            fmt.Sprintf("EC2 instance is running with IP %s", ip),
			ObservedGeneration: mr.Generation,
		})
		meta.SetStatusCondition(&mr.Status.Conditions, metav1.Condition{
			Type:               butlerv1alpha1.ConditionTypeProgressing,
			Status:             metav1.ConditionFalse,
			Reason:             butlerv1alpha1.ReasonRunning,
			Message:            "EC2 instance creation complete",
			ObservedGeneration: mr.Generation,
		})

		if err := r.Status().Update(ctx, mr); err != nil {
			return ctrl.Result{}, err
		}

		r.Recorder.Eventf(mr, corev1.EventTypeNormal, "Ready", "EC2 instance is running with IP %s", ip)
		return ctrl.Result{}, nil
	}

	// Still waiting for running state and IP, update condition and requeue
	meta.SetStatusCondition(&mr.Status.Conditions, metav1.Condition{
		Type:               butlerv1alpha1.ConditionTypeProgressing,
		Status:             metav1.ConditionTrue,
		Reason:             butlerv1alpha1.ReasonWaitingForIP,
		Message:            fmt.Sprintf("EC2 instance state: %s, waiting for running state and IP address", status.Status),
		ObservedGeneration: mr.Generation,
	})

	if err := r.Status().Update(ctx, mr); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: requeueShort}, nil
}

// reconcileRunning handles the Running phase - monitors for drift.
func (r *MachineRequestReconciler) reconcileRunning(
	ctx context.Context,
	mr *butlerv1alpha1.MachineRequest,
	ac *awsclient.Client,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Periodically verify the instance still exists and is running
	status, err := ac.GetVMStatus(ctx, mr.Spec.MachineName)
	if err != nil {
		if awsclient.IsNotFound(err) {
			log.Info("EC2 instance no longer exists, marking as failed")
			mr.SetFailure("VMTerminated", "EC2 instance was terminated externally")
			if err := r.Status().Update(ctx, mr); err != nil {
				return ctrl.Result{}, err
			}
			r.Recorder.Event(mr, corev1.EventTypeWarning, "VMTerminated", "EC2 instance was terminated externally")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{RequeueAfter: requeueLong}, nil
	}

	// Update IP if it changed - prefer external IP for bootstrap reachability
	ip := status.IPAddress
	if status.ExternalIP != "" {
		ip = status.ExternalIP
	}
	if ip != "" && ip != mr.Status.IPAddress {
		log.Info("EC2 instance IP changed", "old", mr.Status.IPAddress, "new", ip)
		mr.Status.IPAddress = ip
		now := metav1.Now()
		mr.Status.LastUpdated = &now
		if err := r.Status().Update(ctx, mr); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{RequeueAfter: requeueLong}, nil
}

// reconcileDelete handles MachineRequest deletion - terminates the EC2 instance.
func (r *MachineRequestReconciler) reconcileDelete(
	ctx context.Context,
	mr *butlerv1alpha1.MachineRequest,
	ac *awsclient.Client,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.Info("Deleting EC2 instance", "name", mr.Spec.MachineName)

	// Update phase to Deleting
	if mr.Status.Phase != butlerv1alpha1.MachinePhaseDeleting {
		mr.Status.Phase = butlerv1alpha1.MachinePhaseDeleting
		now := metav1.Now()
		mr.Status.LastUpdated = &now
		if err := r.Status().Update(ctx, mr); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Terminate the EC2 instance
	if err := ac.DeleteVM(ctx, mr.Spec.MachineName); err != nil {
		log.Error(err, "Failed to terminate EC2 instance")
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	// Remove finalizer
	controllerutil.RemoveFinalizer(mr, finalizerName)
	if err := r.Update(ctx, mr); err != nil {
		return ctrl.Result{}, err
	}

	log.Info("EC2 instance terminated successfully")
	r.Recorder.Event(mr, corev1.EventTypeNormal, "Deleted", "EC2 instance terminated")
	return ctrl.Result{}, nil
}

// Helper methods

func (r *MachineRequestReconciler) getProviderConfig(ctx context.Context, mr *butlerv1alpha1.MachineRequest) (*butlerv1alpha1.ProviderConfig, error) {
	pc := &butlerv1alpha1.ProviderConfig{}
	ns := mr.Spec.ProviderRef.Namespace
	if ns == "" {
		ns = mr.Namespace
	}

	key := types.NamespacedName{
		Name:      mr.Spec.ProviderRef.Name,
		Namespace: ns,
	}

	if err := r.Get(ctx, key, pc); err != nil {
		return nil, fmt.Errorf("failed to get ProviderConfig %s: %w", key, err)
	}

	return pc, nil
}

func (r *MachineRequestReconciler) createAWSClient(ctx context.Context, pc *butlerv1alpha1.ProviderConfig) (*awsclient.Client, error) {
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

	cfg := awsclient.ClientConfig{
		InstanceType: awsclient.DefaultInstanceType,
	}
	if len(pc.Spec.AWS.SubnetIDs) > 0 {
		cfg.SubnetID = pc.Spec.AWS.SubnetIDs[0]
	}
	if len(pc.Spec.AWS.SecurityGroupIDs) > 0 {
		cfg.SecurityGroupID = pc.Spec.AWS.SecurityGroupIDs[0]
	}

	return awsclient.NewClient(ctx, accessKeyID, secretAccessKey, pc.Spec.AWS.Region, pc.Spec.AWS.VPCID, cfg)
}

func (r *MachineRequestReconciler) updatePhase(ctx context.Context, mr *butlerv1alpha1.MachineRequest, phase butlerv1alpha1.MachinePhase) (ctrl.Result, error) {
	mr.Status.Phase = phase
	now := metav1.Now()
	mr.Status.LastUpdated = &now
	if err := r.Status().Update(ctx, mr); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}

func (r *MachineRequestReconciler) updateStatusError(ctx context.Context, mr *butlerv1alpha1.MachineRequest, reason, message string) (ctrl.Result, error) {
	mr.SetFailure(reason, message)
	meta.SetStatusCondition(&mr.Status.Conditions, metav1.Condition{
		Type:               butlerv1alpha1.ConditionTypeReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: mr.Generation,
	})
	if err := r.Status().Update(ctx, mr); err != nil {
		return ctrl.Result{}, err
	}
	r.Recorder.Event(mr, corev1.EventTypeWarning, reason, message)
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MachineRequestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&butlerv1alpha1.MachineRequest{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Named("machinerequest").
		Complete(r)
}
