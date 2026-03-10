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

// Package imagesync provides ImageSync fulfillment for AWS providers
// during bootstrap. This controller watches ImageSync resources and imports
// disk images into the target AWS account as AMIs via EC2 ImportImage.
//
// NOTE: This code follows the same pattern as butler-provider-gcp's imagesync.
// During bootstrap, only provider controllers are running (butler-controller
// is not installed yet). In steady state, butler-controller handles ImageSync.
package imagesync

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"

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
	"sigs.k8s.io/controller-runtime/pkg/log"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

const (
	requeueShort = 15 * time.Second
	requeueLong  = 60 * time.Second
	requeueReady = 10 * time.Minute
)

// Reconciler reconciles ImageSync resources for AWS providers.
type Reconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=imagesyncs,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=imagesyncs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=imagesyncs/finalizers,verbs=update
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=providerconfigs,verbs=get;list;watch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=butlerconfigs,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile handles ImageSync reconciliation for AWS.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	is := &butlerv1alpha1.ImageSync{}
	if err := r.Get(ctx, req.NamespacedName, is); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	logger.V(1).Info("reconciling ImageSync", "name", is.Name, "phase", is.Status.Phase)

	// Get ProviderConfig to check if this is an AWS request.
	pc, err := r.getProviderConfig(ctx, is)
	if err != nil {
		logger.V(1).Info("skipping ImageSync: cannot get ProviderConfig", "error", err)
		return ctrl.Result{}, nil
	}

	if pc.Spec.Provider != butlerv1alpha1.ProviderTypeAWS {
		logger.V(1).Info("skipping non-AWS ImageSync", "provider", pc.Spec.Provider)
		return ctrl.Result{}, nil
	}

	// Handle deletion.
	if !is.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, is, pc)
	}

	// Add finalizer.
	if !controllerutil.ContainsFinalizer(is, butlerv1alpha1.FinalizerImageSync) {
		controllerutil.AddFinalizer(is, butlerv1alpha1.FinalizerImageSync)
		if err := r.Update(ctx, is); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Set initial phase if not set.
	if is.Status.Phase == "" {
		is.SetPhase(butlerv1alpha1.ImageSyncPhasePending)
		is.Status.ObservedGeneration = is.Generation
		if err := r.Status().Update(ctx, is); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Fetch ButlerConfig for factory URL.
	bc, err := r.getButlerConfig(ctx)
	if err != nil {
		return r.setFailed(ctx, is, "ButlerConfigNotFound", err.Error())
	}

	if !bc.IsImageFactoryConfigured() {
		return r.setFailed(ctx, is, "ImageFactoryNotConfigured", "ButlerConfig.spec.imageFactory is not configured")
	}

	// Dispatch based on phase.
	switch is.Status.Phase {
	case butlerv1alpha1.ImageSyncPhasePending:
		return r.reconcilePending(ctx, is, pc, bc)
	case butlerv1alpha1.ImageSyncPhaseDownloading, butlerv1alpha1.ImageSyncPhaseUploading:
		return r.reconcileInProgress(ctx, is, pc)
	case butlerv1alpha1.ImageSyncPhaseFailed:
		return ctrl.Result{RequeueAfter: requeueLong}, nil
	case butlerv1alpha1.ImageSyncPhaseReady:
		return ctrl.Result{RequeueAfter: requeueReady}, nil
	}

	return ctrl.Result{}, nil
}

// reconcilePending initiates the EC2 image import.
func (r *Reconciler) reconcilePending(ctx context.Context, is *butlerv1alpha1.ImageSync, pc *butlerv1alpha1.ProviderConfig, bc *butlerv1alpha1.ButlerConfig) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	factoryURL := bc.GetImageFactoryURL()
	artifactURL := buildArtifactURL(factoryURL, is.Spec.FactoryRef, is.Spec.Format)
	imageName := buildProviderImageName(is)

	is.Status.ArtifactURL = artifactURL

	// Get AWS credentials.
	ec2Client, err := r.createEC2Client(ctx, pc)
	if err != nil {
		return r.setFailed(ctx, is, "CredentialsError", fmt.Sprintf("failed to create EC2 client: %v", err))
	}

	// Check if AMI already exists by tag.
	existing, err := r.findExistingAMI(ctx, ec2Client, is.Name)
	if err != nil {
		logger.V(1).Info("error checking for existing AMI", "error", err)
	}
	if existing != "" {
		logger.Info("AMI already exists", "ami", existing)
		return r.setReady(ctx, is, existing)
	}

	// Start the import.
	logger.Info("importing image into AWS", "name", imageName, "url", artifactURL)

	resp, err := ec2Client.ImportImage(ctx, &ec2.ImportImageInput{
		Description: aws.String(fmt.Sprintf("Butler Image Factory import: %s", imageName)),
		DiskContainers: []ec2types.ImageDiskContainer{
			{
				Format: aws.String("RAW"),
				Url:    aws.String(artifactURL),
			},
		},
		TagSpecifications: []ec2types.TagSpecification{
			{
				ResourceType: ec2types.ResourceTypeImportImageTask,
				Tags: []ec2types.Tag{
					{Key: aws.String("Name"), Value: aws.String(imageName)},
					{Key: aws.String("butler.butlerlabs.dev/imagesync"), Value: aws.String(is.Name)},
					{Key: aws.String("butler.butlerlabs.dev/managed-by"), Value: aws.String("butler")},
				},
			},
		},
	})
	if err != nil {
		return r.setFailed(ctx, is, "ImportImageFailed", fmt.Sprintf("failed to start EC2 ImportImage: %v", err))
	}

	taskID := ""
	if resp.ImportTaskId != nil {
		taskID = *resp.ImportTaskId
	}

	is.Status.ProviderTaskID = taskID
	is.SetPhase(butlerv1alpha1.ImageSyncPhaseDownloading)
	is.Status.ObservedGeneration = is.Generation
	meta.SetStatusCondition(&is.Status.Conditions, metav1.Condition{
		Type:               butlerv1alpha1.ConditionTypeProgressing,
		Status:             metav1.ConditionTrue,
		Reason:             butlerv1alpha1.ReasonImageDownloading,
		Message:            fmt.Sprintf("EC2 ImportImage started (task %s) from %s", taskID, artifactURL),
		ObservedGeneration: is.Generation,
	})
	if err := r.Status().Update(ctx, is); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: requeueShort}, nil
}

// reconcileInProgress polls the EC2 import task status.
func (r *Reconciler) reconcileInProgress(ctx context.Context, is *butlerv1alpha1.ImageSync, pc *butlerv1alpha1.ProviderConfig) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	taskID := is.Status.ProviderTaskID
	if taskID == "" {
		return r.setFailed(ctx, is, "MissingTaskID", "import task ID not found in status")
	}

	ec2Client, err := r.createEC2Client(ctx, pc)
	if err != nil {
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	resp, err := ec2Client.DescribeImportImageTasks(ctx, &ec2.DescribeImportImageTasksInput{
		ImportTaskIds: []string{taskID},
	})
	if err != nil {
		logger.V(1).Info("error polling import task", "taskID", taskID, "error", err)
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	if len(resp.ImportImageTasks) == 0 {
		return r.setFailed(ctx, is, "TaskNotFound", fmt.Sprintf("import task %s not found", taskID))
	}

	task := resp.ImportImageTasks[0]
	status := ""
	if task.Status != nil {
		status = *task.Status
	}

	logger.V(1).Info("import task status", "taskID", taskID, "status", status)

	switch status {
	case "completed":
		amiID := ""
		if task.ImageId != nil {
			amiID = *task.ImageId
		}
		if amiID == "" {
			return r.setFailed(ctx, is, "NoAMI", "import completed but no AMI ID returned")
		}

		// Tag the resulting AMI for identification.
		imageName := buildProviderImageName(is)
		_, tagErr := ec2Client.CreateTags(ctx, &ec2.CreateTagsInput{
			Resources: []string{amiID},
			Tags: []ec2types.Tag{
				{Key: aws.String("Name"), Value: aws.String(imageName)},
				{Key: aws.String("butler.butlerlabs.dev/imagesync"), Value: aws.String(is.Name)},
				{Key: aws.String("butler.butlerlabs.dev/managed-by"), Value: aws.String("butler")},
			},
		})
		if tagErr != nil {
			logger.V(1).Info("failed to tag AMI (non-fatal)", "ami", amiID, "error", tagErr)
		}

		is.Status.ProviderTaskID = ""
		return r.setReady(ctx, is, amiID)

	case "active":
		return ctrl.Result{RequeueAfter: requeueShort}, nil

	case "deleted", "deleting":
		msg := "import task was deleted"
		if task.StatusMessage != nil && *task.StatusMessage != "" {
			msg = *task.StatusMessage
		}
		return r.setFailed(ctx, is, "ImportDeleted", msg)

	default:
		// Unknown or transitional status.
		if task.StatusMessage != nil && strings.Contains(strings.ToLower(*task.StatusMessage), "error") {
			return r.setFailed(ctx, is, "ImportError", *task.StatusMessage)
		}
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}
}

// handleDeletion cleans up the AMI and associated snapshots, then removes the finalizer.
func (r *Reconciler) handleDeletion(ctx context.Context, is *butlerv1alpha1.ImageSync, pc *butlerv1alpha1.ProviderConfig) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(is, butlerv1alpha1.FinalizerImageSync) {
		return ctrl.Result{}, nil
	}

	// Clean up the AMI if one was created.
	if is.Status.ProviderImageRef != "" {
		ec2Client, err := r.createEC2Client(ctx, pc)
		if err != nil {
			logger.V(1).Info("cannot create EC2 client for cleanup, removing finalizer anyway", "error", err)
		} else {
			r.cleanupAMI(ctx, ec2Client, is.Status.ProviderImageRef, logger)
		}
	}

	logger.Info("removing ImageSync finalizer", "name", is.Name)

	if err := r.Get(ctx, types.NamespacedName{Name: is.Name, Namespace: is.Namespace}, is); err != nil {
		return ctrl.Result{}, err
	}

	controllerutil.RemoveFinalizer(is, butlerv1alpha1.FinalizerImageSync)
	return ctrl.Result{}, r.Update(ctx, is)
}

// cleanupAMI deregisters an AMI and deletes its associated snapshots.
// Errors are logged but do not block finalizer removal.
func (r *Reconciler) cleanupAMI(ctx context.Context, ec2Client *ec2.Client, amiID string, logger interface{ Info(string, ...any) }) {
	// Find snapshots associated with this AMI.
	descResp, err := ec2Client.DescribeImages(ctx, &ec2.DescribeImagesInput{
		ImageIds: []string{amiID},
	})
	if err != nil {
		logger.Info("could not describe AMI for cleanup", "ami", amiID, "error", err)
		return
	}

	var snapshotIDs []string
	if len(descResp.Images) > 0 {
		for _, bdm := range descResp.Images[0].BlockDeviceMappings {
			if bdm.Ebs != nil && bdm.Ebs.SnapshotId != nil {
				snapshotIDs = append(snapshotIDs, *bdm.Ebs.SnapshotId)
			}
		}
	}

	// Deregister the AMI.
	_, err = ec2Client.DeregisterImage(ctx, &ec2.DeregisterImageInput{
		ImageId: aws.String(amiID),
	})
	if err != nil {
		logger.Info("failed to deregister AMI", "ami", amiID, "error", err)
		return
	}
	logger.Info("deregistered AMI", "ami", amiID)

	// Delete associated snapshots.
	for _, snapID := range snapshotIDs {
		_, err := ec2Client.DeleteSnapshot(ctx, &ec2.DeleteSnapshotInput{
			SnapshotId: aws.String(snapID),
		})
		if err != nil {
			logger.Info("failed to delete snapshot", "snapshot", snapID, "error", err)
		} else {
			logger.Info("deleted snapshot", "snapshot", snapID)
		}
	}
}

// setFailed sets the ImageSync to Failed phase.
func (r *Reconciler) setFailed(ctx context.Context, is *butlerv1alpha1.ImageSync, reason, message string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Error(fmt.Errorf("%s", message), "image sync failed", "reason", reason)

	is.SetFailure(reason, message)
	is.Status.ObservedGeneration = is.Generation
	meta.SetStatusCondition(&is.Status.Conditions, metav1.Condition{
		Type:               butlerv1alpha1.ConditionTypeReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: is.Generation,
	})

	if err := r.Status().Update(ctx, is); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueLong}, nil
}

// setReady transitions the ImageSync to Ready phase.
func (r *Reconciler) setReady(ctx context.Context, is *butlerv1alpha1.ImageSync, providerImageRef string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("image sync ready", "providerImageRef", providerImageRef)

	is.SetPhase(butlerv1alpha1.ImageSyncPhaseReady)
	is.Status.ProviderImageRef = providerImageRef
	is.Status.ObservedGeneration = is.Generation
	is.Status.FailureReason = ""
	is.Status.FailureMessage = ""
	meta.SetStatusCondition(&is.Status.Conditions, metav1.Condition{
		Type:               butlerv1alpha1.ConditionTypeReady,
		Status:             metav1.ConditionTrue,
		Reason:             butlerv1alpha1.ReasonImageReady,
		Message:            fmt.Sprintf("Image synced to provider: %s", providerImageRef),
		ObservedGeneration: is.Generation,
	})

	if err := r.Status().Update(ctx, is); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueReady}, nil
}

// Helper methods

func (r *Reconciler) getProviderConfig(ctx context.Context, is *butlerv1alpha1.ImageSync) (*butlerv1alpha1.ProviderConfig, error) {
	pc := &butlerv1alpha1.ProviderConfig{}
	ns := is.Spec.ProviderConfigRef.Namespace
	if ns == "" {
		ns = is.Namespace
	}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      is.Spec.ProviderConfigRef.Name,
		Namespace: ns,
	}, pc); err != nil {
		return nil, fmt.Errorf("failed to get ProviderConfig %s/%s: %w", ns, is.Spec.ProviderConfigRef.Name, err)
	}
	return pc, nil
}

func (r *Reconciler) getButlerConfig(ctx context.Context) (*butlerv1alpha1.ButlerConfig, error) {
	bc := &butlerv1alpha1.ButlerConfig{}
	if err := r.Get(ctx, types.NamespacedName{Name: "butler"}, bc); err != nil {
		return nil, fmt.Errorf("failed to get ButlerConfig: %w", err)
	}
	return bc, nil
}

func (r *Reconciler) getProviderCredentials(ctx context.Context, pc *butlerv1alpha1.ProviderConfig) (map[string][]byte, error) {
	ns := pc.Spec.CredentialsRef.Namespace
	if ns == "" {
		ns = pc.Namespace
	}
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: pc.Spec.CredentialsRef.Name, Namespace: ns}, secret); err != nil {
		return nil, fmt.Errorf("failed to get secret %s/%s: %w", ns, pc.Spec.CredentialsRef.Name, err)
	}
	return secret.Data, nil
}

// createEC2Client builds an EC2 client from ProviderConfig credentials.
func (r *Reconciler) createEC2Client(ctx context.Context, pc *butlerv1alpha1.ProviderConfig) (*ec2.Client, error) {
	if pc.Spec.AWS == nil {
		return nil, fmt.Errorf("ProviderConfig %s has no AWS configuration", pc.Name)
	}

	creds, err := r.getProviderCredentials(ctx, pc)
	if err != nil {
		return nil, err
	}

	accessKeyID := string(creds["accessKeyID"])
	secretAccessKey := string(creds["secretAccessKey"])
	if accessKeyID == "" || secretAccessKey == "" {
		return nil, fmt.Errorf("credentials secret missing required keys 'accessKeyID' and/or 'secretAccessKey'")
	}

	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(pc.Spec.AWS.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			accessKeyID, secretAccessKey, "",
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	return ec2.NewFromConfig(awsCfg), nil
}

// findExistingAMI looks for an AMI tagged with this ImageSync name.
func (r *Reconciler) findExistingAMI(ctx context.Context, ec2Client *ec2.Client, isName string) (string, error) {
	resp, err := ec2Client.DescribeImages(ctx, &ec2.DescribeImagesInput{
		Filters: []ec2types.Filter{
			{
				Name:   aws.String("tag:butler.butlerlabs.dev/imagesync"),
				Values: []string{isName},
			},
			{
				Name:   aws.String("tag:butler.butlerlabs.dev/managed-by"),
				Values: []string{"butler"},
			},
		},
	})
	if err != nil {
		return "", fmt.Errorf("failed to describe images: %w", err)
	}

	for _, img := range resp.Images {
		if img.State == ec2types.ImageStateAvailable && img.ImageId != nil {
			return *img.ImageId, nil
		}
	}
	return "", nil
}

// buildArtifactURL constructs the factory download URL for an image.
func buildArtifactURL(factoryURL string, ref butlerv1alpha1.ImageFactoryRef, format string) string {
	factoryURL = strings.TrimSuffix(factoryURL, "/")
	if format == "" {
		format = "raw.gz"
	}
	arch := ref.Arch
	if arch == "" {
		arch = "amd64"
	}
	platform := ref.Platform
	if platform == "" {
		platform = "nocloud"
	}
	return fmt.Sprintf("%s/image/%s/%s/%s-%s.%s", factoryURL, ref.SchematicID, ref.Version, platform, arch, format)
}

// buildProviderImageName generates a deterministic image name.
func buildProviderImageName(is *butlerv1alpha1.ImageSync) string {
	if is.Spec.DisplayName != "" {
		return sanitizeName(is.Spec.DisplayName)
	}
	platform := is.Spec.FactoryRef.Platform
	if platform == "" {
		platform = "nocloud"
	}
	version := strings.ReplaceAll(is.Spec.FactoryRef.Version, ".", "-")
	arch := is.Spec.FactoryRef.Arch
	if arch == "" {
		arch = "amd64"
	}
	schematicPrefix := is.Spec.FactoryRef.SchematicID
	if len(schematicPrefix) > 8 {
		schematicPrefix = schematicPrefix[:8]
	}
	name := fmt.Sprintf("%s-%s-%s-%s-butler", platform, version, arch, schematicPrefix)
	return sanitizeName(name)
}

// invalidChars matches any character not allowed in image names.
var invalidChars = regexp.MustCompile(`[^a-z0-9.-]`)

// sanitizeName converts a string into a valid image name.
func sanitizeName(name string) string {
	name = strings.ToLower(name)
	name = strings.ReplaceAll(name, " ", "-")
	name = strings.ReplaceAll(name, "_", "-")
	name = invalidChars.ReplaceAllString(name, "")
	for strings.Contains(name, "--") {
		name = strings.ReplaceAll(name, "--", "-")
	}
	if len(name) > 63 {
		name = name[:63]
	}
	name = strings.Trim(name, "-.")
	return name
}

// SetupWithManager sets up the controller with the Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&butlerv1alpha1.ImageSync{}).
		Named("imagesync").
		Complete(r)
}
