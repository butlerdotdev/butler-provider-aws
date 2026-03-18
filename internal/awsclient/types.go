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

package awsclient

import "fmt"

// EC2 instance state constants.
const (
	InstanceStateRunning    = "running"
	InstanceStatePending    = "pending"
	InstanceStateStopping   = "stopping"
	InstanceStateStopped    = "stopped"
	InstanceStateTerminated = "terminated"
)

// Default values for AWS VM provisioning.
const (
	DefaultInstanceType = "m5.xlarge"
	DefaultAMI          = "" // Must be specified per region
)

// DefaultTalosAMIs maps AWS regions to custom Talos Linux v1.12.2 AMIs (amd64)
// with iscsi-tools and util-linux-tools extensions (required for Longhorn storage).
// Schematic: 613e1592b2da41ae5e265e8789429f22e121aab91cb4deb6bc3c0b6262961245
// Built via Talos Image Factory → S3 → import-snapshot → register-image.
var DefaultTalosAMIs = map[string]string{
	"us-east-1":      "ami-0cd30b7027afffd4e",
	"us-east-2":      "ami-0f0e7059a7735d0a0",
	"us-west-1":      "ami-0abe7bca2fb75fced",
	"us-west-2":      "ami-0d03dfcef2e1eee26",
	"eu-west-1":      "ami-0c0a06de3c0c30646",
	"eu-central-1":   "ami-06e7ff093b83a3cb0",
	"ap-southeast-1": "ami-088879e1d3a9b3c3e",
	"ap-northeast-1": "ami-0aed11a4c14c1f6b5",
}

// VMCreateOptions defines options for creating an EC2 instance.
type VMCreateOptions struct {
	Name     string
	CPU      int32
	MemoryMB int32
	DiskGB   int32
	UserData string
	Labels   map[string]string
	Image    string // AMI override from MachineRequest.Spec.Image
}

// VMStatus represents the current status of an EC2 instance.
type VMStatus struct {
	Exists           bool
	Status           string // running, pending, stopping, stopped, terminated
	IPAddress        string // Private IP
	ExternalIP       string // Public IP (if present)
	InstanceID       string // EC2 instance ID
	AvailabilityZone string // e.g., us-east-1a
}

// NotFoundError indicates an EC2 resource was not found.
type NotFoundError struct {
	Resource string
	Name     string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("%s %q not found", e.Resource, e.Name)
}

// IsNotFound returns true if the error is a NotFoundError.
func IsNotFound(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*NotFoundError)
	return ok
}
