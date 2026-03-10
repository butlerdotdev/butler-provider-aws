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

// VMCreateOptions defines options for creating an EC2 instance.
type VMCreateOptions struct {
	Name     string
	CPU      int32
	MemoryMB int32
	DiskGB   int32
	UserData string
	Labels   map[string]string
}

// VMStatus represents the current status of an EC2 instance.
type VMStatus struct {
	Exists     bool
	Status     string // running, pending, stopping, stopped, terminated
	IPAddress  string // Private IP
	ExternalIP string // Public IP (if present)
	InstanceID string // EC2 instance ID
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
