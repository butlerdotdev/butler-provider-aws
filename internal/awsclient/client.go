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

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

// ClientConfig holds AWS-specific configuration for the EC2 client.
type ClientConfig struct {
	InstanceType    string
	AMI             string
	SubnetID        string
	SecurityGroupID string
}

// Client wraps the AWS EC2 SDK for VM lifecycle operations.
type Client struct {
	ec2Client *ec2.Client
	region    string
	vpcID     string
	config    ClientConfig
}

// NewClient creates a new AWS EC2 client from explicit credentials.
func NewClient(ctx context.Context, accessKeyID, secretAccessKey, region, vpcID string, cfg ClientConfig) (*Client, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			accessKeyID, secretAccessKey, "",
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	return &Client{
		ec2Client: ec2.NewFromConfig(awsCfg),
		region:    region,
		vpcID:     vpcID,
		config:    cfg,
	}, nil
}

// CreateVM creates an EC2 instance and returns the instance ID.
func (c *Client) CreateVM(ctx context.Context, opts VMCreateOptions) (string, error) {
	instanceType := types.InstanceType(c.config.InstanceType)
	if c.config.InstanceType == "" {
		instanceType = types.InstanceType(DefaultInstanceType)
	}

	input := &ec2.RunInstancesInput{
		ImageId:      aws.String(c.config.AMI),
		InstanceType: instanceType,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	}

	// Network interface with public IP for bootstrap reachability
	ni := types.InstanceNetworkInterfaceSpecification{
		DeviceIndex:              aws.Int32(0),
		AssociatePublicIpAddress: aws.Bool(true),
	}
	if c.config.SubnetID != "" {
		ni.SubnetId = aws.String(c.config.SubnetID)
	}
	if c.config.SecurityGroupID != "" {
		ni.Groups = []string{c.config.SecurityGroupID}
	}
	input.NetworkInterfaces = []types.InstanceNetworkInterfaceSpecification{ni}

	// UserData for cloud-init / Talos machine config
	if opts.UserData != "" {
		input.UserData = aws.String(base64.StdEncoding.EncodeToString([]byte(opts.UserData)))
	}

	// Root volume
	if opts.DiskGB > 0 {
		input.BlockDeviceMappings = []types.BlockDeviceMapping{
			{
				DeviceName: aws.String("/dev/xvda"),
				Ebs: &types.EbsBlockDevice{
					VolumeSize: aws.Int32(opts.DiskGB),
					VolumeType: types.VolumeTypeGp3,
				},
			},
		}
	}

	// Tags for identification and cleanup
	tags := []types.Tag{
		{Key: aws.String("Name"), Value: aws.String(opts.Name)},
		{Key: aws.String("butler.butlerlabs.dev/managed-by"), Value: aws.String("butler")},
	}
	for k, v := range opts.Labels {
		tags = append(tags, types.Tag{Key: aws.String(k), Value: aws.String(v)})
	}
	input.TagSpecifications = []types.TagSpecification{
		{
			ResourceType: types.ResourceTypeInstance,
			Tags:         tags,
		},
	}

	result, err := c.ec2Client.RunInstances(ctx, input)
	if err != nil {
		return "", fmt.Errorf("failed to create EC2 instance: %w", err)
	}

	if len(result.Instances) == 0 {
		return "", fmt.Errorf("RunInstances returned no instances")
	}

	return aws.ToString(result.Instances[0].InstanceId), nil
}

// GetVMStatus returns the current status of an EC2 instance by name tag.
func (c *Client) GetVMStatus(ctx context.Context, name string) (*VMStatus, error) {
	input := &ec2.DescribeInstancesInput{
		Filters: []types.Filter{
			{
				Name:   aws.String("tag:Name"),
				Values: []string{name},
			},
			{
				Name:   aws.String("instance-state-name"),
				Values: []string{"pending", "running", "stopping", "stopped"},
			},
		},
	}

	result, err := c.ec2Client.DescribeInstances(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to describe instances: %w", err)
	}

	for _, reservation := range result.Reservations {
		for _, instance := range reservation.Instances {
			return &VMStatus{
				Exists:     true,
				Status:     string(instance.State.Name),
				IPAddress:  aws.ToString(instance.PrivateIpAddress),
				ExternalIP: aws.ToString(instance.PublicIpAddress),
				InstanceID: aws.ToString(instance.InstanceId),
			}, nil
		}
	}

	return nil, &NotFoundError{Resource: "instance", Name: name}
}

// DeleteVM terminates an EC2 instance by name tag.
func (c *Client) DeleteVM(ctx context.Context, name string) error {
	status, err := c.GetVMStatus(ctx, name)
	if err != nil {
		if IsNotFound(err) {
			return nil // Already gone
		}
		return err
	}

	_, err = c.ec2Client.TerminateInstances(ctx, &ec2.TerminateInstancesInput{
		InstanceIds: []string{status.InstanceID},
	})
	if err != nil {
		return fmt.Errorf("failed to terminate instance %s: %w", status.InstanceID, err)
	}

	return nil
}
