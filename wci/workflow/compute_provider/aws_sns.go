package computeprovider

import (
 "context"
 "encoding/json"
 "errors"
 "fmt"

 "github.com/aws/aws-sdk-go-v2/aws"
 "github.com/aws/aws-sdk-go-v2/aws/arn"
 "github.com/aws/aws-sdk-go-v2/service/sns"
 "go.temporal.io/auto-scaled-workers/wci/client"
 "go.temporal.io/auto-scaled-workers/wci/workflow/iface"
 "go.temporal.io/server/common/dynamicconfig"
)

const (
 configAWSSNSTopicARN = "topic_arn"
 configAWSSNSRole = "role"
 configAWSSNSRoleExternalID = "role_external_id"
)

type awsSNSComputeProvider struct {
 intermediaryRoles [][]client.AWSIAMRoleRequest
}
type snsAPI interface {
 Publish(context.Context, *sns.PublishInput, ...func(*sns.Options)) (*sns.PublishOutput, error)
 GetTopicAttributes(context.Context, *sns.GetTopicAttributesInput, ...func(*sns.Options)) (*sns.GetTopicAttributesOutput, error)
}
func init() { RegisterComputeProvider(iface.ComputeProviderTypeAWSSNS, NewAWSSNSComputeProvider) }
func NewAWSSNSComputeProvider(_ context.Context, dc *dynamicconfig.Collection) (ComputeProvider, error) {
 var roles [][]client.AWSIAMRoleRequest
 if dc != nil {
  roles = client.WorkerControllerAWSIntermediaryRoles.Get(dc)()
 }
 return &awsSNSComputeProvider{intermediaryRoles: roles}, nil
}
func (p *awsSNSComputeProvider) LaunchStrategy() LaunchStrategy { return LaunchStrategyInvoke }
func (p *awsSNSComputeProvider) ValidateConfig(ctx context.Context, _ RequestContext, cfg ComputeProviderConfig) error {
 c, topic, region, err := p.getSNSClientAndTopicARN(ctx, cfg)
 if err != nil { return fmt.Errorf("cannot connect to the compute provider: %w", err) }
 if _, err = c.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: aws.String(topic)}); err != nil { return fmt.Errorf("cannot access the compute resource: %w", err) }
 if err := p.checkExternalID(ctx, cfg, region); err != nil { return fmt.Errorf("IAM role trust policy does not enforce ExternalID condition: %w", err) }
 return nil
}
func (p *awsSNSComputeProvider) InvokeWorker(ctx context.Context, rc RequestContext, cfg ComputeProviderConfig) error {
 err := p.invokeWorker(ctx, rc, cfg)
 return NewProviderError(classifyAWSFailure(err), err)
}
func (p *awsSNSComputeProvider) invokeWorker(ctx context.Context, rc RequestContext, cfg ComputeProviderConfig) error {
 c, topic, _, err := p.getSNSClientAndTopicARN(ctx, cfg)
 if err != nil { return err }
 message, err := json.Marshal(struct {
  Event string `json:"event"`
  NamespaceName string `json:"namespaceName"`
  DeploymentName string `json:"deploymentName"`
  BuildID string `json:"buildId"`
 }{ "worker-activation", rc.NamespaceName, rc.DeploymentName, rc.DeploymentBuildID })
 if err != nil { return fmt.Errorf("failed to marshal SNS worker activation message: %w", err) }
 if _, err := c.Publish(ctx, &sns.PublishInput{TopicArn: aws.String(topic), Message: aws.String(string(message))}); err != nil {
  return fmt.Errorf("failed to publish worker activation message to SNS topic %q: %w", topic, err)
 }
 return nil
}
func (p *awsSNSComputeProvider) UpdateWorkerSetSize(context.Context, RequestContext, ComputeProviderConfig, int32) error { return errors.ErrUnsupported }
func (p *awsSNSComputeProvider) checkExternalID(ctx context.Context, cfg ComputeProviderConfig, region string) error {
 role, _ := cfg[configAWSSNSRole].(string); id, _ := cfg[configAWSSNSRoleExternalID].(string)
 if role == "" || id == "" { return nil }
 return verifyExternalIDEnforcedFn(ctx, region, role, p.intermediaryRoles)
}
var newSNSClientFn = newSNSClient
func newSNSClient(ctx context.Context, region, role string, externalID *string, roles [][]client.AWSIAMRoleRequest) (snsAPI, error) {
 cfg, err := buildAWSConfig(ctx, region, role, externalID, roles)
 if err != nil { return nil, err }
 return sns.NewFromConfig(cfg), nil
}
func (p *awsSNSComputeProvider) getSNSClientAndTopicARN(ctx context.Context, cfg ComputeProviderConfig) (snsAPI, string, string, error) {
 topic, ok := cfg[configAWSSNSTopicARN].(string)
 if !ok || topic == "" { return nil, "", "", fmt.Errorf("AWS SNS topic ARN not found or invalid") }
 parsed, err := arn.Parse(topic)
 if err != nil { return nil, "", "", fmt.Errorf("invalid SNS topic ARN %q: %w", topic, err) }
 if parsed.Service != "sns" || parsed.Region == "" || parsed.AccountID == "" || parsed.Resource == "" { return nil, "", "", fmt.Errorf("AWS SNS topic ARN must reference an SNS topic") }
 role, _ := cfg[configAWSSNSRole].(string)
 if role != "" { if err := validateRoleARN(role); err != nil { return nil, "", "", err } }
 var externalID *string
 if id, ok := cfg[configAWSSNSRoleExternalID].(string); ok && id != "" { externalID = &id }
 // With no explicit target role, use the pod's ambient AWS credentials directly.
 // Intermediary and target role assumption are only used when a role override is configured.
 roles := p.intermediaryRoles
 if role == "" {
  roles = nil
  externalID = nil
 }
 c, err := newSNSClientFn(ctx, parsed.Region, role, externalID, roles)
 if err != nil { return nil, "", "", err }
 return c, topic, parsed.Region, nil
}
