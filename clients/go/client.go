package zbc

import (
	"github.com/zeebe-io/zeebe/clients/go/commands"
	"github.com/zeebe-io/zeebe/clients/go/pb"
	"github.com/zeebe-io/zeebe/clients/go/worker"
	"google.golang.org/grpc"
	"time"
)

const DefaultRequestTimeout = 15 * time.Second

type ZBClientImpl struct {
	gateway        pb.GatewayClient
	requestTimeout time.Duration
	connection     *grpc.ClientConn
}

func (client *ZBClientImpl) NewTopologyCommand() *commands.TopologyCommand {
	return commands.NewTopologyCommand(client.gateway, client.requestTimeout)
}

func (client *ZBClientImpl) NewDeployWorkflowCommand() *commands.DeployCommand {
	return commands.NewDeployCommand(client.gateway, client.requestTimeout)
}

func (client *ZBClientImpl) NewPublishMessageCommand() commands.PublishMessageCommandStep1 {
	return commands.NewPublishMessageCommand(client.gateway, client.requestTimeout)
}

func (client *ZBClientImpl) NewCreateInstanceCommand() commands.CreateInstanceCommandStep1 {
	return commands.NewCreateInstanceCommand(client.gateway, client.requestTimeout)
}

func (client *ZBClientImpl) NewCancelInstanceCommand() commands.CancelInstanceStep1 {
	return commands.NewCancelInstanceCommand(client.gateway, client.requestTimeout)
}

func (client *ZBClientImpl) NewCreateJobCommand() commands.CreateJobCommandStep1 {
	return commands.NewCreateJobCommand(client.gateway, client.requestTimeout)
}

func (client *ZBClientImpl) NewCompleteJobCommand() commands.CompleteJobCommandStep1 {
	return commands.NewCompleteJobCommand(client.gateway, client.requestTimeout)
}

func (client *ZBClientImpl) NewFailJobCommand() commands.FailJobCommandStep1 {
	return commands.NewFailJobCommand(client.gateway, client.requestTimeout)
}

func (client *ZBClientImpl) NewUpdateJobRetriesCommand() commands.UpdateJobRetriesCommandStep1 {
	return commands.NewUpdateJobRetriesCommand(client.gateway, client.requestTimeout)
}

func (client *ZBClientImpl) NewUpdatePayloadCommand() commands.UpdatePayloadCommandStep1 {
	return commands.NewUpdatePayloadCommand(client.gateway, client.requestTimeout)
}

func (client *ZBClientImpl) NewActivateJobsCommand() commands.ActivateJobsCommandStep1 {
	return commands.NewActivateJobsCommand(client.gateway, client.requestTimeout)
}

func (client *ZBClientImpl) NewListWorkflowsCommand() commands.ListWorkflowsStep1 {
	return commands.NewListWorkflowsCommand(client.gateway, client.requestTimeout)
}

func (client *ZBClientImpl) NewGetWorkflowCommand() commands.GetWorkflowStep1 {
	return commands.NewGetWorkflowCommand(client.gateway, client.requestTimeout)
}

func (client *ZBClientImpl) NewJobWorker() worker.JobWorkerBuilderStep1 {
	return worker.NewJobWorkerBuilder(client.gateway, client, client.requestTimeout)
}

func (client *ZBClientImpl) SetRequestTimeout(requestTimeout time.Duration) ZBClient {
	client.requestTimeout = requestTimeout
	return client
}

func (client *ZBClientImpl) Close() error {
	return client.connection.Close()
}

func NewZBClient(gatewayAddress string) (ZBClient, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())

	conn, err := grpc.Dial(gatewayAddress, opts...)
	if err != nil {
		return nil, err
	}
	return &ZBClientImpl{
		gateway:        pb.NewGatewayClient(conn),
		requestTimeout: DefaultRequestTimeout,
		connection:     conn,
	}, nil
}
