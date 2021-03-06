package worker

import (
	"context"
	"github.com/zeebe-io/zeebe/clients/go/entities"
	"github.com/zeebe-io/zeebe/clients/go/pb"
	"io"
	"log"
	"sync"
	"time"
)

type jobPoller struct {
	client         pb.GatewayClient
	request        pb.ActivateJobsRequest
	requestTimeout time.Duration
	bufferSize     int
	pollInterval   time.Duration

	jobQueue       chan entities.Job
	workerFinished chan bool
	closeSignal    chan struct{}
	remaining      int
	threshold      int
}

func (poller *jobPoller) poll(closeWait *sync.WaitGroup) {
	defer closeWait.Done()

	// initial poll
	poller.activateJobs()

	for {
		select {
		// either a job was finished
		case <-poller.workerFinished:
			poller.remaining--
		// or the poll interval exceeded
		case <-time.After(poller.pollInterval):
		// or poller should stop
		case <-poller.closeSignal:
			return
		}

		if poller.shouldActivateJobs() {
			poller.activateJobs()
		}
	}
}

func (poller *jobPoller) shouldActivateJobs() bool {
	return poller.remaining <= poller.threshold
}

func (poller *jobPoller) activateJobs() {
	ctx, cancel := context.WithTimeout(context.Background(), poller.requestTimeout)
	defer cancel()

	poller.request.Amount = int32(poller.bufferSize - poller.remaining)
	stream, err := poller.client.ActivateJobs(ctx, &poller.request)
	if err != nil {
		log.Println("Failed to request jobs for worker", poller.request.Worker, err)
		return
	}

	for {
		response, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Println("Failed to activate jobs for worker", poller.request.Worker, err)
			break
		}

		poller.remaining += len(response.Jobs)
		for _, job := range response.Jobs {
			poller.jobQueue <- entities.Job{*job}
		}
	}
}
