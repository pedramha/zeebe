package io.zeebe.broker.workflow.processor.v2;

import java.util.EnumMap;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.workflow.data.WorkflowInstanceRecord;
import io.zeebe.broker.workflow.map.DeployedWorkflow;
import io.zeebe.broker.workflow.map.WorkflowCache;
import io.zeebe.broker.workflow.processor.v2.handler.ActivateActivityHandler;
import io.zeebe.broker.workflow.processor.v2.handler.ActivateGatewayHandler;
import io.zeebe.broker.workflow.processor.v2.handler.CompleteActivityHandler;
import io.zeebe.broker.workflow.processor.v2.handler.ConsumeTokenHandler;
import io.zeebe.broker.workflow.processor.v2.handler.CreateJobHandler;
import io.zeebe.broker.workflow.processor.v2.handler.ExclusiveSplitHandler;
import io.zeebe.broker.workflow.processor.v2.handler.StartActivityHandler;
import io.zeebe.broker.workflow.processor.v2.handler.TakeSequenceFlowHandler;
import io.zeebe.broker.workflow.processor.v2.handler.TriggerNoneEventHandler;
import io.zeebe.logstreams.processor.EventLifecycleContext;
import io.zeebe.model.bpmn.instance.BpmnStep;
import io.zeebe.model.bpmn.instance.FlowElement;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.transport.ClientResponse;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;

public class WorkflowInstanceLifecycle implements Lifecycle<WorkflowInstanceIntent, WorkflowInstanceRecord> {

  private final WorkflowCache workflowCache;
  private final EnumMap<BpmnStep, BpmnStepHandler<?>> stepHandlers;
  private final WorkflowInstances workflowInstances;

  private ActorControl actor;

  public WorkflowInstanceLifecycle(WorkflowCache workflowCache)
  {
    stepHandlers = new EnumMap<>(BpmnStep.class);
    stepHandlers.put(BpmnStep.ACTIVATE_GATEWAY, new ActivateGatewayHandler());
    stepHandlers.put(BpmnStep.EXCLUSIVE_SPLIT, new ExclusiveSplitHandler());
//    stepHandlers.put(BpmnStep.PARALLEL_MERGE, null);
//    stepHandlers.put(BpmnStep.PARALLEL_SPLIT, null);
    stepHandlers.put(BpmnStep.SCOPE_MERGE, new ConsumeTokenHandler());
    stepHandlers.put(BpmnStep.TAKE_SEQUENCE_FLOW, new TakeSequenceFlowHandler());
    stepHandlers.put(BpmnStep.TRIGGER_NONE_EVENT, new TriggerNoneEventHandler());
    stepHandlers.put(BpmnStep.START_ACTIVITY, new StartActivityHandler());
    stepHandlers.put(BpmnStep.ACTIVATE_ACTIVITY, new ActivateActivityHandler());
    stepHandlers.put(BpmnStep.CREATE_JOB, new CreateJobHandler());
    stepHandlers.put(BpmnStep.COMPLETE_ACTIVITY, new CompleteActivityHandler());

    this.workflowCache = workflowCache;
  }

  @Override
  public void onOpen(ActorControl streamProcessorActor) {
    this.actor = streamProcessorActor;
  }

  @Override
  public void onEnter(TypedRecord<WorkflowInstanceRecord> record) {
    // TODO: select step handler and call

    final long workflowKey = record.getValue().getWorkflowKey();
    final DeployedWorkflow deployedWorkflow = workflowCache.getWorkflowByKey(workflowKey);

    if (deployedWorkflow == null) {
      // TODO: not garabge-free
      fetchWorkflow(workflowKey, w -> callStepHandler(w, record), ctx);
    } else {
      callStepHandler(deployedWorkflow, record);
    }
  }

  private void callStepHandler(DeployedWorkflow workflow, TypedRecord<WorkflowInstanceRecord> record)
  {
    final WorkflowInstanceRecord value = record.getValue();
    final DirectBuffer activityId = value.getActivityId();

    final FlowElement element;
    final BpmnStep step;

    if (activityId.capacity() > 0)
    {
      element = workflow.getWorkflow().findFlowElementById(activityId);
      step = element.getBpmnStep((WorkflowInstanceIntent) record.getMetadata().getIntent());
    }
    else
    {
      element = null;
      step = workflow.getWorkflow().getBpmnStep((WorkflowInstanceIntent) record.getMetadata().getIntent());
    }

    final BpmnStepHandler<?> stepHandler = stepHandlers.get(step);

    if (stepHandler != null)
    {
      final WorkflowInstance workflowInstance = workflowInstances.getWorkflowInstance(record.getKey());

      // TODO: this is supposed to be the containing activity instance; for sequence flows, it is the scope;
      // for ACTIVITY_READY, it is the current activity instance; the current way to find the activity instance is quite hacky
      ActivityInstance activityInstance = null;

      if (workflowInstance != null)
      {
        activityInstance = workflowInstance.getActivityInstance(record.getKey());
        if (activityInstance == null)
        {
          activityInstance = workflowInstance.getActivityInstance(record.getValue().getWorkflowInstanceKey());
        }
      }

      // TODO: not garbage-free
      final BpmnStepContext stepContext = new BpmnStepContext<>();
      stepContext.setCurrentRecord(record);
      stepContext.setElement(element);
      stepContext.setWorkflow(workflow.getWorkflow());
      stepContext.setWorkflowInstance(workflowInstance);
      stepContext.setActivityInstance(activityInstance);
      stepHandler.handle(stepContext);
    }
  }


  public void fetchWorkflow(
      long workflowKey, Consumer<DeployedWorkflow> onFetched, EventLifecycleContext ctx) {
    final ActorFuture<ClientResponse> responseFuture =
        workflowCache.fetchWorkflowByKey(workflowKey);
    final ActorFuture<Void> onCompleted = new CompletableActorFuture<>();

    ctx.async(onCompleted);

    actor.runOnCompletion(
        responseFuture,
        (response, err) -> {
          if (err != null) {
            onCompleted.completeExceptionally(
                new RuntimeException("Could not fetch workflow", err));
          } else {
            try {
              final DeployedWorkflow workflow =
                  workflowCache.addWorkflow(response.getResponseBuffer());

              onFetched.accept(workflow);

              onCompleted.complete(null);
            } catch (Exception e) {
              onCompleted.completeExceptionally(
                  new RuntimeException("Error while processing fetched workflow", e));
            }
          }
        });
  }

  @Override
  public void onPublish(TypedRecord<WorkflowInstanceRecord> context, long key,
      WorkflowInstanceIntent intent, WorkflowInstanceRecord value) {

    switch (intent)
    {
      case ACTIVITY_READY:

      default:
        ;
    }
  }

}