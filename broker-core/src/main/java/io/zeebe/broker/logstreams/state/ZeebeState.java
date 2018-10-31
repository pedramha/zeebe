package io.zeebe.broker.logstreams.state;

import io.zeebe.broker.job.JobStateController;
import io.zeebe.broker.subscription.message.data.WorkflowInstanceSubscriptionRecord;
import io.zeebe.broker.subscription.message.state.MessageStateController;
import io.zeebe.broker.subscription.message.state.SubscriptionState;
import io.zeebe.broker.util.KeyStateController;
import io.zeebe.broker.workflow.deployment.distribute.processor.state.DeploymentsStateController;
import io.zeebe.broker.workflow.state.DeployedWorkflow;
import io.zeebe.broker.workflow.state.ElementInstanceState;
import io.zeebe.broker.workflow.state.NextValueManager;
import io.zeebe.broker.workflow.state.TimerInstanceState;
import io.zeebe.broker.workflow.state.WorkflowPersistenceCache;
import io.zeebe.broker.workflow.state.WorkflowState;
import io.zeebe.broker.workflow.state.WorkflowSubscription;
import io.zeebe.protocol.impl.record.value.deployment.DeploymentRecord;
import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.agrona.DirectBuffer;
import org.rocksdb.RocksDB;

public class ZeebeState extends KeyStateController {

  private final DeploymentsStateController deploymentState = new DeploymentsStateController();
  private final WorkflowState workflowState = new WorkflowState();
  private final JobStateController jobState = new JobStateController();
  private final MessageStateController messageState = new MessageStateController();

  @Override
  public RocksDB open(final File dbDirectory, final boolean reopen) throws Exception {
    final List<byte[]> familyNames = WorkflowState.getColumnFamilyNames();
    familyNames.addAll(JobStateController.getColumnFamilyNames());
    familyNames.addAll(MessageStateController.getColumnFamilyNames());

    final List<byte[]> columnFamilyNames =
        Stream.of(
                COLUMN_FAMILY_NAMES,
                WorkflowPersistenceCache.COLUMN_FAMILY_NAMES,
                ElementInstanceState.COLUMN_FAMILY_NAMES,
                SubscriptionState.COLUMN_FAMILY_NAMES,
                TimerInstanceState.COLUMN_FAMILY_NAMES)
            .flatMap(Stream::of)
            .collect(Collectors.toList());

    final RocksDB rocksDB = super.open(dbDirectory, reopen, columnFamilyNames);

    workflowKeyHandle = this.getColumnFamilyHandle(WORKFLOW_KEY_FAMILY_NAME);
    workflowVersionHandle = this.getColumnFamilyHandle(WORKFLOW_VERSION_FAMILY_NAME);

    nextValueManager = new NextValueManager(this);
    workflowPersistenceCache = new WorkflowPersistenceCache(this);
    subscriptionState = new SubscriptionState<>(this, WorkflowSubscription.class);
    timerInstanceState = new TimerInstanceState(this);
    elementInstanceState = new ElementInstanceState(this);

    return rocksDB;
  }

  public long getNextWorkflowKey() {
    return nextValueManager.getNextValue(workflowKeyHandle, LATEST_WORKFLOW_KEY);
  }

  public int getNextWorkflowVersion(String bpmnProcessId) {
    return (int) nextValueManager.getNextValue(workflowVersionHandle, bpmnProcessId.getBytes());
  }

  public boolean putDeployment(long deploymentKey, DeploymentRecord deploymentRecord) {
    return workflowPersistenceCache.putDeployment(deploymentKey, deploymentRecord);
  }

  public DeployedWorkflow getWorkflowByProcessIdAndVersion(
      DirectBuffer bpmnProcessId, int version) {
    return workflowPersistenceCache.getWorkflowByProcessIdAndVersion(bpmnProcessId, version);
  }

  public DeployedWorkflow getWorkflowByKey(long workflowKey) {
    return workflowPersistenceCache.getWorkflowByKey(workflowKey);
  }

  public DeployedWorkflow getLatestWorkflowVersionByProcessId(DirectBuffer bpmnProcessId) {
    return workflowPersistenceCache.getLatestWorkflowVersionByProcessId(bpmnProcessId);
  }

  public Collection<DeployedWorkflow> getWorkflows() {
    return workflowPersistenceCache.getWorkflows();
  }

  public Collection<DeployedWorkflow> getWorkflowsByBpmnProcessId(DirectBuffer processId) {
    return workflowPersistenceCache.getWorkflowsByBpmnProcessId(processId);
  }

  public void put(WorkflowSubscription workflowSubscription) {
    subscriptionState.put(workflowSubscription);
  }

  public void updateCommandSendTime(WorkflowSubscription workflowSubscription) {
    subscriptionState.updateCommandSentTime(workflowSubscription);
  }

  public WorkflowSubscription findSubscription(WorkflowInstanceSubscriptionRecord record) {
    return findSubscription(record.getWorkflowInstanceKey(), record.getElementInstanceKey());
  }

  public WorkflowSubscription findSubscription(long workflowInstanceKey, long elementInstanceKey) {
    final WorkflowSubscription workflowSubscription =
        new WorkflowSubscription(workflowInstanceKey, elementInstanceKey);
    return subscriptionState.getSubscription(workflowSubscription);
  }

  public List<WorkflowSubscription> findSubscriptionsBefore(long time) {
    return subscriptionState.findSubscriptionBefore(time);
  }

  public boolean remove(WorkflowInstanceSubscriptionRecord record) {
    final WorkflowSubscription persistedSubscription = findSubscription(record);

    final boolean exist = persistedSubscription != null;
    if (exist) {
      subscriptionState.remove(persistedSubscription);
    }
    return exist;
  }

  public void remove(WorkflowSubscription workflowSubscription) {
    subscriptionState.remove(workflowSubscription);
  }

  public TimerInstanceState getTimerState() {
    return timerInstanceState;
  }

  /**
   * @return only a meaningful value after {@link WorkflowState#open(File, boolean)} was called,
   *     i.e. during the lifetime of the owning stream processor.
   */
  public ElementInstanceState getElementInstanceState() {
    return elementInstanceState;
  }
}
