/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.execution.scheduler.faulttolerant;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import io.trino.execution.StageId;
import io.trino.execution.scheduler.faulttolerant.EventDrivenFaultTolerantQueryScheduler.PreSchedulingTaskContext;
import io.trino.execution.scheduler.faulttolerant.EventDrivenFaultTolerantQueryScheduler.ScheduledTask;
import io.trino.execution.scheduler.faulttolerant.EventDrivenFaultTolerantQueryScheduler.Scheduler.PreSchedulingTaskContexts;
import io.trino.execution.scheduler.faulttolerant.NodeAllocator.NodeLease;
import io.trino.node.InternalNode;
import io.trino.spi.QueryId;
import org.junit.jupiter.api.Test;

import static io.trino.execution.scheduler.faulttolerant.TaskExecutionClass.EAGER_SPECULATIVE;
import static io.trino.execution.scheduler.faulttolerant.TaskExecutionClass.SPECULATIVE;
import static io.trino.execution.scheduler.faulttolerant.TaskExecutionClass.STANDARD;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPreSchedulingTaskContext
{
    @Test
    public void testNodeCompletionBeforeSchedulerEvent()
    {
        SettableFuture<InternalNode> node = SettableFuture.create();
        PreSchedulingTaskContext context = new PreSchedulingTaskContext(new TestingNodeLease(node), STANDARD);
        assertThat(context.toString())
                .contains("nodeFutureDone=false", "nodeFutureCancelled=false", "waitingForNode=true", "waitingForSinkInstanceHandle=false");

        node.set(null);
        assertThat(context.toString())
                .contains("nodeFutureDone=true", "waitingForNode=true");

        context.markWaitingForNodeCompleted();
        context.setWaitingForSinkInstanceHandle(true);
        assertThat(context.toString())
                .contains("nodeFutureDone=true", "waitingForNode=false", "waitingForSinkInstanceHandle=true");
    }

    @Test
    public void testCancelledNodeFuture()
    {
        SettableFuture<InternalNode> node = SettableFuture.create();
        PreSchedulingTaskContext context = new PreSchedulingTaskContext(new TestingNodeLease(node), STANDARD);
        node.cancel(false);
        assertThat(context.toString())
                .contains("nodeFutureDone=true", "nodeFutureCancelled=true", "waitingForNode=true");
    }

    @Test
    public void testFailedNodeFuture()
    {
        SettableFuture<InternalNode> node = SettableFuture.create();
        PreSchedulingTaskContext context = new PreSchedulingTaskContext(new TestingNodeLease(node), STANDARD);
        node.setException(new IllegalStateException("failed acquisition"));
        assertThat(context.toString())
                .contains("nodeFutureDone=true", "nodeFutureCancelled=false", "waitingForNode=true");
    }

    @Test
    public void testWaitingCountsAndContextDetails()
    {
        PreSchedulingTaskContexts contexts = new PreSchedulingTaskContexts();
        ScheduledTask task = new ScheduledTask(new StageId(new QueryId("query"), 0), 0);
        SettableFuture<InternalNode> node = SettableFuture.create();
        contexts.addContext(task, new TestingNodeLease(node), STANDARD);
        ScheduledTask speculativeTask = new ScheduledTask(task.stageId(), 1);
        SettableFuture<InternalNode> speculativeNode = SettableFuture.create();
        contexts.addContext(speculativeTask, new TestingNodeLease(speculativeNode), SPECULATIVE);
        ScheduledTask eagerSpeculativeTask = new ScheduledTask(task.stageId(), 2);
        SettableFuture<InternalNode> eagerSpeculativeNode = SettableFuture.create();
        contexts.addContext(eagerSpeculativeTask, new TestingNodeLease(eagerSpeculativeNode), EAGER_SPECULATIVE);
        assertThat(contexts.toString())
                .contains("taskCount=3", "waitCounters={STANDARD=1, SPECULATIVE=1, EAGER_SPECULATIVE=1}", "waitingForNode=true", "nodeFutureDone=false", "omittedContexts=0")
                .contains("executionClass=STANDARD", "executionClass=SPECULATIVE", "executionClass=EAGER_SPECULATIVE");

        node.set(null);
        contexts.markWaitingForNodeCompleted(task);
        contexts.getContext(task).setWaitingForSinkInstanceHandle(true);
        assertThat(contexts.toString())
                .contains("waitCounters={STANDARD=0, SPECULATIVE=1, EAGER_SPECULATIVE=1}", "waitingForNode=false", "nodeFutureDone=true", "waitingForSinkInstanceHandle=true");

        speculativeNode.set(null);
        contexts.markWaitingForNodeCompleted(speculativeTask);
        assertThat(contexts.toString())
                .contains("waitCounters={STANDARD=0, SPECULATIVE=0, EAGER_SPECULATIVE=1}");

        eagerSpeculativeNode.set(null);
        contexts.markWaitingForNodeCompleted(eagerSpeculativeTask);
        assertThat(contexts.toString())
                .contains("waitCounters={STANDARD=0, SPECULATIVE=0, EAGER_SPECULATIVE=0}");
    }

    @Test
    public void testContextDetailsAreBounded()
    {
        PreSchedulingTaskContexts contexts = new PreSchedulingTaskContexts();
        StageId stage = new StageId(new QueryId("query"), 0);
        for (int partition = 0; partition < 100; partition++) {
            contexts.addContext(new ScheduledTask(stage, partition), new TestingNodeLease(SettableFuture.create()), STANDARD);
        }
        String diagnostics = contexts.toString();
        assertThat(diagnostics).contains("taskCount=100", "STANDARD=100", "omittedContexts=90");
        assertThat(diagnostics.split("waitingForNode=true", -1)).hasSize(11);
    }

    private record TestingNodeLease(ListenableFuture<InternalNode> node)
            implements NodeLease
    {
        @Override
        public ListenableFuture<InternalNode> getNode()
        {
            return node;
        }

        @Override
        public void setExecutionClass(TaskExecutionClass executionClass)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setMemoryRequirement(DataSize memoryRequirement)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void release()
        {
            throw new UnsupportedOperationException();
        }
    }
}
