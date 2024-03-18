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
package io.trino.plugin.varada.storage.flows;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;

class FlowPriorityQueue
{
    private static final Logger logger = Logger.get(FlowPriorityQueue.class);

    private final PriorityQueue<Flow> priorityQueue = new PriorityQueue<>();
    private final Map<String, Flow> pendingFlows = new HashMap<>();
    private final Map<String, Flow> runningFlows = new HashMap<>();

    synchronized CompletableFuture<Boolean> addFlow(FlowType flowType, long flowId, Optional<String> additionalInfo)
    {
        String flowKey = createKey(flowType, flowId);
        if (pendingFlows.containsKey(flowKey)) {
            throw new RuntimeException(String.format("flow %s already exists", flowKey));
        }
        Flow flow = new Flow(new CompletableFuture<>(), flowId, flowType, additionalInfo);
        priorityQueue.add(flow);
        pendingFlows.put(flowKey, flow);
        Flow highPriotiyFlow = priorityQueue.peek();
        if (highPriotiyFlow != null) {
            boolean flowImmediateExecute = false;
            Flow runningFlow = runningFlows.values().stream().findFirst().orElse(null);
            if (runningFlow == null || (runningFlow.flowType == highPriotiyFlow.flowType && flowType == runningFlow.flowType)) {
                flowImmediateExecute = true;
                executeFlow(flow);
            }
            logger.debug("flow %s added and run? %s. high priority flow %s", flow, flowImmediateExecute, highPriotiyFlow);
        }
        return flow.waitingFuture;
    }

    synchronized void removeFlow(FlowType flowType, long flowId, boolean force)
    {
        String flowKey = createKey(flowType, flowId);
        Flow flow = pendingFlows.get(flowKey);
        if (flow != null) {
            priorityQueue.remove(flow);
            pendingFlows.remove(flowKey);
            runningFlows.remove(flowKey);
            if (force) {
                flow.waitingFuture.cancel(true);
            }
            long currentTime = System.currentTimeMillis();
            long flowExecutionTime = currentTime - flow.executionTime;
            logger.debug("flow %s waited %dms and executed %dms", flowKey, (flow.executionTime - flow.schedulingTime), flowExecutionTime);
            if (flowType.getPriority() > 0 && flowExecutionTime > 60_000) {
                logger.warn("flow %s took too long: %dms", flowKey, flowExecutionTime);
            }
        }
        else {
            logger.error("trying to remove a non existing flow %s", createKey(flowType, flowId));
        }
        Flow nextToRun = priorityQueue.peek();
        logger.debug("flow %s removed. next run %s", flowKey, nextToRun);
        if (nextToRun != null && (nextToRun.flowType == flowType || runningFlows.isEmpty())) {
            if (nextToRun.flowType == flowType) {
                executeFlow(nextToRun);
            }
            else {
                pendingFlows.values().stream().filter(pendingFlow -> pendingFlow.flowType == nextToRun.flowType).forEach(this::executeFlow);
            }
        }
    }

    private void executeFlow(Flow nextToRun)
    {
        nextToRun.setExecutionTime();
        nextToRun.waitingFuture.complete(true);
        runningFlows.put(createKey(nextToRun.flowType, nextToRun.id), nextToRun);
    }

    @VisibleForTesting
    public Map<String, Flow> getRunningFlows()
    {
        return ImmutableMap.copyOf(runningFlows);
    }

    @VisibleForTesting
    public Map<String, Flow> getPendingFlows()
    {
        return ImmutableMap.copyOf(pendingFlows);
    }

    private String createKey(FlowType flowType, long id)
    {
        return flowType + "___" + id;
    }

    private static class Flow
            implements Comparable<Flow>
    {
        private final CompletableFuture<Boolean> waitingFuture;
        private final long id;
        private final FlowType flowType;
        private final long schedulingTime = System.currentTimeMillis();
        private final Optional<String> additionalInfo;
        private long executionTime = schedulingTime;

        Flow(CompletableFuture<Boolean> waitingFuture, long id, FlowType flowType, Optional<String> additionalInfo)
        {
            this.waitingFuture = waitingFuture;
            this.id = id;
            this.flowType = flowType;
            this.additionalInfo = additionalInfo;
        }

        @Override
        public int compareTo(Flow other)
        {
            return Integer.compare(this.flowType.getPriority(), other.flowType.getPriority());
        }

        private void setExecutionTime()
        {
            executionTime = System.currentTimeMillis();
        }

        @Override
        public String toString()
        {
            return "Flow{" +
                    "id=" + id +
                    ", flowType=" + flowType +
                    ", schedulingTime=" + schedulingTime +
                    ", executionTime=" + executionTime +
                    ", additionalInfo=" + additionalInfo +
                    '}';
        }
    }
}
