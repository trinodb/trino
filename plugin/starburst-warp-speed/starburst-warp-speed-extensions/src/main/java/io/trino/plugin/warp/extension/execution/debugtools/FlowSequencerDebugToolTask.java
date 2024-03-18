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
package io.trino.plugin.warp.extension.execution.debugtools;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.varada.execution.TaskData;
import io.trino.plugin.varada.storage.flows.FlowType;
import io.trino.plugin.varada.storage.flows.FlowsSequencer;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@TaskResourceMarker(worker = false)
@Path("flows")
//@Api(value = "Flow-Sequencer", tags = "flow-sequencer")
public class FlowSequencerDebugToolTask
        implements TaskResource
{
    public static final String DUMP_TASK_NAME = "flow-sequencer-dump";
    public static final String ABORT_FLOW_TASK_NAME = "flow-sequencer-abort-flow";
    private static final Logger logger = Logger.get(FlowSequencerDebugToolTask.class);
    private final FlowsSequencer flowsSequencer;

    @Inject
    public FlowSequencerDebugToolTask(FlowsSequencer flowsSequencer)
    {
        this.flowsSequencer = requireNonNull(flowsSequencer);
    }

    @Path(DUMP_TASK_NAME)
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    //@ApiOperation(value = "dump-flows", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public FlowSequencerDumpResult dump()
    {
        List<String> runningFlows = (List<String>) flowsSequencer.getRunningFlows().values().stream().map(Object::toString).collect(Collectors.toList());
        List<String> pendingFlows = (List<String>) flowsSequencer.getPendingFlows().values().stream().map(Object::toString).collect(Collectors.toList());
        return new FlowSequencerDumpResult(runningFlows, pendingFlows);
    }

    @Path(ABORT_FLOW_TASK_NAME)
    @POST
    public void abortFlow(AbortFlowData abortFlowData)
    {
        logger.warn("aborting flow %s", abortFlowData);
        flowsSequencer.flowFinished(abortFlowData.flowType, abortFlowData.flowId, true);
    }

    public static class AbortFlowData
            extends TaskData
    {
        private final FlowType flowType;
        private final long flowId;

        @JsonCreator
        public AbortFlowData(@JsonProperty(value = "flowType") FlowType flowType,
                @JsonProperty(value = "flowId") long flowId)
        {
            this.flowType = flowType;
            this.flowId = flowId;
        }

        @JsonProperty(value = "flowType")
        public FlowType getFlowType()
        {
            return flowType;
        }

        @JsonProperty(value = "flowId")
        public long getFlowId()
        {
            return flowId;
        }

        @Override
        public String toString()
        {
            return "AbortFlowRequest{" +
                    "flowType=" + flowType +
                    ", flowId=" + flowId +
                    '}';
        }

        @Override
        protected String getTaskName()
        {
            return ABORT_FLOW_TASK_NAME;
        }
    }

    public static class FlowSequencerDumpResult
    {
        private final List<String> runningFlows;
        private final List<String> pendingFlows;

        @JsonCreator
        public FlowSequencerDumpResult(@JsonProperty(value = "running-flows") List<String> runningFlows,
                @JsonProperty(value = "pending-flows") List<String> pendingFlows)
        {
            this.runningFlows = runningFlows;
            this.pendingFlows = pendingFlows;
        }

        @JsonProperty(value = "running-flows")
        public List<String> getRunningFlows()
        {
            return runningFlows;
        }

        @JsonProperty(value = "pending-flows")
        public List<String> getPendingFlows()
        {
            return pendingFlows;
        }
    }
}
