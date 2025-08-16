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
package io.trino.operator;

import io.trino.client.NodeVersion;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryPerformanceFetcher;
import io.trino.execution.StageInfo;
import io.trino.execution.StagesInfo;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.planprinter.PlanPrinter.textDistributedPlan;
import static java.util.Objects.requireNonNull;

public class ExplainAnalyzeOperator
        implements Operator
{
    public static class ExplainAnalyzeOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final QueryPerformanceFetcher queryPerformanceFetcher;
        private final Metadata metadata;
        private final FunctionManager functionManager;
        private final boolean verbose;
        private final NodeVersion version;
        private boolean closed;

        public ExplainAnalyzeOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                QueryPerformanceFetcher queryPerformanceFetcher,
                Metadata metadata,
                FunctionManager functionManager,
                boolean verbose,
                NodeVersion version)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.queryPerformanceFetcher = requireNonNull(queryPerformanceFetcher, "queryPerformanceFetcher is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.functionManager = requireNonNull(functionManager, "functionManager is null");
            this.verbose = verbose;
            this.version = requireNonNull(version, "version is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, ExplainAnalyzeOperator.class.getSimpleName());
            return new ExplainAnalyzeOperator(operatorContext, queryPerformanceFetcher, metadata, functionManager, verbose, version);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new ExplainAnalyzeOperatorFactory(operatorId, planNodeId, queryPerformanceFetcher, metadata, functionManager, verbose, version);
        }
    }

    private final OperatorContext operatorContext;
    private final QueryPerformanceFetcher queryPerformanceFetcher;
    private final Metadata metadata;
    private final FunctionManager functionManager;
    private final boolean verbose;
    private final NodeVersion version;
    private boolean finishing;
    private boolean outputConsumed;

    public ExplainAnalyzeOperator(
            OperatorContext operatorContext,
            QueryPerformanceFetcher queryPerformanceFetcher,
            Metadata metadata,
            FunctionManager functionManager,
            boolean verbose,
            NodeVersion version)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.queryPerformanceFetcher = requireNonNull(queryPerformanceFetcher, "queryPerformanceFetcher is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.verbose = verbose;
        this.version = requireNonNull(version, "version is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && outputConsumed;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(needsInput());

        // Ignore the input
    }

    @Override
    public Page getOutput()
    {
        if (!finishing) {
            return null;
        }

        QueryInfo queryInfo = queryPerformanceFetcher.getQueryInfo(operatorContext.getDriverContext().getTaskId().getQueryId());
        checkState(queryInfo.getStages().isPresent(), "Stages informations is missing");
        checkState(queryInfo.getStages().get().getOutputStage().getSubStages().size() == 1, "Expected one sub stage of explain node");

        if (!hasFinalStageInfo(queryInfo.getStages().get())) {
            return null;
        }

        List<StageInfo> stagesWithoutOutputStage = queryInfo.getStages().orElseThrow().getStages().stream()
                .filter(stage -> !stage.getStageId().equals(queryInfo.getStages().orElseThrow().getOutputStageId()))
                .collect(toImmutableList());

        String plan = textDistributedPlan(
                stagesWithoutOutputStage,
                queryInfo.getQueryStats(),
                metadata,
                functionManager,
                operatorContext.getSession(),
                verbose,
                version);
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, 1);
        VARCHAR.writeString(builder, plan);

        outputConsumed = true;
        return new Page(builder.build());
    }

    private boolean hasFinalStageInfo(StagesInfo stages)
    {
        boolean isFinalStageInfo = isFinalStageInfo(stages);
        if (!isFinalStageInfo) {
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return isFinalStageInfo(stages);
    }

    private boolean isFinalStageInfo(StagesInfo stages)
    {
        List<StageInfo> subStages = stages.getSubStagesDeepPreOrder(operatorContext.getDriverContext().getTaskId().getStageId());
        return subStages.stream().allMatch(StageInfo::isFinalStageInfo);
    }
}
