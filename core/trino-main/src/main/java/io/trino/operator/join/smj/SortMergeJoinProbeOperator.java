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
package io.trino.operator.join.smj;

import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.spiller.Spiller;
import io.trino.spiller.SpillerFactory;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.type.BlockTypeOperators;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SortMergeJoinProbeOperator
        implements Operator
{
    private final OperatorContext operatorContext;
    private final PageIterator probePages;
    private final Optional<SpillerFactory> spillerFactory;
    private final List<Type> buildTypes;

//    private final LocalMemoryContext revocableMemoryContext;
    private final LocalMemoryContext localUserMemoryContext;

    private SortMergeJoinScanner scanner;

    public SortMergeJoinProbeOperator(
            int driverInstanceIndex,
            Integer maxBufferPageCount,
            Integer numRowsInMemoryBufferThreshold,
            Optional<SpillerFactory> spillerFactory,
            OperatorContext operatorContext, JoinNode.Type joinType, SortMergeJoinBridge bridge, BlockTypeOperators blockTypeOperators,
            List<Type> probeTypes, List<Integer> probeEquiJoinClauseChannels, List<Integer> probeOutputChannels,
            List<Type> buildTypes, List<Integer> buildEquiJoinClauseChannels, List<Integer> buildOutputChannels)
    {
        this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
        this.operatorContext = operatorContext;
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
//        this.revocableMemoryContext = operatorContext.localRevocableMemoryContext();
        this.buildTypes = buildTypes;
        this.probePages = new PageIterator(maxBufferPageCount);
        switch (joinType) {
            case INNER:
                this.scanner = new SortMergeInnerJoinScanner(bridge.getTaskId(), numRowsInMemoryBufferThreshold, this::createBuildSideSpiller, probePages, bridge.getBuildPages(driverInstanceIndex), blockTypeOperators,
                        probeTypes, probeEquiJoinClauseChannels, probeOutputChannels,
                        buildTypes, buildEquiJoinClauseChannels, buildOutputChannels);
                break;
            case LEFT:
                this.scanner = new SortMergeLeftOuterJoinScanner(bridge.getTaskId(), numRowsInMemoryBufferThreshold, this::createBuildSideSpiller, probePages, bridge.getBuildPages(driverInstanceIndex), blockTypeOperators,
                        probeTypes, probeEquiJoinClauseChannels, probeOutputChannels,
                        buildTypes, buildEquiJoinClauseChannels, buildOutputChannels);
                break;
            case RIGHT:
                this.scanner = new SortMergeRightOuterJoinScanner(bridge.getTaskId(), numRowsInMemoryBufferThreshold, this::createBuildSideSpiller, probePages, bridge.getBuildPages(driverInstanceIndex), blockTypeOperators,
                        probeTypes, probeEquiJoinClauseChannels, probeOutputChannels,
                        buildTypes, buildEquiJoinClauseChannels, buildOutputChannels);
                break;
            case FULL:
                this.scanner = new SortMergeFullOuterJoinScanner(bridge.getTaskId(), numRowsInMemoryBufferThreshold, this::createBuildSideSpiller, probePages, bridge.getBuildPages(driverInstanceIndex), blockTypeOperators,
                        probeTypes, probeEquiJoinClauseChannels, probeOutputChannels,
                        buildTypes, buildEquiJoinClauseChannels, buildOutputChannels);
                break;
        }
    }

    private Spiller createBuildSideSpiller()
    {
        return spillerFactory.get().create(
                buildTypes,
                operatorContext.getSpillContext(),
                operatorContext.newAggregateUserMemoryContext());
    }

    private void updateMemoryUsage()
    {
        long size = scanner.getBufferedMatches().getMemorySize();
        localUserMemoryContext.setBytes(size);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return probePages.getFull().isDone();
    }

    @Override
    public void addInput(Page page)
    {
        this.probePages.add(page);
    }

    @Override
    public Page getOutput()
    {
        Page page = scanner.process();
        updateMemoryUsage();
        return page;
    }

    @Override
    public void finish()
    {
        this.probePages.noMorePage();
    }

    @Override
    public boolean isFinished()
    {
        return scanner.isFinished();
    }
}
