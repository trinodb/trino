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
package io.trino.operator.join;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.trino.operator.NullSafeHashCompiler;
import io.trino.operator.OperatorFactory;
import io.trino.operator.WorkProcessorOperatorAdapter;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spiller.GenericPartitioningSpillerFactory;
import io.trino.spiller.PartitioningSpillerFactory;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.operator.JoinOperatorType.innerJoin;
import static io.trino.operator.JoinOperatorType.lookupOuterJoin;
import static io.trino.operator.OperatorFactories.spillingJoin;
import static io.trino.operator.join.JoinTestUtils.DummySpillerFactory;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestLookupJoinOperatorFactory
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final NullSafeHashCompiler HASH_COMPILER = new NullSafeHashCompiler(TYPE_OPERATORS);
    private static final PartitioningSpillerFactory PARTITIONING_SPILLER_FACTORY =
            new GenericPartitioningSpillerFactory(new DummySpillerFactory());

    @Test
    public void testWithAdjustedTotalOperatorsCountReturnsNewInstance()
    {
        LookupJoinOperatorFactory factory = createFactory(OptionalInt.of(8));

        LookupJoinOperatorFactory adjusted = factory.withAdjustedTotalOperatorsCount(1);

        // should return a new instance with adjusted count
        assertThat(adjusted).isNotSameAs(factory);
        assertThat(getTotalOperatorsCount(adjusted)).isEqualTo(OptionalInt.of(9));
        // original should be unchanged
        assertThat(getTotalOperatorsCount(factory)).isEqualTo(OptionalInt.of(8));
    }

    @Test
    public void testWithAdjustedTotalOperatorsCountMultipleAdjustments()
    {
        LookupJoinOperatorFactory factory = createFactory(OptionalInt.of(8));

        // simulate multiple outer drivers adjusting the same factory
        LookupJoinOperatorFactory adjusted1 = factory.withAdjustedTotalOperatorsCount(1);
        LookupJoinOperatorFactory adjusted2 = adjusted1.withAdjustedTotalOperatorsCount(1);

        assertThat(getTotalOperatorsCount(adjusted1)).isEqualTo(OptionalInt.of(9));
        assertThat(getTotalOperatorsCount(adjusted2)).isEqualTo(OptionalInt.of(10));
    }

    @Test
    public void testWithAdjustedTotalOperatorsCountZeroReturnsThis()
    {
        LookupJoinOperatorFactory factory = createFactory(OptionalInt.of(8));

        LookupJoinOperatorFactory same = factory.withAdjustedTotalOperatorsCount(0);

        // should return the same instance when additionalOperators is 0
        assertThat(same).isSameAs(factory);
    }

    @Test
    public void testWithAdjustedTotalOperatorsCountEmptyReturnsThis()
    {
        LookupJoinOperatorFactory factory = createFactory(OptionalInt.empty());

        LookupJoinOperatorFactory same = factory.withAdjustedTotalOperatorsCount(1);

        // should return the same instance when totalOperatorsCount is empty
        assertThat(same).isSameAs(factory);
    }

    @Test
    public void testDuplicatePreservesTotalOperatorsCount()
    {
        LookupJoinOperatorFactory factory = createFactory(OptionalInt.of(8));

        LookupJoinOperatorFactory duplicated = factory.duplicate();

        assertThat(duplicated).isNotSameAs(factory);
        assertThat(getTotalOperatorsCount(duplicated)).isEqualTo(OptionalInt.of(8));
    }

    @Test
    public void testDuplicateAfterAdjustmentPreservesAdjustedCount()
    {
        // this is the core scenario that caused the bug:
        // factory is adjusted before duplication, duplicated factory should have the adjusted count
        LookupJoinOperatorFactory factory = createFactory(OptionalInt.of(8));

        LookupJoinOperatorFactory adjusted = factory.withAdjustedTotalOperatorsCount(1);
        LookupJoinOperatorFactory duplicated = adjusted.duplicate();

        assertThat(getTotalOperatorsCount(adjusted)).isEqualTo(OptionalInt.of(9));
        assertThat(getTotalOperatorsCount(duplicated)).isEqualTo(OptionalInt.of(9));
    }

    @Test
    public void testOuterOperatorFactoryPresenceForLookupOuterJoin()
    {
        LookupJoinOperatorFactory factory = createLookupOuterFactory(OptionalInt.of(8));

        Optional<OperatorFactory> outerFactory = factory.createOuterOperatorFactory();

        // LOOKUP_OUTER join should have an outer operator factory
        assertThat(outerFactory).isPresent();
    }

    @Test
    public void testOuterOperatorFactoryAbsenceForInnerJoin()
    {
        LookupJoinOperatorFactory factory = createFactory(OptionalInt.of(8));

        Optional<OperatorFactory> outerFactory = factory.createOuterOperatorFactory();

        // INNER join should not have an outer operator factory
        assertThat(outerFactory).isEmpty();
    }

    @Test
    public void testAddLookupOuterDriversScenario()
    {
        // simulate: [OuterJoin1, InnerJoin2] pipeline
        // when OuterJoin1 creates outer driver, InnerJoin2 gets duplicated
        // InnerJoin2's totalOperatorsCount should be adjusted from 8 to 9
        LookupJoinOperatorFactory join1 = createLookupOuterFactory(OptionalInt.of(8));
        LookupJoinOperatorFactory join2 = createFactory(OptionalInt.of(8));

        assertThat(join1.createOuterOperatorFactory()).isPresent();

        // adjust join2 (simulating what addLookupOuterDrivers does)
        LookupJoinOperatorFactory adjustedJoin2 = join2.withAdjustedTotalOperatorsCount(1);
        // duplicate the adjusted factory (for the outer driver)
        LookupJoinOperatorFactory duplicatedJoin2 = adjustedJoin2.duplicate();

        // both the adjusted and duplicated factories should have correct count
        assertThat(getTotalOperatorsCount(adjustedJoin2)).isEqualTo(OptionalInt.of(9));
        assertThat(getTotalOperatorsCount(duplicatedJoin2)).isEqualTo(OptionalInt.of(9));
        // original join2 should be unchanged
        assertThat(getTotalOperatorsCount(join2)).isEqualTo(OptionalInt.of(8));
    }

    @Test
    public void testAdjustmentWithWorkProcessorOperatorAdapter()
    {
        OperatorFactory wrappedFactory = createWrappedFactory(OptionalInt.of(8));

        assertThat(wrappedFactory).isInstanceOf(WorkProcessorOperatorAdapter.Factory.class);

        WorkProcessorOperatorAdapter.Factory adapterFactory = (WorkProcessorOperatorAdapter.Factory) wrappedFactory;
        LookupJoinOperatorFactory innerFactory = (LookupJoinOperatorFactory) adapterFactory.getWorkProcessorOperatorFactory();

        LookupJoinOperatorFactory adjustedInner = innerFactory.withAdjustedTotalOperatorsCount(1);
        assertThat(getTotalOperatorsCount(adjustedInner)).isEqualTo(OptionalInt.of(9));

        // create new wrapped factory with adjusted inner
        OperatorFactory adjustedWrapped = WorkProcessorOperatorAdapter.createAdapterOperatorFactory(adjustedInner);
        assertThat(adjustedWrapped).isInstanceOf(WorkProcessorOperatorAdapter.Factory.class);

        LookupJoinOperatorFactory newInner = (LookupJoinOperatorFactory)
                ((WorkProcessorOperatorAdapter.Factory) adjustedWrapped).getWorkProcessorOperatorFactory();
        assertThat(getTotalOperatorsCount(newInner)).isEqualTo(OptionalInt.of(9));
    }

    @Test
    public void testMultipleOuterJoinsInPipeline()
    {
        // simulate: [OuterJoin1, OuterJoin2, InnerJoin3]
        // OuterJoin1 adjusts OuterJoin2 and InnerJoin3
        // OuterJoin2 adjusts InnerJoin3
        // so InnerJoin3 should be adjusted twice (from 8 to 10)
        LookupJoinOperatorFactory join3 = createFactory(OptionalInt.of(8));

        // first adjustment (from OuterJoin1)
        LookupJoinOperatorFactory adjustedOnce = join3.withAdjustedTotalOperatorsCount(1);
        assertThat(getTotalOperatorsCount(adjustedOnce)).isEqualTo(OptionalInt.of(9));

        // second adjustment (from OuterJoin2)
        LookupJoinOperatorFactory adjustedTwice = adjustedOnce.withAdjustedTotalOperatorsCount(1);
        assertThat(getTotalOperatorsCount(adjustedTwice)).isEqualTo(OptionalInt.of(10));
    }

    private LookupJoinOperatorFactory createFactory(OptionalInt totalOperatorsCount)
    {
        return createFactoryWithType(innerJoin(false, false), totalOperatorsCount);
    }

    private LookupJoinOperatorFactory createLookupOuterFactory(OptionalInt totalOperatorsCount)
    {
        return createFactoryWithType(lookupOuterJoin(false), totalOperatorsCount);
    }

    private LookupJoinOperatorFactory createFactoryWithType(
            io.trino.operator.JoinOperatorType joinType,
            OptionalInt totalOperatorsCount)
    {
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        List<Integer> probeJoinChannels = ImmutableList.of(0);
        List<Type> hashChannelTypes = probeJoinChannels.stream()
                .map(probeTypes::get)
                .toList();

        JoinBridgeManager<PartitionedLookupSourceFactory> bridgeManager = JoinBridgeManager.lookupAllAtOnce(
                new PartitionedLookupSourceFactory(
                        probeTypes,
                        probeTypes,
                        hashChannelTypes,
                        1,
                        false,
                        HASH_COMPILER));

        return new LookupJoinOperatorFactory(
                0,
                new PlanNodeId("test"),
                bridgeManager,
                probeTypes,
                probeTypes,
                probeTypes,
                joinType,
                new JoinProbe.JoinProbeFactory(Ints.toArray(ImmutableList.of(0)), probeJoinChannels),
                HASH_COMPILER,
                totalOperatorsCount,
                probeJoinChannels,
                PARTITIONING_SPILLER_FACTORY);
    }

    private OperatorFactory createWrappedFactory(OptionalInt totalOperatorsCount)
    {
        List<Type> probeTypes = ImmutableList.of(BIGINT);
        List<Integer> probeJoinChannels = ImmutableList.of(0);
        List<Type> hashChannelTypes = probeJoinChannels.stream()
                .map(probeTypes::get)
                .toList();

        JoinBridgeManager<PartitionedLookupSourceFactory> bridgeManager = JoinBridgeManager.lookupAllAtOnce(
                new PartitionedLookupSourceFactory(
                        probeTypes,
                        probeTypes,
                        hashChannelTypes,
                        1,
                        false,
                        HASH_COMPILER));

        return spillingJoin(
                innerJoin(false, false),
                0,
                new PlanNodeId("test"),
                bridgeManager,
                probeTypes,
                probeJoinChannels,
                Optional.empty(),
                totalOperatorsCount,
                PARTITIONING_SPILLER_FACTORY,
                HASH_COMPILER);
    }

    private OptionalInt getTotalOperatorsCount(LookupJoinOperatorFactory factory)
    {
        try {
            var field = LookupJoinOperatorFactory.class.getDeclaredField("totalOperatorsCount");
            field.setAccessible(true);
            return (OptionalInt) field.get(factory);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
