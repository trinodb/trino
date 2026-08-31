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
package io.trino.plugin.paimon;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.table.source.TableScan.Plan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RowRangeIndex;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_CANNOT_OPEN_SPLIT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.DynamicFilter.NOT_BLOCKED;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PaimonSplitManagerTest
{
    private static final PaimonColumnHandle ID_COLUMN = PaimonColumnHandle.of("id", DataTypes.BIGINT());
    private static final RowType ROW_TYPE = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));

    @Test
    public void testGetTableHandleRequiresPaimonTableHandle()
    {
        ConnectorTableHandle wrongHandle = new ConnectorTableHandle() {};

        assertThatThrownBy(() -> PaimonSplitManager.getTableHandle(wrongHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon split planning requires PaimonTableHandle, got: %s", wrongHandle.getClass().getName());
    }

    @Test
    public void testGetTableHandleReturnsPaimonTableHandle()
    {
        PaimonTableHandle handle = new PaimonTableHandle("schema", "table", Collections.emptyMap());

        assertThat(PaimonSplitManager.getTableHandle(handle)).isSameAs(handle);
    }

    @Test
    public void testGetTableFunctionHandleRequiresPaimonTableHandle()
    {
        ConnectorTableFunctionHandle wrongHandle = new ConnectorTableFunctionHandle() {};

        assertThatThrownBy(() -> PaimonSplitManager.getTableFunctionHandle(wrongHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon table function split planning requires PaimonTableHandle, got: %s", wrongHandle.getClass().getName());
    }

    @Test
    public void testGetTableFunctionHandleReturnsPaimonTableHandle()
    {
        PaimonTableHandle handle = new PaimonTableHandle("schema", "table", Collections.emptyMap());

        assertThat(PaimonSplitManager.getTableFunctionHandle(handle)).isSameAs(handle);
    }

    @Test
    public void testCanApplyDynamicFilterReturnsTrueWithoutLimit()
    {
        PaimonTableHandle handle = new PaimonTableHandle("schema", "table", Collections.emptyMap());

        assertThat(PaimonSplitManager.canApplyDynamicFilter(handle)).isTrue();
    }

    @Test
    public void testCanApplyDynamicFilterReturnsFalseWithLimit()
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "schema",
                "table",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(10));

        assertThat(PaimonSplitManager.canApplyDynamicFilter(handle)).isFalse();
    }

    @Test
    public void testEffectivePredicateIgnoresDynamicDomainsWhenLimitPreventsDynamicFilter()
    {
        TupleDomain<PaimonColumnHandle> staticPredicate = TupleDomain.withColumnDomains(Map.of(
                ID_COLUMN, Domain.singleValue(BIGINT, 1L)));
        PaimonTableHandle handle = new PaimonTableHandle(
                "schema",
                "table",
                Collections.emptyMap(),
                staticPredicate,
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(10));

        TupleDomain<PaimonColumnHandle> result = PaimonSplitManager.effectivePredicate(
                handle, dynamicFilter(TupleDomain.withColumnDomains(Map.of(ID_COLUMN, Domain.singleValue(BIGINT, 2L))), false));

        assertThat(result).isEqualTo(staticPredicate);
    }

    @Test
    public void testEffectivePredicateKeepsDynamicNoneWhenLimitPreventsDynamicFilter()
    {
        TupleDomain<PaimonColumnHandle> staticPredicate = TupleDomain.withColumnDomains(Map.of(
                ID_COLUMN, Domain.singleValue(BIGINT, 1L)));
        PaimonTableHandle handle = new PaimonTableHandle(
                "schema",
                "table",
                Collections.emptyMap(),
                staticPredicate,
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(10));

        TupleDomain<PaimonColumnHandle> result = PaimonSplitManager.effectivePredicate(
                handle, dynamicFilter(TupleDomain.none(), false));

        assertThat(result).isEqualTo(TupleDomain.none());
    }

    @Test
    public void testEffectivePredicateIgnoresDynamicDomains()
    {
        TupleDomain<PaimonColumnHandle> staticPredicate = TupleDomain.withColumnDomains(Map.of(
                ID_COLUMN, Domain.singleValue(BIGINT, 1L)));
        PaimonTableHandle handle = new PaimonTableHandle(
                "schema",
                "table",
                Collections.emptyMap(),
                staticPredicate,
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        TupleDomain<PaimonColumnHandle> result = PaimonSplitManager.effectivePredicate(
                handle, dynamicFilter(TupleDomain.withColumnDomains(Map.of(ID_COLUMN, Domain.singleValue(BIGINT, 2L))), false));

        assertThat(result).isEqualTo(staticPredicate);
    }

    @Test
    public void testEffectivePredicateKeepsDynamicNone()
    {
        TupleDomain<PaimonColumnHandle> staticPredicate = TupleDomain.withColumnDomains(Map.of(
                ID_COLUMN, Domain.singleValue(BIGINT, 1L)));
        PaimonTableHandle handle = new PaimonTableHandle(
                "schema",
                "table",
                Collections.emptyMap(),
                staticPredicate,
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        TupleDomain<PaimonColumnHandle> result = PaimonSplitManager.effectivePredicate(
                handle, dynamicFilter(TupleDomain.none(), false));

        assertThat(result).isEqualTo(TupleDomain.none());
    }

    @Test
    public void testIsEmptySplitWhenPredicateIsNone()
    {
        PaimonTableHandle handle = new PaimonTableHandle("schema", "table", Collections.emptyMap());

        assertThat(PaimonSplitManager.isEmptySplit(TupleDomain.none(), handle)).isTrue();
    }

    @Test
    public void testIsEmptySplitWhenLimitIsZero()
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "schema",
                "table",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(0));

        assertThat(PaimonSplitManager.isEmptySplit(TupleDomain.all(), handle)).isTrue();
    }

    @Test
    public void testIsEmptySplitWhenPredicateAllAndNoLimit()
    {
        PaimonTableHandle handle = new PaimonTableHandle("schema", "table", Collections.emptyMap());

        assertThat(PaimonSplitManager.isEmptySplit(TupleDomain.all(), handle)).isFalse();
    }

    @Test
    public void testPushLimitAppliesLimit()
    {
        RecordingReadBuilder readBuilder = new RecordingReadBuilder();
        PaimonTableHandle handle = new PaimonTableHandle(
                "schema",
                "table",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(42));

        PaimonSplitManager.pushLimit(readBuilder, handle);

        assertThat(readBuilder.appliedLimit).hasValue(42);
    }

    @Test
    public void testPushLimitIgnoresMissingLimit()
    {
        RecordingReadBuilder readBuilder = new RecordingReadBuilder();
        PaimonTableHandle handle = new PaimonTableHandle("schema", "table", Collections.emptyMap());

        PaimonSplitManager.pushLimit(readBuilder, handle);

        assertThat(readBuilder.appliedLimit).isEmpty();
    }

    @Test
    public void testPushLimitIgnoresOverflowLimit()
    {
        RecordingReadBuilder readBuilder = new RecordingReadBuilder();
        PaimonTableHandle handle = new PaimonTableHandle(
                "schema",
                "table",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(Long.MAX_VALUE));

        PaimonSplitManager.pushLimit(readBuilder, handle);

        assertThat(readBuilder.appliedLimit).isEmpty();
    }

    @Test
    public void testPushPredicateAppliesFilter()
    {
        RecordingReadBuilder readBuilder = new RecordingReadBuilder();
        Table table = table(ROW_TYPE);
        TupleDomain<PaimonColumnHandle> predicate = TupleDomain.withColumnDomains(Map.of(
                ID_COLUMN, Domain.singleValue(BIGINT, 7L)));

        PaimonSplitManager.pushPredicate(readBuilder, table, predicate);

        assertThat(readBuilder.appliedFilters).hasSize(1);
        assertThat(readBuilder.appliedRowRanges).isEmpty();
    }

    @Test
    public void testPushPredicateAppliesRowIdRanges()
    {
        RecordingReadBuilder readBuilder = new RecordingReadBuilder();
        Table table = table(ROW_TYPE);
        PaimonColumnHandle rowIdColumn = PaimonColumnHandle.of("_row_id", SpecialFields.ROW_ID.type());
        TupleDomain<PaimonColumnHandle> predicate = TupleDomain.withColumnDomains(Map.of(
                rowIdColumn, Domain.singleValue(BIGINT, 5L)));

        PaimonSplitManager.pushPredicate(readBuilder, table, predicate);

        assertThat(readBuilder.appliedRowRanges).hasValue(List.of(new Range(5, 5)));
        assertThat(readBuilder.appliedFilters).isEmpty();
    }

    @Test
    public void testPushPredicateSkipsEmptyExtremeRowIdRanges()
    {
        PaimonColumnHandle rowIdColumn = PaimonColumnHandle.of("_row_id", SpecialFields.ROW_ID.type());

        RecordingReadBuilder greaterThanMax = new RecordingReadBuilder();
        PaimonSplitManager.pushPredicate(greaterThanMax, table(ROW_TYPE), TupleDomain.withColumnDomains(Map.of(
                rowIdColumn, Domain.create(ValueSet.ofRanges(io.trino.spi.predicate.Range.greaterThan(BIGINT, Long.MAX_VALUE)), false))));

        assertThat(greaterThanMax.appliedRowRanges).hasValue(List.of());
        assertThat(greaterThanMax.appliedFilters).isEmpty();

        RecordingReadBuilder lessThanMin = new RecordingReadBuilder();
        PaimonSplitManager.pushPredicate(lessThanMin, table(ROW_TYPE), TupleDomain.withColumnDomains(Map.of(
                rowIdColumn, Domain.create(ValueSet.ofRanges(io.trino.spi.predicate.Range.lessThan(BIGINT, Long.MIN_VALUE)), false))));

        assertThat(lessThanMin.appliedRowRanges).hasValue(List.of());
        assertThat(lessThanMin.appliedFilters).isEmpty();
    }

    @Test
    public void testPushPredicateIgnoresAllDomain()
    {
        RecordingReadBuilder readBuilder = new RecordingReadBuilder();
        Table table = table(ROW_TYPE);

        PaimonSplitManager.pushPredicate(readBuilder, table, TupleDomain.all());

        assertThat(readBuilder.appliedFilters).isEmpty();
        assertThat(readBuilder.appliedRowRanges).isEmpty();
    }

    @Test
    public void testPlanScanDoesNotDropManifestStatisticsBeforePlanning()
    {
        RecordingReadBuilder readBuilder = new RecordingReadBuilder();

        assertThat(PaimonSplitManager.planScan(readBuilder)).isEmpty();
    }

    @Test
    public void testCalculateSplitWeight()
    {
        assertThat(PaimonSplitManager.calculateSplitWeight(split(100), 1000, 0.1)).isEqualTo(0.1);
        assertThat(PaimonSplitManager.calculateSplitWeight(split(500), 1000, 0.1)).isEqualTo(0.5);
        assertThat(PaimonSplitManager.calculateSplitWeight(split(2000), 1000, 0.1)).isEqualTo(1.0);
        assertThat(PaimonSplitManager.calculateSplitWeight(split(100), 0, 0.1)).isEqualTo(0.1);
        assertThat(PaimonSplitManager.calculateSplitWeight(split(0), 1000, 0.1)).isEqualTo(0.1);
    }

    @Test
    public void testSplitWeightRowCountPrefersMergedCount()
    {
        assertThat(PaimonSplitManager.splitWeightRowCount(split(100, OptionalLong.of(50)))).isEqualTo(50L);
        assertThat(PaimonSplitManager.splitWeightRowCount(split(100, OptionalLong.empty()))).isEqualTo(100L);
    }

    @Test
    public void testSplitSourceLimitCountsRowCountWhenMergedCountIsMissing()
    {
        PaimonSplitSource source = new PaimonSplitSource(List.of(
                PaimonSplit.fromSplit(split(10), 1.0),
                PaimonSplit.fromSplit(split(10), 1.0)),
                OptionalLong.of(5));
        assertThat(source.getNextBatch(10, DynamicFilterSnapshot.EMPTY)).isCompletedWithValueMatching(batch -> batch.size() == 1);
        assertThat(source.isFinished()).isTrue();
        assertThat(source.getNextBatch(10, DynamicFilterSnapshot.EMPTY)).isCompletedWithValueMatching(batch -> batch.isEmpty());
    }

    @Test
    public void testSplitSourceLimitPrefersMergedRowCount()
    {
        PaimonSplitSource source = new PaimonSplitSource(List.of(
                PaimonSplit.fromSplit(split(100, OptionalLong.of(3)), 1.0),
                PaimonSplit.fromSplit(split(100, OptionalLong.of(4)), 1.0),
                PaimonSplit.fromSplit(split(100, OptionalLong.of(4)), 1.0)),
                OptionalLong.of(5));

        assertThat(source.getNextBatch(10, DynamicFilterSnapshot.EMPTY)).isCompletedWithValueMatching(batch -> batch.size() == 2);
        assertThat(source.isFinished()).isTrue();
        assertThat(source.getNextBatch(10, DynamicFilterSnapshot.EMPTY)).isCompletedWithValueMatching(batch -> batch.isEmpty());
    }

    @Test
    public void testEmptySplitSource()
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "schema",
                "table",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(10));

        PaimonSplitSource source = PaimonSplitManager.emptySplitSource(handle);

        assertThat(source.getNextBatch(1, DynamicFilterSnapshot.EMPTY)).isCompletedWithValueMatching(
                batch -> batch.isEmpty());
        assertThat(source.isFinished()).isTrue();
    }

    @Test
    public void testUnsupportedReadOperationForNormalTable()
    {
        PaimonTableHandle handle = new PaimonTableHandle("schema", "table", Collections.emptyMap());
        UnsupportedOperationException cause = new UnsupportedOperationException("unsupported");

        TrinoException exception = PaimonSplitManager.unsupportedReadOperation(handle, cause);

        assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
        assertThat(exception).hasMessage(
                "Paimon table read uses features which are not supported by the Trino connector: unsupported");
    }

    @Test
    public void testUnsupportedReadOperationForIncrementalTable()
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "schema", "table", Map.of(CoreOptions.INCREMENTAL_BETWEEN.key(), "1,2"));
        UnsupportedOperationException cause = new UnsupportedOperationException("unsupported");

        TrinoException exception = PaimonSplitManager.unsupportedReadOperation(handle, cause);

        assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
        assertThat(exception).hasMessage(
                "Paimon system.table_changes uses features which are not supported by the Trino connector: unsupported");
    }

    @Test
    public void testUnsupportedReadOperationUsesExceptionTypeWhenMessageIsMissing()
    {
        PaimonTableHandle handle = new PaimonTableHandle("schema", "table", Collections.emptyMap());
        UnsupportedOperationException cause = new UnsupportedOperationException();

        TrinoException exception = PaimonSplitManager.unsupportedReadOperation(handle, cause);

        assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
        assertThat(exception).hasMessage(
                "Paimon table read uses features which are not supported by the Trino connector: UnsupportedOperationException");
    }

    @Test
    public void testSplitPlanningExceptionForNormalTable()
            throws Exception
    {
        PaimonTableHandle handle = new PaimonTableHandle("schema", "table", Collections.emptyMap());
        Exception cause = new IOException("planning failed");

        RuntimeException exception = PaimonSplitManager.splitPlanningException(handle, cause);

        assertThat(exception).isInstanceOf(TrinoException.class);
        assertThat(((TrinoException) exception).getErrorCode()).isEqualTo(PAIMON_CANNOT_OPEN_SPLIT.toErrorCode());
        assertThat(exception).hasMessage("Failed to plan Paimon splits: planning failed");
        assertThat(exception.getCause()).isSameAs(cause);
    }

    @Test
    public void testSplitPlanningExceptionForIncrementalTable()
            throws Exception
    {
        PaimonTableHandle handle = new PaimonTableHandle(
                "schema", "table", Map.of(CoreOptions.INCREMENTAL_BETWEEN.key(), "1,2"));
        Exception cause = new IOException("planning failed");

        RuntimeException exception = PaimonSplitManager.splitPlanningException(handle, cause);

        assertThat(exception).isInstanceOf(TrinoException.class);
        assertThat(((TrinoException) exception).getErrorCode()).isEqualTo(PAIMON_CANNOT_OPEN_SPLIT.toErrorCode());
        assertThat(exception).hasMessage("Failed to plan Paimon table_changes splits: planning failed");
        assertThat(exception.getCause()).isSameAs(cause);
    }

    @Test
    public void testSplitPlanningExceptionWrapsRuntimeException()
    {
        PaimonTableHandle handle = new PaimonTableHandle("schema", "table", Collections.emptyMap());
        RuntimeException cause = new IndexOutOfBoundsException("Index 1 out of bounds for length 1");

        RuntimeException exception = PaimonSplitManager.splitPlanningException(handle, cause);

        assertThat(exception).isInstanceOfSatisfying(TrinoException.class, trinoException -> {
            assertThat(trinoException.getErrorCode()).isEqualTo(PAIMON_CANNOT_OPEN_SPLIT.toErrorCode());
            assertThat(trinoException).hasMessage("Failed to plan Paimon splits: Index 1 out of bounds for length 1");
            assertThat(trinoException.getCause()).isSameAs(cause);
        });
    }

    @Test
    public void testSplitPlanningExceptionUnwrapsNestedIoFailure()
            throws Exception
    {
        PaimonTableHandle handle = new PaimonTableHandle("schema", "table", Collections.emptyMap());
        IOException ioException = new IOException("manifest read failed");
        RuntimeException cause = new RuntimeException(new RuntimeException(ioException));

        RuntimeException exception = PaimonSplitManager.splitPlanningException(handle, cause);

        assertThat(exception).isInstanceOfSatisfying(TrinoException.class, trinoException -> {
            assertThat(trinoException.getErrorCode()).isEqualTo(PAIMON_CANNOT_OPEN_SPLIT.toErrorCode());
            assertThat(trinoException).hasMessage("Failed to plan Paimon splits: manifest read failed");
            assertThat(trinoException.getCause()).isSameAs(ioException);
        });
    }

    @Test
    public void testSplitPlanningExceptionUnwrapsNestedUnsupportedFailure()
    {
        PaimonTableHandle handle = new PaimonTableHandle("schema", "table", Collections.emptyMap());
        UnsupportedOperationException unsupported = new UnsupportedOperationException("unsupported scan");
        RuntimeException cause = new RuntimeException(new RuntimeException(unsupported));

        RuntimeException exception = PaimonSplitManager.splitPlanningException(handle, cause);

        assertThat(exception).isInstanceOfSatisfying(TrinoException.class, trinoException -> {
            assertThat(trinoException.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
            assertThat(trinoException).hasMessage(
                    "Paimon table read uses features which are not supported by the Trino connector: unsupported scan");
            assertThat(trinoException.getCause()).isSameAs(unsupported);
        });
    }

    @Test
    public void testSplitPlanningExceptionUnwrapsNestedTrinoException()
    {
        PaimonTableHandle handle = new PaimonTableHandle("schema", "table", Collections.emptyMap());
        TrinoException mapped = new TrinoException(PAIMON_CANNOT_OPEN_SPLIT, "already mapped");
        RuntimeException cause = new RuntimeException(new RuntimeException(mapped));

        RuntimeException exception = PaimonSplitManager.splitPlanningException(handle, cause);

        assertThat(exception).isSameAs(mapped);
    }

    @Test
    public void testSplitPlanningExceptionReturnsTrinoExceptionAsIs()
    {
        PaimonTableHandle handle = new PaimonTableHandle("schema", "table", Collections.emptyMap());
        TrinoException cause = new TrinoException(PAIMON_CANNOT_OPEN_SPLIT, "already wrapped");

        RuntimeException exception = PaimonSplitManager.splitPlanningException(handle, cause);

        assertThat(exception).isSameAs(cause);
    }

    private static DynamicFilter dynamicFilter(TupleDomain<ColumnHandle> predicate, boolean awaitable)
    {
        return new DynamicFilter()
        {
            @Override
            public Set<ColumnHandle> getColumnsCovered()
            {
                return predicate.getDomains()
                        .map(Map::keySet)
                        .orElse(Set.of());
            }

            @Override
            public CompletableFuture<?> isBlocked()
            {
                return NOT_BLOCKED;
            }

            @Override
            public boolean isComplete()
            {
                return !awaitable;
            }

            @Override
            public boolean isAwaitable()
            {
                return awaitable;
            }

            @Override
            public TupleDomain<ColumnHandle> getCurrentPredicate()
            {
                return predicate;
            }
        };
    }

    private static Table table(RowType rowType)
    {
        return (Table) Proxy.newProxyInstance(
                PaimonSplitManagerTest.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "rowType" -> rowType;
                    case "copy" -> proxy;
                    case "toString" -> "testing-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Split split(long rowCount)
    {
        return new TestingSplit(rowCount, false, 0);
    }

    private static Split split(long rowCount, OptionalLong mergedRowCount)
    {
        return mergedRowCount.isPresent()
                ? new TestingSplit(rowCount, true, mergedRowCount.orElseThrow())
                : split(rowCount);
    }

    private static class TestingSplit
            implements Split
    {
        private final long rowCount;
        private final boolean hasMergedRowCount;
        private final long mergedRowCount;

        private TestingSplit(long rowCount, boolean hasMergedRowCount, long mergedRowCount)
        {
            this.rowCount = rowCount;
            this.hasMergedRowCount = hasMergedRowCount;
            this.mergedRowCount = mergedRowCount;
        }

        @Override
        public long rowCount()
        {
            return rowCount;
        }

        @Override
        public OptionalLong mergedRowCount()
        {
            return hasMergedRowCount ? OptionalLong.of(mergedRowCount) : OptionalLong.empty();
        }
    }

    private static class RecordingReadBuilder
            implements ReadBuilder
    {
        private Optional<Integer> appliedLimit = Optional.empty();
        private Optional<List<Range>> appliedRowRanges = Optional.empty();
        private List<Predicate> appliedFilters = new ArrayList<>();

        @Override
        public String tableName()
        {
            return "testing";
        }

        @Override
        public RowType readType()
        {
            return ROW_TYPE;
        }

        @Override
        public ReadBuilder withFilter(Predicate predicate)
        {
            appliedFilters.add(predicate);
            return this;
        }

        @Override
        public ReadBuilder withPartitionFilter(Map<String, String> partitionSpec)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ReadBuilder withPartitionFilter(PartitionPredicate partitionPredicate)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ReadBuilder withBucket(int bucket)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ReadBuilder withBucketFilter(Filter<Integer> bucketFilter)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ReadBuilder withReadType(RowType readType)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ReadBuilder withProjection(int[] projection)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ReadBuilder withLimit(int limit)
        {
            appliedLimit = Optional.of(limit);
            return this;
        }

        @Override
        public ReadBuilder withTopN(TopN topN)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ReadBuilder withShard(int indexOfThisSubtask, int numberOfParallelSubtasks)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ReadBuilder withRowRanges(List<Range> rowRanges)
        {
            appliedRowRanges = Optional.of(List.copyOf(rowRanges));
            return this;
        }

        @Override
        public ReadBuilder withRowRangeIndex(RowRangeIndex rowRangeIndex)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ReadBuilder dropStats()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableScan newScan()
        {
            return (TableScan) Proxy.newProxyInstance(
                    PaimonSplitManagerTest.class.getClassLoader(),
                    new Class<?>[] {TableScan.class},
                    (proxy, method, _) -> switch (method.getName()) {
                        case "withMetricRegistry", "withGlobalIndexResult" -> proxy;
                        case "plan" -> (Plan) () -> List.of();
                        case "toString" -> "testing-table-scan";
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
        }

        @Override
        public StreamTableScan newStreamScan()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableRead newRead()
        {
            throw new UnsupportedOperationException();
        }
    }
}
