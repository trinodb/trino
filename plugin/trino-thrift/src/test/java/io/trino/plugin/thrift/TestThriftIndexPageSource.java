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
package io.trino.plugin.thrift;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.plugin.thrift.api.TrinoThriftId;
import io.trino.plugin.thrift.api.TrinoThriftNullableColumnSet;
import io.trino.plugin.thrift.api.TrinoThriftNullableSchemaName;
import io.trino.plugin.thrift.api.TrinoThriftNullableTableMetadata;
import io.trino.plugin.thrift.api.TrinoThriftNullableToken;
import io.trino.plugin.thrift.api.TrinoThriftPageResult;
import io.trino.plugin.thrift.api.TrinoThriftSchemaTableName;
import io.trino.plugin.thrift.api.TrinoThriftService;
import io.trino.plugin.thrift.api.TrinoThriftSplit;
import io.trino.plugin.thrift.api.TrinoThriftSplitBatch;
import io.trino.plugin.thrift.api.TrinoThriftTupleDomain;
import io.trino.plugin.thrift.api.datatypes.TrinoThriftInteger;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.trino.plugin.thrift.api.TrinoThriftBlock.integerData;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.Collections.shuffle;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestThriftIndexPageSource
{
    private static final long MAX_BYTES_PER_RESPONSE = 16_000_000;

    @Test
    public void testGetNextPageTwoConcurrentRequests()
            throws Exception
    {
        final int splits = 3;
        final int lookupRequestsConcurrency = 2;
        final int rowsPerSplit = 1;
        List<SettableFuture<TrinoThriftPageResult>> futures = IntStream.range(0, splits)
                .mapToObj(i -> SettableFuture.<TrinoThriftPageResult>create())
                .collect(toImmutableList());
        List<CountDownLatch> signals = IntStream.range(0, splits)
                .mapToObj(i -> new CountDownLatch(1))
                .collect(toImmutableList());
        TestingThriftService client = new TestingThriftService(rowsPerSplit, false, false)
        {
            @Override
            public ListenableFuture<TrinoThriftPageResult> getRows(TrinoThriftId splitId, List<String> columns, long maxBytes, TrinoThriftNullableToken nextToken)
            {
                int key = Ints.fromByteArray(splitId.getId());
                signals.get(key).countDown();
                return futures.get(key);
            }
        };
        ThriftConnectorStats stats = new ThriftConnectorStats();
        long pageSizeReceived = 0;
        ThriftIndexPageSource pageSource = new ThriftIndexPageSource(
                (context, headers) -> client,
                ImmutableMap.of(),
                stats,
                new ThriftIndexHandle(new SchemaTableName("default", "table1"), TupleDomain.all()),
                ImmutableList.of(column("a", INTEGER)),
                ImmutableList.of(column("b", INTEGER)),
                new InMemoryRecordSet(ImmutableList.of(INTEGER), generateKeys(0, splits)),
                MAX_BYTES_PER_RESPONSE,
                lookupRequestsConcurrency);

        assertThat(pageSource.getNextPage()).isNull();
        assertThat((long) stats.getIndexPageSize().getAllTime().getTotal()).isEqualTo(0);
        signals.get(0).await(1, SECONDS);
        signals.get(1).await(1, SECONDS);
        signals.get(2).await(1, SECONDS);
        assertThat(signals.get(0).getCount())
                .describedAs("first request wasn't sent")
                .isEqualTo(0);
        assertThat(signals.get(1).getCount())
                .describedAs("second request wasn't sent")
                .isEqualTo(0);
        assertThat(signals.get(2).getCount())
                .describedAs("third request shouldn't be sent")
                .isEqualTo(1);

        // at this point first two requests were sent
        assertThat(pageSource.isFinished()).isFalse();
        assertThat(pageSource.getNextPage()).isNull();
        assertThat((long) stats.getIndexPageSize().getAllTime().getTotal()).isEqualTo(0);

        // completing the second request
        futures.get(1).set(pageResult(20, null));
        Page page = pageSource.getNextPage();
        pageSizeReceived += page.getSizeInBytes();
        assertThat((long) stats.getIndexPageSize().getAllTime().getTotal()).isEqualTo(pageSizeReceived);
        assertThat(page).isNotNull();
        assertThat(page.getPositionCount()).isEqualTo(1);
        assertThat(INTEGER.getInt(page.getBlock(0), 0)).isEqualTo(20);
        // not complete yet
        assertThat(pageSource.isFinished()).isFalse();

        // once one of the requests completes the next one should be sent
        signals.get(2).await(1, SECONDS);
        assertThat(signals.get(2).getCount())
                .describedAs("third request wasn't sent")
                .isEqualTo(0);

        // completing the first request
        futures.get(0).set(pageResult(10, null));
        page = pageSource.getNextPage();
        assertThat(page).isNotNull();
        pageSizeReceived += page.getSizeInBytes();
        assertThat((long) stats.getIndexPageSize().getAllTime().getTotal()).isEqualTo(pageSizeReceived);
        assertThat(page.getPositionCount()).isEqualTo(1);
        assertThat(INTEGER.getInt(page.getBlock(0), 0)).isEqualTo(10);
        // still not complete
        assertThat(pageSource.isFinished()).isFalse();

        // completing the third request
        futures.get(2).set(pageResult(30, null));
        page = pageSource.getNextPage();
        assertThat(page).isNotNull();
        pageSizeReceived += page.getSizeInBytes();
        assertThat((long) stats.getIndexPageSize().getAllTime().getTotal()).isEqualTo(pageSizeReceived);
        assertThat(page.getPositionCount()).isEqualTo(1);
        assertThat(INTEGER.getInt(page.getBlock(0), 0)).isEqualTo(30);
        // finished now
        assertThat(pageSource.isFinished()).isTrue();

        // after completion
        assertThat(pageSource.getNextPage()).isNull();
        pageSource.close();
    }

    @Test
    public void testGetNextPageMultipleSplitRequest()
            throws Exception
    {
        runGeneralTest(5, 2, 2, true);
    }

    @Test
    public void testGetNextPageNoSplits()
            throws Exception
    {
        runGeneralTest(0, 2, 2, false);
    }

    @Test
    public void testGetNextPageOneConcurrentRequest()
            throws Exception
    {
        runGeneralTest(3, 1, 3, false);
    }

    @Test
    public void testGetNextPageMoreConcurrencyThanRequestsNoContinuation()
            throws Exception
    {
        runGeneralTest(2, 4, 1, false);
    }

    private static void runGeneralTest(int splits, int lookupRequestsConcurrency, int rowsPerSplit, boolean twoSplitBatches)
            throws Exception
    {
        TestingThriftService client = new TestingThriftService(rowsPerSplit, true, twoSplitBatches);
        ThriftIndexPageSource pageSource = new ThriftIndexPageSource(
                (context, headers) -> client,
                ImmutableMap.of(),
                new ThriftConnectorStats(),
                new ThriftIndexHandle(new SchemaTableName("default", "table1"), TupleDomain.all()),
                ImmutableList.of(column("a", INTEGER)),
                ImmutableList.of(column("b", INTEGER)),
                new InMemoryRecordSet(ImmutableList.of(INTEGER), generateKeys(1, splits + 1)),
                MAX_BYTES_PER_RESPONSE,
                lookupRequestsConcurrency);

        List<Integer> actual = new ArrayList<>();
        while (!pageSource.isFinished()) {
            CompletableFuture<?> blocked = pageSource.isBlocked();
            blocked.get(1, SECONDS);
            Page page = pageSource.getNextPage();
            if (page != null) {
                Block block = page.getBlock(0);
                for (int position = 0; position < block.getPositionCount(); position++) {
                    actual.add(INTEGER.getInt(block, position));
                }
            }
        }

        Collections.sort(actual);
        List<Integer> expected = new ArrayList<>(splits * rowsPerSplit);
        for (int split = 1; split <= splits; split++) {
            for (int row = 0; row < rowsPerSplit; row++) {
                expected.add(split * 10 + row);
            }
        }
        assertThat(actual).isEqualTo(expected);

        // must be null after finish
        assertThat(pageSource.getNextPage()).isNull();

        pageSource.close();
    }

    private static class TestingThriftService
            implements TrinoThriftService
    {
        private final int rowsPerSplit;
        private final boolean shuffleSplits;
        private final boolean twoSplitBatches;

        public TestingThriftService(int rowsPerSplit, boolean shuffleSplits, boolean twoSplitBatches)
        {
            this.rowsPerSplit = rowsPerSplit;
            this.shuffleSplits = shuffleSplits;
            this.twoSplitBatches = twoSplitBatches;
        }

        @Override
        public ListenableFuture<TrinoThriftSplitBatch> getIndexSplits(TrinoThriftSchemaTableName schemaTableName, List<String> indexColumnNames, List<String> outputColumnNames, TrinoThriftPageResult keys, TrinoThriftTupleDomain outputConstraint, int maxSplitCount, TrinoThriftNullableToken nextToken)
        {
            if (keys.getRowCount() == 0) {
                return immediateFuture(new TrinoThriftSplitBatch(ImmutableList.of(), null));
            }
            TrinoThriftId newNextToken = null;
            int[] values = keys.getColumnBlocks().get(0).getIntegerData().getInts();
            int begin;
            int end;
            if (twoSplitBatches) {
                if (nextToken.getToken() == null) {
                    begin = 0;
                    end = values.length / 2;
                    newNextToken = new TrinoThriftId(Ints.toByteArray(1));
                }
                else {
                    begin = values.length / 2;
                    end = values.length;
                }
            }
            else {
                begin = 0;
                end = values.length;
            }

            List<TrinoThriftSplit> splits = new ArrayList<>(end - begin);
            for (int i = begin; i < end; i++) {
                splits.add(new TrinoThriftSplit(new TrinoThriftId(Ints.toByteArray(values[i])), ImmutableList.of()));
            }
            if (shuffleSplits) {
                shuffle(splits);
            }
            return immediateFuture(new TrinoThriftSplitBatch(splits, newNextToken));
        }

        @Override
        public ListenableFuture<TrinoThriftPageResult> getRows(TrinoThriftId splitId, List<String> columns, long maxBytes, TrinoThriftNullableToken nextToken)
        {
            if (rowsPerSplit == 0) {
                return immediateFuture(new TrinoThriftPageResult(ImmutableList.of(), 0, null));
            }
            int key = Ints.fromByteArray(splitId.getId());
            int offset = nextToken.getToken() != null ? Ints.fromByteArray(nextToken.getToken().getId()) : 0;
            TrinoThriftId newNextToken = offset + 1 < rowsPerSplit ? new TrinoThriftId(Ints.toByteArray(offset + 1)) : null;
            return immediateFuture(pageResult(key * 10 + offset, newNextToken));
        }

        // methods below are not used for the test

        @Override
        public List<String> listSchemaNames()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<TrinoThriftSchemaTableName> listTables(TrinoThriftNullableSchemaName schemaNameOrNull)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TrinoThriftNullableTableMetadata getTableMetadata(TrinoThriftSchemaTableName schemaTableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListenableFuture<TrinoThriftSplitBatch> getSplits(TrinoThriftSchemaTableName schemaTableName, TrinoThriftNullableColumnSet desiredColumns, TrinoThriftTupleDomain outputConstraint, int maxSplitCount, TrinoThriftNullableToken nextToken)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static ThriftColumnHandle column(String name, Type type)
    {
        return new ThriftColumnHandle(name, type, null, false);
    }

    private static List<List<Integer>> generateKeys(int beginInclusive, int endExclusive)
    {
        return IntStream.range(beginInclusive, endExclusive)
                .mapToObj(ImmutableList::of)
                .collect(toImmutableList());
    }

    private static TrinoThriftPageResult pageResult(int value, TrinoThriftId nextToken)
    {
        return new TrinoThriftPageResult(ImmutableList.of(integerData(new TrinoThriftInteger(null, new int[] {value}))), 1, nextToken);
    }
}
