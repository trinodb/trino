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
package io.trino.plugin.starrocks;

import io.trino.spi.connector.SourcePage;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

final class TestStarRocksFlightSqlPageSource
{
    @Test
    void testPageSourceSkipsEmptyBatchesAndReadsRows()
    {
        List<StarRocksColumnHandle> columns = List.of(
                new StarRocksColumnHandle("id", "id", BIGINT, 0),
                new StarRocksColumnHandle("name", "name", createVarcharType(20), 1));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                BigIntVector emptyId = new BigIntVector("id", allocator);
                VarCharVector emptyName = new VarCharVector("name", allocator);
                VectorSchemaRoot emptyRoot = new VectorSchemaRoot(List.of(emptyId, emptyName));
                BigIntVector firstBatchId = new BigIntVector("id", allocator);
                VarCharVector firstBatchName = new VarCharVector("name", allocator);
                VectorSchemaRoot firstBatchRoot = new VectorSchemaRoot(List.of(firstBatchId, firstBatchName));
                BigIntVector secondBatchId = new BigIntVector("id", allocator);
                VarCharVector secondBatchName = new VarCharVector("name", allocator);
                VectorSchemaRoot secondBatchRoot = new VectorSchemaRoot(List.of(secondBatchId, secondBatchName))) {
            emptyId.setValueCount(0);
            emptyName.setValueCount(0);
            emptyRoot.setRowCount(0);

            firstBatchId.allocateNew();
            firstBatchId.setSafe(0, 1L);
            firstBatchId.setSafe(1, 2L);
            firstBatchId.setValueCount(2);

            firstBatchName.allocateNew();
            firstBatchName.setSafe(0, "alpha".getBytes(UTF_8));
            firstBatchName.setSafe(1, "beta".getBytes(UTF_8));
            firstBatchName.setValueCount(2);
            firstBatchRoot.setRowCount(2);

            secondBatchId.allocateNew();
            secondBatchId.setSafe(0, 3L);
            secondBatchId.setValueCount(1);

            secondBatchName.allocateNew();
            secondBatchName.setSafe(0, "gamma".getBytes(UTF_8));
            secondBatchName.setValueCount(1);
            secondBatchRoot.setRowCount(1);

            StarRocksFlightSqlPageSource pageSource = new StarRocksFlightSqlPageSource(
                    new TestingStarRocksFlightSqlResult(List.of(emptyRoot, firstBatchRoot, secondBatchRoot)),
                    new StarRocksArrowToPageConverter(),
                    columns);

            SourcePage firstPage = pageSource.getNextSourcePage();
            assertThat(pageSource.isFinished()).isFalse();
            assertThat(firstPage.getPositionCount()).isEqualTo(2);
            assertThat(BIGINT.getLong(firstPage.getBlock(0), 0)).isEqualTo(1L);
            assertThat(createVarcharType(20).getObjectValue(firstPage.getBlock(1), 1)).isEqualTo("beta");
            assertThat(pageSource.getCompletedPositions().orElseThrow()).isEqualTo(2L);

            SourcePage secondPage = pageSource.getNextSourcePage();
            assertThat(pageSource.isFinished()).isFalse();
            assertThat(secondPage.getPositionCount()).isEqualTo(1);
            assertThat(BIGINT.getLong(secondPage.getBlock(0), 0)).isEqualTo(3L);
            assertThat(createVarcharType(20).getObjectValue(secondPage.getBlock(1), 0)).isEqualTo("gamma");
            assertThat(pageSource.getCompletedPositions().orElseThrow()).isEqualTo(3L);

            assertThat(pageSource.getNextSourcePage()).isNull();
            assertThat(pageSource.isFinished()).isTrue();
        }
    }

    @Test
    void testPageSourceDoesNotPrefetchEndOfStreamBeforeReturningCurrentPage()
    {
        List<StarRocksColumnHandle> columns = List.of(new StarRocksColumnHandle("id", "id", BIGINT, 0));

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                BigIntVector id = new BigIntVector("id", allocator);
                VectorSchemaRoot root = new VectorSchemaRoot(List.of(id))) {
            id.allocateNew();
            id.setSafe(0, 99L);
            id.setValueCount(1);
            root.setRowCount(1);

            TestingStarRocksFlightSqlResult result = new TestingStarRocksFlightSqlResult(List.of(root));
            StarRocksFlightSqlPageSource pageSource = new StarRocksFlightSqlPageSource(result, new StarRocksArrowToPageConverter(), columns);

            SourcePage firstPage = pageSource.getNextSourcePage();
            assertThat(firstPage.getPositionCount()).isEqualTo(1);
            assertThat(BIGINT.getLong(firstPage.getBlock(0), 0)).isEqualTo(99L);
            assertThat(result.loadCalls()).isEqualTo(1);

            assertThat(pageSource.getNextSourcePage()).isNull();
            assertThat(result.loadCalls()).isEqualTo(2);
        }
    }

    private static final class TestingStarRocksFlightSqlResult
            implements StarRocksFlightSqlResult
    {
        private final List<VectorSchemaRoot> roots;
        private final AtomicInteger loadCalls = new AtomicInteger();
        private int index = -1;

        private TestingStarRocksFlightSqlResult(List<VectorSchemaRoot> roots)
        {
            this.roots = new ArrayList<>(roots);
        }

        @Override
        public boolean loadNextBatch()
        {
            loadCalls.incrementAndGet();
            index++;
            return index < roots.size();
        }

        @Override
        public VectorSchemaRoot getVectorSchemaRoot()
        {
            return roots.get(index);
        }

        @Override
        public long getMemoryUsage()
        {
            return roots.stream()
                    .flatMap(root -> root.getFieldVectors().stream())
                    .mapToLong(FieldVector::getBufferSize)
                    .sum();
        }

        @Override
        public void close() {}

        private int loadCalls()
        {
            return loadCalls.get();
        }
    }
}
