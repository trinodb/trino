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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.jaxrs.JsonMapper;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonModule;
import io.airlift.units.DataSize;
import io.trino.exchange.SpoolingExchangeInput;
import io.trino.execution.StageId;
import io.trino.metadata.HandleJsonModule;
import io.trino.metadata.Split;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.split.RemoteSplit;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Optional;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.operator.ExchangeOperator.REMOTE_CATALOG_HANDLE;
import static io.trino.spi.StandardErrorCode.EXCEEDED_TASK_DESCRIPTOR_STORAGE_CAPACITY;
import static io.trino.testing.TestingHandles.createTestCatalogHandle;
import static io.trino.testing.assertions.Assert.assertEventually;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTaskDescriptorStorage
{
    private static final QueryId QUERY_1 = new QueryId("query1");
    private static final QueryId QUERY_2 = new QueryId("query2");

    private static final StageId QUERY_1_STAGE_1 = new StageId(QUERY_1, 1);
    private static final StageId QUERY_1_STAGE_2 = new StageId(QUERY_1, 2);
    private static final StageId QUERY_2_STAGE_1 = new StageId(QUERY_2, 1);
    private static final StageId QUERY_2_STAGE_2 = new StageId(QUERY_2, 2);

    @Test
    public void testHappyPath()
    {
        // disable compression to get expected memory usage
        TaskDescriptorStorage manager = createTaskDescriptorStorage(DataSize.of(15, KILOBYTE), DataSize.of(20, KILOBYTE), DataSize.of(20, KILOBYTE));
        manager.initialize(QUERY_1);
        manager.initialize(QUERY_2);

        manager.put(QUERY_1_STAGE_1, createTaskDescriptor(0, DataSize.of(1, KILOBYTE), "catalog1"));
        manager.put(QUERY_1_STAGE_1, createTaskDescriptor(1, DataSize.of(1, KILOBYTE), "catalog2"));
        manager.put(QUERY_1_STAGE_2, createTaskDescriptor(0, DataSize.of(2, KILOBYTE), "catalog3"));
        manager.put(QUERY_2_STAGE_1, createTaskDescriptor(0, DataSize.of(1, KILOBYTE), "catalog4"));
        manager.put(QUERY_2_STAGE_2, createTaskDescriptor(0, DataSize.of(1, KILOBYTE), "catalog5"));
        manager.put(QUERY_2_STAGE_2, createTaskDescriptor(1, DataSize.of(2, KILOBYTE), "catalog6"));

        assertThat(manager.getReservedUncompressedBytes())
                .isGreaterThanOrEqualTo(toBytes(10, KILOBYTE))
                .isLessThanOrEqualTo(toBytes(15, KILOBYTE));

        assertThat(manager.get(QUERY_1_STAGE_1, 0))
                .flatMap(TestTaskDescriptorStorage::getCatalogName)
                .contains("catalog1");
        assertThat(manager.get(QUERY_1_STAGE_1, 1))
                .flatMap(TestTaskDescriptorStorage::getCatalogName)
                .contains("catalog2");
        assertThat(manager.get(QUERY_1_STAGE_2, 0))
                .flatMap(TestTaskDescriptorStorage::getCatalogName)
                .contains("catalog3");
        assertThat(manager.get(QUERY_2_STAGE_1, 0))
                .flatMap(TestTaskDescriptorStorage::getCatalogName)
                .contains("catalog4");
        assertThat(manager.get(QUERY_2_STAGE_2, 0))
                .flatMap(TestTaskDescriptorStorage::getCatalogName)
                .contains("catalog5");
        assertThat(manager.get(QUERY_2_STAGE_2, 1))
                .flatMap(TestTaskDescriptorStorage::getCatalogName)
                .contains("catalog6");

        manager.remove(QUERY_1_STAGE_1, 0);
        manager.remove(QUERY_2_STAGE_2, 1);

        assertThatThrownBy(() -> manager.get(QUERY_1_STAGE_1, 0))
                .hasMessageContaining("descriptor not found for key");
        assertThatThrownBy(() -> manager.get(QUERY_2_STAGE_2, 1))
                .hasMessageContaining("descriptor not found for key");

        assertThat(manager.getReservedUncompressedBytes())
                .isGreaterThanOrEqualTo(toBytes(6, KILOBYTE))
                .isLessThanOrEqualTo(toBytes(8, KILOBYTE));
    }

    @Test
    public void testDestroy()
    {
        // disable compression to get expected memory usage
        TaskDescriptorStorage manager = new TaskDescriptorStorage(DataSize.of(5, KILOBYTE), DataSize.of(10, KILOBYTE), DataSize.of(10, KILOBYTE), jsonCodec(TaskDescriptor.class), jsonCodec(Split.class));
        manager.initialize(QUERY_1);
        manager.initialize(QUERY_2);

        manager.put(QUERY_1_STAGE_1, createTaskDescriptor(0, DataSize.of(1, KILOBYTE)));
        assertThat(manager.get(QUERY_1_STAGE_1, 0)).isPresent();
        assertThat(manager.getReservedUncompressedBytes()).isGreaterThanOrEqualTo(toBytes(1, KILOBYTE));
        manager.put(QUERY_2_STAGE_1, createTaskDescriptor(0, DataSize.of(1, KILOBYTE)));
        assertThat(manager.get(QUERY_2_STAGE_1, 0)).isPresent();
        assertThat(manager.getReservedUncompressedBytes()).isGreaterThanOrEqualTo(toBytes(2, KILOBYTE));

        manager.destroy(QUERY_1);
        assertThat(manager.get(QUERY_1_STAGE_1, 0)).isEmpty();
        assertThat(manager.get(QUERY_2_STAGE_1, 0)).isPresent();
        assertThat(manager.getReservedUncompressedBytes())
                .isGreaterThanOrEqualTo(toBytes(1, KILOBYTE))
                .isLessThanOrEqualTo(toBytes(2, KILOBYTE));

        manager.destroy(QUERY_2);
        assertThat(manager.get(QUERY_1_STAGE_1, 0)).isEmpty();
        assertThat(manager.get(QUERY_2_STAGE_1, 0)).isEmpty();
        assertThat(manager.getReservedUncompressedBytes()).isEqualTo(0);
    }

    @Test
    public void testCapacityExceeded()
    {
        // disable compression to get expected memory usage
        TaskDescriptorStorage manager = createTaskDescriptorStorage(DataSize.of(5, KILOBYTE), DataSize.of(10, KILOBYTE), DataSize.of(10, KILOBYTE));
        manager.initialize(QUERY_1);
        manager.initialize(QUERY_2);

        manager.put(QUERY_1_STAGE_1, createTaskDescriptor(0, DataSize.of(1, KILOBYTE), "catalog1"));
        manager.put(QUERY_1_STAGE_1, createTaskDescriptor(1, DataSize.of(1, KILOBYTE), "catalog2"));
        manager.put(QUERY_1_STAGE_2, createTaskDescriptor(0, DataSize.of(1, KILOBYTE), "catalog3"));
        manager.put(QUERY_2_STAGE_1, createTaskDescriptor(0, DataSize.of(1, KILOBYTE), "catalog4"));
        manager.put(QUERY_2_STAGE_2, createTaskDescriptor(0, DataSize.of(2, KILOBYTE), "catalog5"));

        // assert that the memory has been released
        assertThat(manager.getReservedUncompressedBytes())
                .isGreaterThanOrEqualTo(toBytes(4, KILOBYTE))
                .isLessThanOrEqualTo(toBytes(5, KILOBYTE));

        // check that the any future operations for QUERY_1 will fail
        assertThatThrownBy(() -> manager.put(QUERY_1_STAGE_1, createTaskDescriptor(0, DataSize.of(1, KILOBYTE))))
                .matches(TestTaskDescriptorStorage::isStorageCapacityExceededFailure);
        assertThatThrownBy(() -> manager.put(QUERY_1_STAGE_2, createTaskDescriptor(1, DataSize.of(1, KILOBYTE))))
                .matches(TestTaskDescriptorStorage::isStorageCapacityExceededFailure);
        assertThatThrownBy(() -> manager.get(QUERY_1_STAGE_1, 0))
                .matches(TestTaskDescriptorStorage::isStorageCapacityExceededFailure);
        assertThatThrownBy(() -> manager.get(QUERY_1_STAGE_1, 1))
                .matches(TestTaskDescriptorStorage::isStorageCapacityExceededFailure);
        assertThatThrownBy(() -> manager.get(QUERY_1_STAGE_2, 0))
                .matches(TestTaskDescriptorStorage::isStorageCapacityExceededFailure);
        assertThatThrownBy(() -> manager.remove(QUERY_1_STAGE_1, 0))
                .matches(TestTaskDescriptorStorage::isStorageCapacityExceededFailure);
        assertThatThrownBy(() -> manager.remove(QUERY_1_STAGE_1, 1))
                .matches(TestTaskDescriptorStorage::isStorageCapacityExceededFailure);
        assertThatThrownBy(() -> manager.remove(QUERY_1_STAGE_2, 0))
                .matches(TestTaskDescriptorStorage::isStorageCapacityExceededFailure);

        // QUERY_2 is still active
        assertThat(manager.get(QUERY_2_STAGE_1, 0))
                .flatMap(TestTaskDescriptorStorage::getCatalogName)
                .contains("catalog4");
        assertThat(manager.get(QUERY_2_STAGE_2, 0))
                .flatMap(TestTaskDescriptorStorage::getCatalogName)
                .contains("catalog5");

        // add more descriptors for QUERY_2 to push the buffer above capacity
        manager.put(QUERY_2_STAGE_2, createTaskDescriptor(1, DataSize.of(3, KILOBYTE), "catalog6"));

        // assert that the memory has been released
        assertThat(manager.getReservedUncompressedBytes()).isEqualTo(0);

        // check that the any future operations for QUERY_2 will fail
        assertThatThrownBy(() -> manager.put(QUERY_2_STAGE_2, createTaskDescriptor(3, DataSize.of(1, KILOBYTE))))
                .matches(TestTaskDescriptorStorage::isStorageCapacityExceededFailure);
        assertThatThrownBy(() -> manager.get(QUERY_2_STAGE_1, 0))
                .matches(TestTaskDescriptorStorage::isStorageCapacityExceededFailure);
        assertThatThrownBy(() -> manager.remove(QUERY_2_STAGE_1, 0))
                .matches(TestTaskDescriptorStorage::isStorageCapacityExceededFailure);
    }

    @Test
    public void testCompression()
    {
        TaskDescriptorStorage manager = createTaskDescriptorStorage(DataSize.of(150, KILOBYTE), DataSize.of(100, KILOBYTE), DataSize.of(80, KILOBYTE));
        manager.initialize(QUERY_1);
        manager.initialize(QUERY_2);

        manager.put(QUERY_1_STAGE_1, createTaskDescriptor(0, DataSize.of(20, KILOBYTE), "catalog1"));
        manager.put(QUERY_1_STAGE_1, createTaskDescriptor(1, DataSize.of(20, KILOBYTE), "catalog2"));
        manager.put(QUERY_1_STAGE_2, createTaskDescriptor(0, DataSize.of(20, KILOBYTE), "catalog3"));
        long q1Bytes = manager.getReservedUncompressedBytes();

        manager.put(QUERY_2_STAGE_1, createTaskDescriptor(2, DataSize.of(10, KILOBYTE), "catalog4"));
        manager.put(QUERY_2_STAGE_2, createTaskDescriptor(0, DataSize.of(20, KILOBYTE), "catalog5"));

        // we are below compression threshold
        assertThat(manager.getReservedUncompressedBytes())
                .isGreaterThanOrEqualTo(toBytes(90, KILOBYTE))
                .isLessThanOrEqualTo(toBytes(100, KILOBYTE));
        assertThat(manager.getReservedCompressedBytes())
                .isEqualTo(0);
        assertThat(manager.getOriginalCompressedBytes())
                .isEqualTo(0);

        // this one pushes us above the threshold
        manager.put(QUERY_2_STAGE_2, createTaskDescriptor(1, DataSize.of(20, KILOBYTE), "catalog3"));
        assertThat(manager.getReservedUncompressedBytes())
                .isGreaterThanOrEqualTo(toBytes(100, KILOBYTE));
        assertThat(manager.getReservedCompressedBytes())
                .isEqualTo(0);
        assertThat(manager.getOriginalCompressedBytes())
                .isEqualTo(0);
        long reservedUncompressedBytesOnHighWater = manager.getReservedUncompressedBytes();

        // next one gets compressed
        manager.put(QUERY_2_STAGE_2, createTaskDescriptor(2, DataSize.of(20, KILOBYTE), "catalog3"));
        assertThat(manager.getReservedUncompressedBytes())
                .isEqualTo(reservedUncompressedBytesOnHighWater);
        assertThat(manager.getReservedCompressedBytes())
                .isGreaterThan(0);
        assertThat(manager.getOriginalCompressedBytes())
                .isGreaterThan(toBytes(20, KILOBYTE));

        long singleTaskCompressedSize = manager.getReservedCompressedBytes();

        // create a couple more so we would be over max if not compression
        manager.put(QUERY_2_STAGE_2, createTaskDescriptor(3, DataSize.of(200, KILOBYTE), "catalog3"));
        manager.put(QUERY_2_STAGE_2, createTaskDescriptor(4, DataSize.of(200, KILOBYTE), "catalog3"));
        assertThat(manager.getReservedUncompressedBytes())
                .isEqualTo(reservedUncompressedBytesOnHighWater);
        assertThat(manager.getReservedCompressedBytes())
                .isGreaterThan(0);
        assertThat(manager.getOriginalCompressedBytes())
                .isGreaterThan(toBytes(420, KILOBYTE));

        // stress it until we get to the threshold
        int i = 5;
        // cross the threshold
        while (true) {
            boolean lastRun = manager.getReservedCompressedBytes() + manager.getReservedUncompressedBytes() + singleTaskCompressedSize > DataSize.of(150, KILOBYTE).toBytes();
            manager.put(QUERY_2_STAGE_2, createTaskDescriptor(i++, DataSize.of(10, KILOBYTE), "catalog3"));
            if (lastRun) {
                break;
            }
        }

        // next operation on Q2 should fail
        assertThatThrownBy(() -> manager.put(QUERY_2_STAGE_2, createTaskDescriptor(1001, DataSize.of(10, KILOBYTE), "catalog3")))
                .hasMessageContaining("Task descriptor storage capacity has been exceeded");

        // memory for Q2 should be released
        assertThat(manager.getReservedUncompressedBytes())
                .isEqualTo(q1Bytes);
        assertThat(manager.getReservedCompressedBytes())
                .isEqualTo(0);
        assertThat(manager.getOriginalCompressedBytes())
                .isEqualTo(0);

        // we are below low water mark here so we should create uncompressed task descriptors again
        manager.put(QUERY_1_STAGE_2, createTaskDescriptor(1, DataSize.of(20, KILOBYTE), "catalog3")); // bump partition 1 to 1KB -> 2KB
        assertThat(manager.getReservedUncompressedBytes())
                .isGreaterThan(q1Bytes);
        assertThat(manager.getReservedCompressedBytes())
                .isEqualTo(0);
        assertThat(manager.getOriginalCompressedBytes())
                .isEqualTo(0);

        // cross the threshold again
        manager.put(QUERY_1_STAGE_2, createTaskDescriptor(2, DataSize.of(50, KILOBYTE), "catalog3"));
        manager.put(QUERY_1_STAGE_2, createTaskDescriptor(3, DataSize.of(50, KILOBYTE), "catalog3"));
        manager.put(QUERY_1_STAGE_2, createTaskDescriptor(4, DataSize.of(50, KILOBYTE), "catalog3"));
        assertThat(manager.getReservedUncompressedBytes())
                .isGreaterThan(0);
        assertThat(manager.getReservedCompressedBytes())
                .isGreaterThan(0);
        assertThat(manager.getOriginalCompressedBytes())
                .isGreaterThan(0);

        // remove Q1; storage should be empty
        manager.remove(QUERY_1_STAGE_1, 0);
        manager.remove(QUERY_1_STAGE_1, 1);
        manager.remove(QUERY_1_STAGE_2, 0);
        manager.remove(QUERY_1_STAGE_2, 1);
        manager.remove(QUERY_1_STAGE_2, 2);
        manager.remove(QUERY_1_STAGE_2, 3);
        manager.remove(QUERY_1_STAGE_2, 4);
        assertThat(manager.getReservedUncompressedBytes())
                .isEqualTo(0);
        assertThat(manager.getReservedCompressedBytes())
                .isEqualTo(0);
        assertThat(manager.getOriginalCompressedBytes())
                .isEqualTo(0);
    }

    @Test
    @Timeout(20)
    public void testBackgroundCompression()
    {
        TaskDescriptorStorage manager = createTaskDescriptorStorage(DataSize.of(150, KILOBYTE), DataSize.of(100, KILOBYTE), DataSize.of(80, KILOBYTE));
        manager.initialize(QUERY_1);
        manager.initialize(QUERY_2);

        manager.put(QUERY_1_STAGE_1, createTaskDescriptor(0, DataSize.of(10, KILOBYTE), "catalog1"));
        manager.put(QUERY_1_STAGE_2, createTaskDescriptor(0, DataSize.of(20, KILOBYTE), "catalog1"));
        manager.put(QUERY_2_STAGE_1, createTaskDescriptor(0, DataSize.of(20, KILOBYTE), "catalog1"));
        manager.put(QUERY_1_STAGE_1, createTaskDescriptor(1, DataSize.of(40, KILOBYTE), "catalog1"));
        manager.put(QUERY_1_STAGE_1, createTaskDescriptor(2, DataSize.of(40, KILOBYTE), "catalog1"));
        manager.put(QUERY_2_STAGE_1, createTaskDescriptor(1, DataSize.of(40, KILOBYTE), "catalog1"));
        manager.put(QUERY_2_STAGE_1, createTaskDescriptor(2, DataSize.of(40, KILOBYTE), "catalog1"));

        // some descriptors are not compressed and some are compressed for both Q1 and Q2
        assertThat(manager.getReservedUncompressedBytes())
                .isGreaterThan(0);
        assertThat(manager.getReservedCompressedBytes())
                .isGreaterThan(0);
        assertThat(manager.getOriginalCompressedBytes())
                .isGreaterThan(0);

        manager.start();
        assertEventually(
                () -> {
                    assertThat(manager.getReservedUncompressedBytes())
                            .isEqualTo(0);
                    assertThat(manager.getReservedCompressedBytes())
                            .isGreaterThan(0);
                    assertThat(manager.getOriginalCompressedBytes())
                            .isGreaterThan(0);
                });
        manager.stop();
    }

    private static TaskDescriptorStorage createTaskDescriptorStorage(DataSize maxMemory, DataSize compressingHighWaterMark, DataSize compressingLowWaterMark)
    {
        // use Bootstrap to ensure proper JSON serializers setup
        Bootstrap app = new Bootstrap(
                new JsonModule(),
                new HandleJsonModule(),
                binder -> {
                    binder.bind(JsonMapper.class).in(Singleton.class);
                    jsonCodecBinder(binder).bindJsonCodec(TaskDescriptor.class);
                    jsonCodecBinder(binder).bindJsonCodec(Split.class);
                });

        Injector injector = app.initialize();
        JsonCodec<TaskDescriptor> taskDescriptorJsonCodec = injector.getInstance(Key.get(new TypeLiteral<>() { }));
        JsonCodec<Split> splitJsonCodec = injector.getInstance(Key.get(new TypeLiteral<>() { }));

        TaskDescriptorStorage manager = new TaskDescriptorStorage(maxMemory, compressingHighWaterMark, compressingLowWaterMark, taskDescriptorJsonCodec, splitJsonCodec);
        return manager;
    }

    private static TaskDescriptor createTaskDescriptor(int partitionId, DataSize retainedSize)
    {
        return createTaskDescriptor(partitionId, retainedSize, Optional.empty());
    }

    private static TaskDescriptor createTaskDescriptor(int partitionId, DataSize retainedSize, String catalogName)
    {
        return createTaskDescriptor(partitionId, retainedSize, Optional.of(createTestCatalogHandle(catalogName)));
    }

    private static TaskDescriptor createTaskDescriptor(int partitionId, DataSize retainedSize, Optional<CatalogHandle> catalog)
    {
        return new TaskDescriptor(
                partitionId,
                SplitsMapping.builder()
                        .addSplit(new PlanNodeId("1"), 1, new Split(REMOTE_CATALOG_HANDLE, new RemoteSplit(new SpoolingExchangeInput(ImmutableList.of(new TestingExchangeSourceHandle(retainedSize.toBytes())), Optional.empty()))))
                        .build(),
                new NodeRequirements(catalog, Optional.empty(), true));
    }

    private static Optional<String> getCatalogName(TaskDescriptor descriptor)
    {
        return descriptor.getNodeRequirements()
                .getCatalogHandle()
                .map(CatalogHandle::getCatalogName)
                .map(CatalogName::toString);
    }

    private static boolean isStorageCapacityExceededFailure(Throwable t)
    {
        if (!(t instanceof TrinoException trinoException)) {
            return false;
        }
        return trinoException.getErrorCode().getCode() == EXCEEDED_TASK_DESCRIPTOR_STORAGE_CAPACITY.toErrorCode().getCode();
    }

    private static long toBytes(int size, DataSize.Unit unit)
    {
        return DataSize.of(size, unit).toBytes();
    }

    public static class TestingExchangeSourceHandle
            implements ExchangeSourceHandle
    {
        private final long retainedSizeInBytes;

        @JsonCreator
        public TestingExchangeSourceHandle(@JsonProperty("retainedSizeInBytes") long retainedSizeInBytes)
        {
            this.retainedSizeInBytes = retainedSizeInBytes;
        }

        @Override
        public int getPartitionId()
        {
            return 0;
        }

        @Override
        public long getDataSizeInBytes()
        {
            return 0;
        }

        @Override
        @JsonProperty
        public long getRetainedSizeInBytes()
        {
            return retainedSizeInBytes;
        }
    }
}
