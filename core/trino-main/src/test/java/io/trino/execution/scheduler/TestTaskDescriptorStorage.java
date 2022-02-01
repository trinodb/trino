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
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.trino.connector.CatalogName;
import io.trino.execution.StageId;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.sql.planner.plan.PlanNodeId;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.spi.StandardErrorCode.EXCEEDED_TASK_DESCRIPTOR_STORAGE_CAPACITY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

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
        TaskDescriptorStorage manager = new TaskDescriptorStorage(DataSize.of(10, KILOBYTE));
        manager.initialize(QUERY_1);
        manager.initialize(QUERY_2);

        manager.put(QUERY_1_STAGE_1, createTaskDescriptor(0, DataSize.of(1, KILOBYTE), "catalog1"));
        manager.put(QUERY_1_STAGE_1, createTaskDescriptor(1, DataSize.of(1, KILOBYTE), "catalog2"));
        manager.put(QUERY_1_STAGE_2, createTaskDescriptor(0, DataSize.of(2, KILOBYTE), "catalog3"));
        manager.put(QUERY_2_STAGE_1, createTaskDescriptor(0, DataSize.of(1, KILOBYTE), "catalog4"));
        manager.put(QUERY_2_STAGE_2, createTaskDescriptor(0, DataSize.of(1, KILOBYTE), "catalog5"));
        manager.put(QUERY_2_STAGE_2, createTaskDescriptor(1, DataSize.of(2, KILOBYTE), "catalog6"));

        assertThat(manager.getReservedBytes())
                .isGreaterThanOrEqualTo(toBytes(8, KILOBYTE))
                .isLessThanOrEqualTo(toBytes(10, KILOBYTE));

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

        manager.destroy(QUERY_1);
        manager.destroy(QUERY_2);
    }

    @Test
    public void testDestroy()
    {
        TaskDescriptorStorage manager = new TaskDescriptorStorage(DataSize.of(5, KILOBYTE));
        manager.initialize(QUERY_1);
        manager.initialize(QUERY_2);

        manager.put(QUERY_1_STAGE_1, createTaskDescriptor(0, DataSize.of(1, KILOBYTE)));
        assertThat(manager.get(QUERY_1_STAGE_1, 0)).isPresent();
        assertThat(manager.getReservedBytes()).isGreaterThanOrEqualTo(toBytes(1, KILOBYTE));
        manager.put(QUERY_2_STAGE_1, createTaskDescriptor(0, DataSize.of(1, KILOBYTE)));
        assertThat(manager.get(QUERY_2_STAGE_1, 0)).isPresent();
        assertThat(manager.getReservedBytes()).isGreaterThanOrEqualTo(toBytes(2, KILOBYTE));

        manager.destroy(QUERY_1);
        assertThat(manager.get(QUERY_1_STAGE_1, 0)).isEmpty();
        assertThat(manager.get(QUERY_2_STAGE_1, 0)).isPresent();
        assertThat(manager.getReservedBytes())
                .isGreaterThanOrEqualTo(toBytes(1, KILOBYTE))
                .isLessThanOrEqualTo(toBytes(2, KILOBYTE));

        manager.destroy(QUERY_2);
        assertThat(manager.get(QUERY_1_STAGE_1, 0)).isEmpty();
        assertThat(manager.get(QUERY_2_STAGE_1, 0)).isEmpty();
        assertEquals(manager.getReservedBytes(), 0);
    }

    @Test
    public void testCapacityExceeded()
    {
        TaskDescriptorStorage manager = new TaskDescriptorStorage(DataSize.of(5, KILOBYTE));
        manager.initialize(QUERY_1);
        manager.initialize(QUERY_2);

        manager.put(QUERY_1_STAGE_1, createTaskDescriptor(0, DataSize.of(1, KILOBYTE), "catalog1"));
        manager.put(QUERY_1_STAGE_1, createTaskDescriptor(1, DataSize.of(1, KILOBYTE), "catalog2"));
        manager.put(QUERY_1_STAGE_2, createTaskDescriptor(0, DataSize.of(1, KILOBYTE), "catalog3"));
        manager.put(QUERY_2_STAGE_1, createTaskDescriptor(0, DataSize.of(1, KILOBYTE), "catalog4"));
        manager.put(QUERY_2_STAGE_2, createTaskDescriptor(0, DataSize.of(2, KILOBYTE), "catalog5"));

        // assert that the memory has been released
        assertThat(manager.getReservedBytes())
                .isGreaterThanOrEqualTo(toBytes(3, KILOBYTE))
                .isLessThanOrEqualTo(toBytes(4, KILOBYTE));

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
        assertEquals(manager.getReservedBytes(), 0);

        // check that the any future operations for QUERY_2 will fail
        assertThatThrownBy(() -> manager.put(QUERY_2_STAGE_2, createTaskDescriptor(3, DataSize.of(1, KILOBYTE))))
                .matches(TestTaskDescriptorStorage::isStorageCapacityExceededFailure);
        assertThatThrownBy(() -> manager.get(QUERY_2_STAGE_1, 0))
                .matches(TestTaskDescriptorStorage::isStorageCapacityExceededFailure);
    }

    private static TaskDescriptor createTaskDescriptor(int partitionId, DataSize retainedSize)
    {
        return createTaskDescriptor(partitionId, retainedSize, Optional.empty());
    }

    private static TaskDescriptor createTaskDescriptor(int partitionId, DataSize retainedSize, String catalogName)
    {
        return createTaskDescriptor(partitionId, retainedSize, Optional.of(new CatalogName(catalogName)));
    }

    private static TaskDescriptor createTaskDescriptor(int partitionId, DataSize retainedSize, Optional<CatalogName> catalog)
    {
        return new TaskDescriptor(
                partitionId,
                ImmutableListMultimap.of(),
                ImmutableListMultimap.of(new PlanNodeId("1"), new TestingExchangeSourceHandle(retainedSize.toBytes())),
                new NodeRequirements(catalog, ImmutableSet.of()));
    }

    private static Optional<String> getCatalogName(TaskDescriptor descriptor)
    {
        return descriptor.getNodeRequirements()
                .getCatalogName()
                .map(CatalogName::getCatalogName);
    }

    private static boolean isStorageCapacityExceededFailure(Throwable t)
    {
        if (!(t instanceof TrinoException)) {
            return false;
        }
        TrinoException e = (TrinoException) t;
        return e.getErrorCode().getCode() == EXCEEDED_TASK_DESCRIPTOR_STORAGE_CAPACITY.toErrorCode().getCode();
    }

    private static long toBytes(int size, DataSize.Unit unit)
    {
        return DataSize.of(size, unit).toBytes();
    }

    private static class TestingExchangeSourceHandle
            implements ExchangeSourceHandle
    {
        private final long retainedSizeInBytes;

        private TestingExchangeSourceHandle(long retainedSizeInBytes)
        {
            this.retainedSizeInBytes = retainedSizeInBytes;
        }

        @Override
        public int getPartitionId()
        {
            return 0;
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return retainedSizeInBytes;
        }
    }
}
