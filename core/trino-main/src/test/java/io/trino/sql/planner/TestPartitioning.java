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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPartitioning
{
    private static final Symbol COLUMN = new Symbol(BIGINT, "column");

    @Test
    public void testHashPartitioningWithoutColumns()
    {
        // hashing zero columns sends every row to the same partition
        Partitioning partitioning = Partitioning.create(FIXED_HASH_DISTRIBUTION, ImmutableList.of());
        assertThat(partitioning.isPartitionedOn(ImmutableList.of(COLUMN), ImmutableSet.of())).isTrue();
    }

    @Test
    public void testConnectorPartitioningWithoutColumns()
    {
        // a connector partitioning without columns keeps the whole table on one worker
        PartitioningHandle handle = new PartitioningHandle(
                Optional.of(TEST_CATALOG_HANDLE),
                Optional.of(new ConnectorTransactionHandle() {}),
                new ConnectorPartitioningHandle() {});
        Partitioning partitioning = Partitioning.create(handle, ImmutableList.of());
        assertThat(partitioning.isPartitionedOn(ImmutableList.of(COLUMN), ImmutableSet.of())).isTrue();
    }
}
