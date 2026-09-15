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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHudiPredicates
{
    private static final HiveColumnHandle PARTITION_COLUMN = createBaseColumn("part", 0, HIVE_STRING, VARCHAR, PARTITION_KEY, Optional.empty());
    private static final HiveColumnHandle REGULAR_COLUMN = createBaseColumn("col", 1, HIVE_STRING, VARCHAR, REGULAR, Optional.empty());

    @Test
    public void testNoneConstraintStaysNone()
    {
        HudiPredicates predicates = HudiPredicates.from(TupleDomain.none());

        assertThat(predicates.getPartitionColumnPredicates().isNone()).isTrue();
        assertThat(predicates.getRegularColumnPredicates().isNone()).isTrue();
    }

    @Test
    public void testAllConstraintStaysAll()
    {
        HudiPredicates predicates = HudiPredicates.from(TupleDomain.all());

        assertThat(predicates.getPartitionColumnPredicates().isAll()).isTrue();
        assertThat(predicates.getRegularColumnPredicates().isAll()).isTrue();
    }

    @Test
    public void testConstraintSplitByColumnType()
    {
        Domain partitionDomain = Domain.singleValue(VARCHAR, Slices.utf8Slice("p1"));
        Domain regularDomain = Domain.singleValue(VARCHAR, Slices.utf8Slice("v1"));
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                PARTITION_COLUMN, partitionDomain,
                REGULAR_COLUMN, regularDomain));

        HudiPredicates predicates = HudiPredicates.from(predicate);

        assertThat(predicates.getPartitionColumnPredicates())
                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(PARTITION_COLUMN, partitionDomain)));
        assertThat(predicates.getRegularColumnPredicates())
                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(REGULAR_COLUMN, regularDomain)));
    }

    @Test
    public void testOnlyPartitionColumnConstraintLeavesRegularAll()
    {
        Domain partitionDomain = Domain.singleValue(VARCHAR, Slices.utf8Slice("p1"));
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(PARTITION_COLUMN, partitionDomain));

        HudiPredicates predicates = HudiPredicates.from(predicate);

        assertThat(predicates.getPartitionColumnPredicates())
                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(PARTITION_COLUMN, partitionDomain)));
        assertThat(predicates.getRegularColumnPredicates().isAll()).isTrue();
    }

    @Test
    public void testOnlyRegularColumnConstraintLeavesPartitionAll()
    {
        Domain regularDomain = Domain.singleValue(VARCHAR, Slices.utf8Slice("v1"));
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(REGULAR_COLUMN, regularDomain));

        HudiPredicates predicates = HudiPredicates.from(predicate);

        assertThat(predicates.getPartitionColumnPredicates().isAll()).isTrue();
        assertThat(predicates.getRegularColumnPredicates())
                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(REGULAR_COLUMN, regularDomain)));
    }
}
