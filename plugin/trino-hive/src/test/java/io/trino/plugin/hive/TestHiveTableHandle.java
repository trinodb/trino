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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveTableHandle
{
    private final JsonCodec<HiveTableHandle> codec = JsonCodec.jsonCodec(HiveTableHandle.class);

    @Test
    public void testRoundTrip()
    {
        HiveTableHandle expected = new HiveTableHandle("schema", "table", ImmutableMap.of(), ImmutableList.of(), ImmutableList.of(), Optional.empty());

        String json = codec.toJson(expected);
        HiveTableHandle actual = codec.fromJson(json);

        assertThat(actual.getSchemaTableName()).isEqualTo(expected.getSchemaTableName());
    }

    @Test
    public void testGetCanonicalTableHandle()
    {
        HiveColumnHandle hiveColumnHandle = createBaseColumn("any", 0, HIVE_STRING, VARCHAR, PARTITION_KEY, Optional.empty());
        TupleDomain<HiveColumnHandle> compactEffectivePredicate = TupleDomain.withColumnDomains(ImmutableMap.of(hiveColumnHandle, Domain.create(ValueSet.none(VARCHAR), false)));
        HiveTableHandle handle = new HiveTableHandle(
                "schema",
                "table",
                ImmutableList.of(),
                ImmutableList.of(),
                compactEffectivePredicate,
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                AcidTransaction.NO_ACID_TRANSACTION);

        HiveTableHandle canonicalHandle = handle.toCanonical();

        assertThat(canonicalHandle.getSchemaName()).isEqualTo(handle.getSchemaName());
        assertThat(canonicalHandle.getTableName()).isEqualTo(handle.getTableName());
        assertThat(canonicalHandle.getPartitionColumns()).isEqualTo(handle.getPartitionColumns());
        assertThat(canonicalHandle.getDataColumns()).isEqualTo(handle.getDataColumns());
        assertThat(canonicalHandle.getCompactEffectivePredicate()).isEqualTo(TupleDomain.all());
        assertThat(canonicalHandle.getEnforcedConstraint()).isEqualTo(handle.getEnforcedConstraint());
        assertThat(canonicalHandle.getBucketHandle()).isEqualTo(handle.getBucketHandle());
        assertThat(canonicalHandle.getBucketFilter()).isEqualTo(handle.getBucketFilter());
        assertThat(canonicalHandle.getAnalyzePartitionValues()).isEqualTo(handle.getAnalyzePartitionValues());
        assertThat(canonicalHandle.getTransaction()).isEqualTo(handle.getTransaction());
    }
}
