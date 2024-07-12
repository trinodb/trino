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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.trino.plugin.hive.metastore.MetastoreMethod;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.plugin.hive.metastore.MetastoreInvocations.assertMetastoreInvocationsForQuery;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_TABLE;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.REGION;

public class TestDeltaLakePerTransactionMetastoreCache
{
    private static QueryRunner createQueryRunner(boolean enablePerTransactionHiveMetastoreCaching)
            throws Exception
    {
        Map<String, String> deltaLakeProperties = new HashMap<>();
        deltaLakeProperties.put("delta.register-table-procedure.enabled", "true");
        if (!enablePerTransactionHiveMetastoreCaching) {
            // almost disable the cache; 0 is not allowed as config property value
            deltaLakeProperties.put("delta.per-transaction-metastore-cache-maximum-size", "1");
        }

        return DeltaLakeQueryRunner.builder()
                .setDeltaProperties(deltaLakeProperties)
                .setInitialTables(List.of(NATION, REGION))
                .build();
    }

    @Test
    public void testPerTransactionHiveMetastoreCachingEnabled()
            throws Exception
    {
        try (QueryRunner queryRunner = createQueryRunner(true)) {
            // Verify cache works; we expect only two calls to `getTable` because we have two tables in a query.
            assertMetastoreInvocations(queryRunner, "SELECT * FROM nation JOIN region ON nation.regionkey = region.regionkey",
                    ImmutableMultiset.<MetastoreMethod>builder()
                            .addCopies(GET_TABLE, 2)
                            .build());
        }
    }

    @Test
    public void testPerTransactionHiveMetastoreCachingDisabled()
            throws Exception
    {
        try (QueryRunner queryRunner = createQueryRunner(false)) {
            assertMetastoreInvocations(queryRunner, "SELECT * FROM nation JOIN region ON nation.regionkey = region.regionkey",
                    ImmutableMultiset.<MetastoreMethod>builder()
                            .addCopies(GET_TABLE, 2)
                            .build());
        }
    }

    private static void assertMetastoreInvocations(QueryRunner queryRunner, @Language("SQL") String query, Multiset<MetastoreMethod> expectedInvocations)
    {
        assertMetastoreInvocationsForQuery(queryRunner, queryRunner.getDefaultSession(), query, expectedInvocations);
    }
}
