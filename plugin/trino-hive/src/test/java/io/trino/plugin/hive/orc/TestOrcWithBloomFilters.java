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
package io.trino.plugin.hive.orc;

import io.trino.Session;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOrcWithBloomFilters
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .addHiveProperty("hive.orc.bloom-filters.enabled", "true")
                .addHiveProperty("hive.orc.default-bloom-filter-fpp", "0.001")
                .build();
    }

    @Test
    public void testOrcBloomFilterIsWrittenDuringCreate()
    {
        String tableName = "create_orc_with_bloom_filters_" + randomTableSuffix();
        assertUpdate(
                format(
                        "CREATE TABLE %s WITH (%s) AS SELECT orderstatus, totalprice FROM tpch.tiny.orders",
                        tableName,
                        getTableProperties("totalprice", "orderstatus", "totalprice")),
                15000);

        // `totalprice 51890 is chosen to lie between min/max values of row group
        assertBloomFilterBasedRowGroupPruning(format("SELECT * FROM %s WHERE totalprice = 51890", tableName));
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testOrcBloomFilterIsWrittenDuringInsert()
    {
        String tableName = "insert_orc_with_bloom_filters_" + randomTableSuffix();
        assertUpdate(
                format(
                        "CREATE TABLE %s (totalprice DOUBLE, orderstatus VARCHAR) WITH (%s)",
                        tableName,
                        getTableProperties("totalprice", "orderstatus", "totalprice")));
        assertUpdate(format("INSERT INTO %s SELECT totalprice, orderstatus FROM tpch.tiny.orders", tableName), 15000);

        // `totalprice 51890 is chosen to lie between min/max values of row group
        assertBloomFilterBasedRowGroupPruning(format("SELECT * FROM %s WHERE totalprice = 51890", tableName));
        assertUpdate("DROP TABLE " + tableName);
    }

    private String getTableProperties(String bloomFilterColumnName, String bucketingColumnName, String sortByColumnName)
    {
        return format(
                "orc_bloom_filter_columns = ARRAY['%s'], bucketed_by = array['%s'], bucket_count = 1, sorted_by = ARRAY['%s']",
                bloomFilterColumnName,
                bucketingColumnName,
                sortByColumnName);
    }

    private void assertBloomFilterBasedRowGroupPruning(@Language("SQL") String sql)
    {
        assertQueryStats(
                disableBloomFilters(getSession()),
                sql,
                queryStats -> {
                    assertThat(queryStats.getPhysicalInputPositions()).isGreaterThan(0);
                    assertThat(queryStats.getProcessedInputPositions()).isEqualTo(queryStats.getPhysicalInputPositions());
                },
                results -> assertThat(results.getRowCount()).isEqualTo(0));

        assertQueryStats(
                getSession(),
                sql,
                queryStats -> {
                    // This should be evaluated to 0
                    assertThat(queryStats.getPhysicalInputPositions()).isGreaterThan(0);
                    // This should be evaluated to 0
                    assertThat(queryStats.getProcessedInputPositions()).isEqualTo(queryStats.getPhysicalInputPositions());
                },
                results -> assertThat(results.getRowCount()).isEqualTo(0));
    }

    private Session disableBloomFilters(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "orc_bloom_filters_enabled", "false")
                .build();
    }
}
