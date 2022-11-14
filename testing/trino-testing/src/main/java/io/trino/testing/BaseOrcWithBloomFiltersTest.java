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
package io.trino.testing;

import io.trino.Session;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseOrcWithBloomFiltersTest
        extends AbstractTestQueryFramework
{
    protected abstract String getTableProperties(String bloomFilterColumnName, String bucketingColumnName);

    @Test
    public void testOrcBloomFilterIsWrittenDuringCreate()
    {
        String tableName = "create_orc_with_bloom_filters_" + randomNameSuffix();
        assertUpdate(
                format(
                        "CREATE TABLE %s WITH (%s) AS SELECT orderstatus, totalprice FROM tpch.tiny.orders",
                        tableName,
                        getTableProperties("totalprice", "orderstatus")),
                15000);

        // `totalprice 51890 is chosen to lie between min/max values of row group
        assertBloomFilterBasedRowGroupPruning(format("SELECT * FROM %s WHERE totalprice = 51890", tableName));
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInvalidOrcBloomFilterColumnsDuringCreate()
    {
        String tableName = "create_orc_with_bloom_filters_" + randomNameSuffix();
        assertThatThrownBy(() -> computeActual(
                format(
                        "CREATE TABLE %s WITH (%s) AS SELECT orderstatus FROM tpch.tiny.orders",
                        tableName,
                        getTableProperties("totalprice", "orderstatus"))))
                .hasMessage("Orc bloom filter columns [totalprice] not present in schema");
    }

    @Test
    public void testOrcBloomFilterIsWrittenDuringInsert()
    {
        String tableName = "insert_orc_with_bloom_filters_" + randomNameSuffix();
        assertUpdate(
                format(
                        "CREATE TABLE %s (totalprice DOUBLE, orderstatus VARCHAR) WITH (%s)",
                        tableName,
                        getTableProperties("totalprice", "orderstatus")));
        assertUpdate(format("INSERT INTO %s SELECT totalprice, orderstatus FROM tpch.tiny.orders", tableName), 15000);

        // `totalprice 51890 is chosen to lie between min/max values of row group
        assertBloomFilterBasedRowGroupPruning(format("SELECT * FROM %s WHERE totalprice = 51890", tableName));
        assertUpdate("DROP TABLE " + tableName);
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
                    assertThat(queryStats.getPhysicalInputPositions()).isEqualTo(0);
                    assertThat(queryStats.getProcessedInputPositions()).isEqualTo(0);
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
