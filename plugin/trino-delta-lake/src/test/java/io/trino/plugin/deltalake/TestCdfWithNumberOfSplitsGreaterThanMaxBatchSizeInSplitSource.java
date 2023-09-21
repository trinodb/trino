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

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createDeltaLakeQueryRunner;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCdfWithNumberOfSplitsGreaterThanMaxBatchSizeInSplitSource
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createDeltaLakeQueryRunner(DELTA_CATALOG, ImmutableMap.of(
                        "query.schedule-split-batch-size", "1",
                        "node-scheduler.max-splits-per-node", "1",
                        "node-scheduler.min-pending-splits-per-task", "1"),
                ImmutableMap.of("delta.enable-non-concurrent-writes", "true"));
    }

    @Test
    public void testReadCdfChanges()
    {
        String tableName = "test_basic_operations_on_table_with_cdf_enabled_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (page_url VARCHAR, domain VARCHAR, views INTEGER) WITH (change_data_feed_enabled = true)");
        assertUpdate("INSERT INTO " + tableName + " VALUES('url1', 'domain1', 1), ('url2', 'domain2', 2), ('url3', 'domain3', 3)", 3);
        assertUpdate("INSERT INTO " + tableName + " VALUES('url4', 'domain4', 4), ('url5', 'domain5', 2), ('url6', 'domain6', 6)", 3);

        assertUpdate("UPDATE " + tableName + " SET page_url = 'url22' WHERE views = 2", 2);
        assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes('tpch', '" + tableName + "'))",
                """
                        VALUES
                            ('url1', 'domain1', 1, 'insert', BIGINT '1'),
                            ('url2', 'domain2', 2, 'insert', BIGINT '1'),
                            ('url3', 'domain3', 3, 'insert', BIGINT '1'),
                            ('url4', 'domain4', 4, 'insert', BIGINT '2'),
                            ('url5', 'domain5', 2, 'insert', BIGINT '2'),
                            ('url6', 'domain6', 6, 'insert', BIGINT '2'),
                            ('url2', 'domain2', 2, 'update_preimage', BIGINT '3'),
                            ('url22', 'domain2', 2, 'update_postimage', BIGINT '3'),
                            ('url5', 'domain5', 2, 'update_preimage', BIGINT '3'),
                            ('url22', 'domain5', 2, 'update_postimage', BIGINT '3')
                        """);

        assertUpdate("DELETE FROM " + tableName + " WHERE views = 2", 2);
        assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes('tpch', '" + tableName + "', 3))",
                """
                        VALUES
                            ('url22', 'domain2', 2, 'delete', BIGINT '4'),
                            ('url22', 'domain5', 2, 'delete', BIGINT '4')
                        """);

        assertTableChangesQuery("SELECT * FROM TABLE(system.table_changes('tpch', '" + tableName + "')) ORDER BY _commit_version, _change_type, domain",
                """
                        VALUES
                            ('url1', 'domain1', 1, 'insert', BIGINT '1'),
                            ('url2', 'domain2', 2, 'insert', BIGINT '1'),
                            ('url3', 'domain3', 3, 'insert', BIGINT '1'),
                            ('url4', 'domain4', 4, 'insert', BIGINT '2'),
                            ('url5', 'domain5', 2, 'insert', BIGINT '2'),
                            ('url6', 'domain6', 6, 'insert', BIGINT '2'),
                            ('url22', 'domain2', 2, 'update_postimage', BIGINT '3'),
                            ('url22', 'domain5', 2, 'update_postimage', BIGINT '3'),
                            ('url2', 'domain2', 2, 'update_preimage', BIGINT '3'),
                            ('url5', 'domain5', 2, 'update_preimage', BIGINT '3'),
                            ('url22', 'domain2', 2, 'delete', BIGINT '4'),
                            ('url22', 'domain5', 2, 'delete', BIGINT '4')
                        """);
    }

    private void assertTableChangesQuery(@Language("SQL") String sql, @Language("SQL") String expectedResult)
    {
        assertThat(query(sql))
                .exceptColumns("_commit_timestamp")
                .skippingTypesCheck()
                .matches(expectedResult);
    }
}
