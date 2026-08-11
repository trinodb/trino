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
package io.trino.plugin.iceberg;

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

public class TestIcebergStatisticsCatalogDisabled
        extends AbstractTestQueryFramework
{
    private static final String EMPTY_STATS =
            """
            VALUES
              ('nationkey', null, null, null, null, null, null),
              ('regionkey', null, null, null, null, null, null),
              ('comment', null, null, null, null, null, null),
              ('name', null, null, null, null, null, null),
              (null, null, null, null, null, null, null)""";
    private static final String GOOD_STATS =
            """
            VALUES
              ('nationkey', null, 25, 0, null, '0', '24'),
              ('regionkey', null, 5, 0, null, '0', '4'),
              ('comment', 2087.0, 25, 0, null, null, null),
              ('name', 513.0, 25, 0, null, null, null),
              (null, null, null, null, 25, null, null)""";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.table-statistics-enabled", "false")
                .build();
    }

    @Test
    public void testTablePropertyOptIn()
    {
        String tableName = "test_stats_opt_in_via_table_property";
        assertUpdate("CREATE TABLE " + tableName + " WITH (table_statistics_enabled = true) AS SELECT * FROM tpch.sf1.nation", 25);
        assertQuery("SHOW STATS FOR " + tableName, GOOD_STATS);

        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES table_statistics_enabled = false");
        assertQuery("SHOW STATS FOR " + tableName, EMPTY_STATS);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCatalogDefaultDisablesStatistics()
    {
        String tableName = "test_stats_disabled_by_catalog_default";
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);
        assertQuery("SHOW STATS FOR " + tableName, EMPTY_STATS);

        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES table_statistics_enabled = true");
        assertQuery("SHOW STATS FOR " + tableName, GOOD_STATS);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testExplicitSessionPropertyOverridesTableProperty()
    {
        String tableName = "test_stats_session_property_override";
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);

        Session statisticsEnabled = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "statistics_enabled", "true")
                .build();
        assertQuery(statisticsEnabled, "SHOW STATS FOR " + tableName, GOOD_STATS);

        Session statisticsDisabled = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "statistics_enabled", "false")
                .build();
        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES table_statistics_enabled = true");
        assertQuery(statisticsDisabled, "SHOW STATS FOR " + tableName, EMPTY_STATS);

        assertUpdate("DROP TABLE " + tableName);
    }
}
