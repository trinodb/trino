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
package io.trino.plugin.faker;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

final class TestFakerViews
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return FakerQueryRunner.builder().build();
    }

    @Test
    void testView()
    {
        @Language("SQL") String query = "SELECT orderkey, orderstatus, (totalprice / 2) half FROM tpch.tiny.orders";
        @Language("SQL") String expectedQuery = "SELECT orderkey, orderstatus, (totalprice / 2) half FROM orders";

        String catalogName = getSession().getCatalog().orElseThrow();
        String schemaName = getSession().getSchema().orElseThrow();
        String testView = "test_view_" + randomNameSuffix();
        String testViewWithComment = "test_view_with_comment_" + randomNameSuffix();
        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet()) // prime the cache, if any
                .doesNotContain(testView);
        assertUpdate("CREATE VIEW " + testView + " AS SELECT 123 x");
        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet())
                .contains(testView);
        assertUpdate("CREATE OR REPLACE VIEW " + testView + " AS " + query);

        assertUpdate("CREATE VIEW " + testViewWithComment + " COMMENT 'view comment' AS SELECT 123 x");
        assertUpdate("CREATE OR REPLACE VIEW " + testViewWithComment + " COMMENT 'view comment updated' AS " + query);

        String testViewOriginal = "test_view_original_" + randomNameSuffix();
        String testViewRenamed = "test_view_renamed_" + randomNameSuffix();
        assertUpdate("CREATE VIEW " + testViewOriginal + " AS " + query);
        assertUpdate("ALTER VIEW " + testViewOriginal + " RENAME TO " + testViewRenamed);

        // verify comment
        assertThat((String) computeScalar("SHOW CREATE VIEW " + testViewWithComment)).contains("COMMENT 'view comment updated'");
        assertThat(query(
                """
                SELECT table_name, comment
                FROM system.metadata.table_comments
                WHERE catalog_name = '%s' AND schema_name = '%s'
                """.formatted(catalogName, schemaName)))
                .skippingTypesCheck()
                .containsAll("VALUES ('" + testView + "', null), ('" + testViewWithComment + "', 'view comment updated')");

        assertUpdate("COMMENT ON VIEW " + testViewWithComment + " IS 'view comment updated twice'");
        assertThat((String) computeScalar("SHOW CREATE VIEW " + testViewWithComment)).contains("COMMENT 'view comment updated twice'");

        // reading
        assertQuery("SELECT * FROM " + testView, expectedQuery);
        assertQuery("SELECT * FROM " + testViewRenamed, expectedQuery);
        assertQuery("SELECT * FROM " + testViewWithComment, expectedQuery);

        assertQuery(
                """
                SELECT *
                FROM %1$s a
                JOIN %1$s b on a.orderkey = b.orderkey
                """.formatted(testView),
                """
                SELECT *
                FROM (%1$s) a
                JOIN (%1$s) b ON a.orderkey = b.orderkey
                """.formatted(expectedQuery));

        assertQuery(
                """
                WITH orders AS (
                  SELECT *
                  FROM tpch.tiny.orders
                  LIMIT 0
                )
                SELECT *
                FROM %s
                """.formatted(testView),
                expectedQuery);

        assertQuery("SELECT * FROM %s.%s.%s".formatted(catalogName, schemaName, testView), expectedQuery);

        assertUpdate("DROP VIEW " + testViewWithComment);

        // information_schema.views without table_name filter
        assertThat(query(
                """
                SELECT table_name, regexp_replace(view_definition, '\\s', '')
                FROM information_schema.views
                WHERE table_schema = '%s'
                """.formatted(schemaName)))
                .skippingTypesCheck()
                .containsAll("VALUES ('%1$s', '%3$s'), ('%2$s', '%3$s')".formatted(testView, testViewRenamed, query.replaceAll("\\s", "")));
        // information_schema.views with table_name filter
        assertQuery(
                """
                SELECT table_name, regexp_replace(view_definition, '\\s', '')
                FROM information_schema.views
                WHERE table_schema = '%s' and table_name IN ('%s', '%s')
                """.formatted(schemaName, testView, testViewRenamed),
                "VALUES ('%1$s', '%3$s'), ('%2$s', '%3$s')".formatted(testView, testViewRenamed, query.replaceAll("\\s", "")));

        // table listing
        assertThat(query("SHOW TABLES"))
                .skippingTypesCheck()
                .containsAll("VALUES '%s', '%s'".formatted(testView, testViewRenamed));
        // information_schema.tables without table_name filter
        assertThat(query(
                """
                SELECT table_name, table_type
                FROM information_schema.tables
                WHERE table_schema = '%s'
                """.formatted(schemaName)))
                .skippingTypesCheck()
                .containsAll("VALUES ('%s', 'VIEW'), ('%s', 'VIEW')".formatted(testView, testViewRenamed));
        // information_schema.tables with table_name filter
        assertQuery(
                """
                SELECT table_name, table_type
                FROM information_schema.tables
                WHERE table_schema = '%s' and table_name IN ('%s', '%s')
                """.formatted(schemaName, testView, testViewRenamed),
                "VALUES ('%s', 'VIEW'), ('%s', 'VIEW')".formatted(testView, testViewRenamed));

        // system.jdbc.tables without filter
        assertThat(query(
                """
                SELECT table_schem, table_name, table_type
                FROM system.jdbc.tables
                """))
                .skippingTypesCheck()
                .containsAll("VALUES ('%1$s', '%2$s', 'VIEW'), ('%1$s', '%3$s', 'VIEW')".formatted(schemaName, testView, testViewRenamed));

        // system.jdbc.tables with table prefix filter
        assertQuery(
                """
                SELECT table_schem, table_name, table_type
                FROM system.jdbc.tables
                WHERE table_cat = '%s' AND table_schem = '%s' AND table_name IN ('%s', '%s')
                """.formatted(catalogName, schemaName, testView, testViewRenamed),
                "VALUES ('%1$s', '%2$s', 'VIEW'), ('%1$s', '%3$s', 'VIEW')".formatted(schemaName, testView, testViewRenamed));

        // column listing
        assertThat(query("SHOW COLUMNS FROM " + testView))
                .result()
                .projected("Column") // column types can very between connectors
                .skippingTypesCheck()
                .matches("VALUES 'orderkey', 'orderstatus', 'half'");

        assertThat(query("DESCRIBE " + testView))
                .result()
                .projected("Column") // column types can very between connectors
                .skippingTypesCheck()
                .matches("VALUES 'orderkey', 'orderstatus', 'half'");

        // information_schema.columns without table_name filter
        assertThat(query(
                """
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE table_schema = '%s'
                """.formatted(schemaName)))
                .skippingTypesCheck()
                .containsAll(
                        """
                        SELECT *
                        FROM (VALUES '%s', '%s')
                        CROSS JOIN UNNEST(ARRAY['orderkey', 'orderstatus', 'half'])
                        """.formatted(testView, testViewRenamed));

        // information_schema.columns with table_name filter
        assertThat(query(
                """
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE table_schema = '%s' and table_name IN ('%s', '%s')
                """.formatted(schemaName, testView, testViewRenamed)))
                .skippingTypesCheck()
                .containsAll(
                        """
                        SELECT *
                        FROM (VALUES '%s', '%s')
                        CROSS JOIN UNNEST(ARRAY['orderkey', 'orderstatus', 'half'])
                        """.formatted(testView, testViewRenamed));

        // view-specific listings
        assertThat(query(
                """
                SELECT table_name
                FROM information_schema.views
                WHERE table_schema = '%s'
                """.formatted(schemaName)))
                .skippingTypesCheck()
                .containsAll("VALUES '%s', '%s'".formatted(testView, testViewRenamed));

        // system.jdbc.columns without filter
        assertThat(query(
                """
                SELECT table_schem, table_name, column_name
                FROM system.jdbc.columns
                """))
                .skippingTypesCheck()
                .containsAll(
                        """
                        SELECT *
                        FROM (VALUES ('%1$s', '%2$s'), ('%1$s', '%3$s'))
                        CROSS JOIN UNNEST(ARRAY['orderkey', 'orderstatus', 'half'])
                        """.formatted(schemaName, testView, testViewRenamed));

        // system.jdbc.columns with schema filter
        assertThat(query(
                """
                SELECT table_schem, table_name, column_name
                FROM system.jdbc.columns
                WHERE table_schem LIKE '%%%s%%'
                """.formatted(schemaName)))
                .skippingTypesCheck()
                .containsAll(
                        """
                        SELECT *
                        FROM (VALUES ('%1$s', '%2$s'), ('%1$s', '%3$s'))
                        CROSS JOIN UNNEST(ARRAY['orderkey', 'orderstatus', 'half'])
                        """.formatted(schemaName, testView, testViewRenamed));

        // system.jdbc.columns with table filter
        assertThat(query(
                """
                SELECT table_schem, table_name, column_name
                FROM system.jdbc.columns
                WHERE table_name LIKE '%%%s%%' OR table_name LIKE '%%%s%%'
                """.formatted(testView, testViewRenamed)))
                .skippingTypesCheck()
                .containsAll(
                        """
                        SELECT *
                        FROM (VALUES ('%1$s', '%2$s'), ('%1$s', '%3$s'))
                        CROSS JOIN UNNEST(ARRAY['orderkey', 'orderstatus', 'half'])
                        """.formatted(schemaName, testView, testViewRenamed));

        assertUpdate("DROP VIEW " + testView);
        assertUpdate("DROP VIEW " + testViewRenamed);
        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet())
                .doesNotContainAnyElementsOf(List.of(testView, testViewRenamed, testViewWithComment));
    }

    @Test
    void testViewConflicts()
    {
        String catalogName = getSession().getCatalog().orElseThrow();
        String schemaName = getSession().getSchema().orElseThrow();

        String testTable = "test_table_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + testTable + " (orderkey INT, orderstatus VARCHAR(255), half VARCHAR(255))");

        assertQueryFails("CREATE VIEW " + testTable + " AS SELECT 123 x", "line 1:1: Table already exists: '%s.%s.%s'".formatted(catalogName, schemaName, testTable));

        String testView = "test_view_" + randomNameSuffix();
        assertUpdate("CREATE VIEW " + testView + " AS SELECT 123 x");

        assertQueryFails("CREATE VIEW " + testView + " AS SELECT 123 x", "line 1:1: View already exists: '%s.%s.%s'".formatted(catalogName, schemaName, testView));
        assertQueryFails("CREATE TABLE " + testView + " (orderkey INT, orderstatus VARCHAR(255), half VARCHAR(255))", "View '%s.%s' already exists".formatted(schemaName, testView));
        assertQueryFails("ALTER VIEW " + testView + " RENAME TO " + testTable, "line 1:1: Target view '%s.%s.%s' does not exist, but a table with that name exists.".formatted(catalogName, schemaName, testTable));
        assertQueryFails("ALTER TABLE " + testTable + " RENAME TO " + testView, "line 1:1: Target table '%s.%s.%s' does not exist, but a view with that name exists.".formatted(catalogName, schemaName, testView));

        assertUpdate("DROP VIEW " + testView);
        assertUpdate("DROP TABLE " + testTable);
    }
}
