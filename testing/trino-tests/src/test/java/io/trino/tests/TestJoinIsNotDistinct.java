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
package io.trino.tests;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class TestJoinIsNotDistinct
{
    private static final String LOCAL_CATALOG = "local";
    private static final String DEFAULT_SCHEMA = "default";

    private final QueryRunner queryRunner;

    private final QueryAssertions assertions;

    TestJoinIsNotDistinct()
    {
        queryRunner = new StandaloneQueryRunner(testSessionBuilder()
                .setCatalog(LOCAL_CATALOG)
                .setSchema(DEFAULT_SCHEMA)
                .build());
        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog(LOCAL_CATALOG, "memory", ImmutableMap.of());

        assertions = new QueryAssertions(queryRunner);
    }

    @AfterAll
    void teardown()
    {
        assertions.close();
    }

    @Test
    void testJoinWithIsNotDistinctFromOnNulls()
    {
        String tableName1 = "test_tab_" + randomNameSuffix();
        String tableName2 = "test_tab_" + randomNameSuffix();
        queryRunner.execute(format("CREATE TABLE %s (k1 INT, k2 INT)", tableName1));
        queryRunner.execute(format("CREATE TABLE %s (k1 INT, k2 INT)", tableName2));

        queryRunner.execute(format("INSERT INTO %s VALUES (1, NULL)", tableName1));
        queryRunner.execute(format("INSERT INTO %s VALUES (1, NULL)", tableName2));
        assertThat(assertions.query(format("SELECT *" +
                " FROM %s t" +
                " INNER JOIN %s AS s" +
                "    ON s.k1 IS NOT DISTINCT FROM t.k1" +
                "    AND s.k2 IS NOT DISTINCT FROM t.k2", tableName1, tableName2)))
                .matches("VALUES (1, CAST(NULL AS INTEGER), 1, CAST(NULL AS INTEGER))");

        queryRunner.execute(format("INSERT INTO %s VALUES (NULL, NULL)", tableName1));
        queryRunner.execute(format("INSERT INTO %s VALUES (NULL, NULL)", tableName2));
        assertThat(assertions.query(format("SELECT *" +
                " FROM %s t" +
                " INNER JOIN %s AS s" +
                "    ON s.k1 IS NOT DISTINCT FROM t.k1" +
                "    AND s.k2 IS NOT DISTINCT FROM t.k2", tableName1, tableName2)))
                .matches("VALUES (1, CAST(NULL AS INTEGER), 1, CAST(NULL AS INTEGER))," +
                        " (CAST(NULL AS INTEGER), CAST(NULL AS INTEGER), CAST(NULL AS INTEGER), CAST(NULL AS INTEGER))");

        queryRunner.execute(format("INSERT INTO %s VALUES (NULL, 2)", tableName1));
        queryRunner.execute(format("INSERT INTO %s VALUES (3, NULL)", tableName2));
        assertThat(assertions.query(format("SELECT *" +
                " FROM %s t" +
                " INNER JOIN %s AS s" +
                "    ON s.k1 IS NOT DISTINCT FROM t.k1" +
                "    AND s.k2 IS NOT DISTINCT FROM t.k2", tableName1, tableName2)))
                .matches("VALUES (1, CAST(NULL AS INTEGER), 1, CAST(NULL AS INTEGER))," +
                        " (CAST(NULL AS INTEGER), CAST(NULL AS INTEGER), CAST(NULL AS INTEGER), CAST(NULL AS INTEGER))");

        queryRunner.execute(format("INSERT INTO %s VALUES (2, 2)", tableName1));
        queryRunner.execute(format("INSERT INTO %s VALUES (2, 2)", tableName2));
        assertThat(assertions.query(format("SELECT *" +
                " FROM %s t" +
                " INNER JOIN %s AS s" +
                "    ON s.k1 IS NOT DISTINCT FROM t.k1" +
                "    AND s.k2 IS NOT DISTINCT FROM t.k2", tableName1, tableName2)))
                .matches("VALUES (1, CAST(NULL AS INTEGER), 1, CAST(NULL AS INTEGER))," +
                        " (CAST(NULL AS INTEGER), CAST(NULL AS INTEGER), CAST(NULL AS INTEGER), CAST(NULL AS INTEGER))," +
                        " (2, 2, 2, 2)");
    }

    @Test
    void testJoinWithIsNotDistinctFromOnNullsOnDerivedTables()
    {
        assertThat(assertions.query("SELECT *" +
                " FROM (SELECT 1 AS k1, NULL AS k2) t" +
                " INNER JOIN (SELECT 1 AS k1, NULL AS k2) AS s" +
                "    ON s.k1 IS NOT DISTINCT FROM t.k1" +
                "    AND s.k2 IS NOT DISTINCT FROM t.k2"))
                .matches("VALUES (1, NULL, 1, NULL)");

        assertThat(assertions.query("SELECT *" +
                " FROM (SELECT NULL AS k1, NULL AS k2) t" +
                " INNER JOIN (SELECT NULL AS k1, NULL AS k2) AS s" +
                "    ON s.k1 IS NOT DISTINCT FROM t.k1" +
                "    AND s.k2 IS NOT DISTINCT FROM t.k2"))
                .matches("VALUES (NULL, NULL, NULL, NULL)");

        assertThat(assertions.query("SELECT *" +
                " FROM (SELECT NULL AS k1, 2 AS k2) t" +
                " INNER JOIN (SELECT 3 AS k1, NULL AS k2) AS s" +
                "    ON s.k1 IS NOT DISTINCT FROM t.k1" +
                "    AND s.k2 IS NOT DISTINCT FROM t.k2"))
                .returnsEmptyResult();
    }
}
