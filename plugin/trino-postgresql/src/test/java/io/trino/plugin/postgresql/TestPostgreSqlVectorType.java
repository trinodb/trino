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
package io.trino.plugin.postgresql;

import io.trino.Session;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TestView;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;

import static io.trino.plugin.postgresql.PostgreSqlConfig.ArrayMapping.AS_ARRAY;
import static io.trino.plugin.postgresql.PostgreSqlConfig.ArrayMapping.AS_JSON;
import static io.trino.plugin.postgresql.PostgreSqlSessionProperties.ARRAY_MAPPING;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestPostgreSqlVectorType
        extends AbstractTestQueryFramework
{
    private TestingPostgreSqlServer postgreSqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        postgreSqlServer = closeAfterClass(new TestingPostgreSqlServer("pgvector/pgvector:0.7.2-pg16", false));
        // 'SCHEMA public' is required until the connector supports pushdown vector type which is installed in other schemas
        postgreSqlServer.execute("CREATE EXTENSION vector SCHEMA public");
        return PostgreSqlQueryRunner.builder(postgreSqlServer).build();
    }

    @Test
    void testVector()
    {
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_vector", "(v vector(1))")) {
            postgreSqlServer.execute("INSERT INTO " + table.getName() + " VALUES ('[1]')");
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES ARRAY[REAL '1.0']");
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE v = ARRAY[REAL '1.0']"))
                    .matches("VALUES ARRAY[REAL '1.0']");
            assertQueryReturnsEmptyResult("SELECT * FROM " + table.getName() + " WHERE v = ARRAY[REAL '2.0']");

            assertThatThrownBy(() -> postgreSqlServer.execute("INSERT INTO " + table.getName() + " VALUES ('[NaN]')"))
                    .hasMessageContaining("NaN not allowed in vector");
            assertThatThrownBy(() -> postgreSqlServer.execute("INSERT INTO " + table.getName() + " VALUES ('[-Infinity]')"))
                    .hasMessageContaining("infinite value not allowed in vector");
            assertThatThrownBy(() -> postgreSqlServer.execute("INSERT INTO " + table.getName() + " VALUES ('[+Infinity]')"))
                    .hasMessageContaining("infinite value not allowed in vector");
        }

        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_vector", "(v vector(2))")) {
            postgreSqlServer.execute("INSERT INTO " + table.getName() + " VALUES ('[1.1,2.2]')");
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES ARRAY[REAL '1.1', REAL '2.2']");
        }

        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_vector", "(v vector(3))")) {
            postgreSqlServer.execute("INSERT INTO " + table.getName() + " VALUES ('[1.11,2.22,3.33]')");
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES ARRAY[REAL '1.11', REAL '2.22', REAL '3.33']");
        }
    }

    @Test
    void testVectorArray()
    {
        Session arrayAsArray = arrayMappingSession(AS_ARRAY);
        Session arrayAsJson = arrayMappingSession(AS_JSON);

        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_vector_array", "(v vector(1)[])")) {
            postgreSqlServer.execute("INSERT INTO " + table.getName() + " VALUES (array['[1]'::vector])");

            assertQueryFails(
                    arrayAsArray,
                    "INSERT INTO " + table.getName() + " VALUES ARRAY[ARRAY[REAL '2']]",
                    "(?s).*invalid input syntax for type vector.*");
            assertThat(query(arrayAsArray, "SELECT * FROM " + table.getName()))
                    .matches("VALUES ARRAY[ARRAY[REAL '1.0']]");

            assertQueryFails(
                    arrayAsJson,
                    "INSERT INTO " + table.getName() + " VALUES JSON '[[2]]'",
                    "Writing to array type is unsupported");
            assertThat(query(arrayAsJson, "SELECT * FROM " + table.getName()))
                    .matches("VALUES JSON '[[1.0]]'");
        }
    }

    private Session arrayMappingSession(PostgreSqlConfig.ArrayMapping arrayMapping)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("postgresql", ARRAY_MAPPING, arrayMapping.name())
                .build();
    }

    @Test
    void testVectorUnsupportedWrite()
    {
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_vector_writes", "(v vector(1))")) {
            assertQueryFails("INSERT INTO " + table.getName() + " VALUES ARRAY[REAL '1.0']", "Writing to vector type is unsupported");
            assertQueryFails("INSERT INTO " + table.getName() + " VALUES ARRAY[REAL '1.0', REAL '2.0']", "Writing to vector type is unsupported");
            assertQueryFails("INSERT INTO " + table.getName() + " VALUES ARRAY[]", "Writing to vector type is unsupported");
            assertQueryFails("INSERT INTO " + table.getName() + " VALUES ARRAY[nan()]", "Writing to vector type is unsupported");
            assertQueryFails("INSERT INTO " + table.getName() + " VALUES ARRAY[-infinity()]", "Writing to vector type is unsupported");
            assertQueryFails("INSERT INTO " + table.getName() + " VALUES ARRAY[+infinity()]", "Writing to vector type is unsupported");

            assertQueryReturnsEmptyResult("SELECT * FROM " + table.getName());
        }
    }

    @Test
    void testVectorNull()
    {
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_vector_null", "(id int, v1 vector(1), v2 vector(2))")) {
            postgreSqlServer.execute("INSERT INTO " + table.getName() + " VALUES (1, NULL, NULL), (2, '[1]', '[1,2]')");
            assertThat(query("SELECT v1 FROM " + table.getName()))
                    .matches("VALUES CAST(NULL AS ARRAY(REAL)), ARRAY[REAL '1.0']");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE v1 IS NULL"))
                    .matches("VALUES 1");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE v1 IS NOT NULL"))
                    .matches("VALUES 2");
        }
    }

    @Test
    void testVectorArbitraryDimension()
    {
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_vector_arbitrary", "(id int, v vector)")) {
            postgreSqlServer.execute("INSERT INTO " + table.getName() + " VALUES (1, '[1]'), (2, '[1,2]'), (3, '[1,2,3]')");
            assertThat(query("SELECT v FROM " + table.getName()))
                    .matches("VALUES ARRAY[REAL '1.0'], ARRAY[REAL '1.0', REAL '2.0'], ARRAY[REAL '1.0', REAL '2.0', REAL '3.0']");

            postgreSqlServer.execute("SELECT v <-> '[4,5,6]' FROM " + table.getName() + " WHERE id = 3");

            assertThatThrownBy(() -> postgreSqlServer.execute("SELECT v <-> '[4,5,6]' FROM " + table.getName()))
                    .hasMessageContaining("different vector dimensions");
            assertThatThrownBy(() -> postgreSqlServer.execute("INSERT INTO " + table.getName() + " VALUES (4, '[]')"))
                    .hasMessageContaining("vector must have at least 1 dimension");
        }
    }

    @Test
    void testVectorMaxDimension()
    {
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_vector_max", "(v vector(16000))")) {
            String postgresValue = IntStream.rangeClosed(1, 16000).mapToObj(String::valueOf).collect(joining(","));
            postgreSqlServer.execute("INSERT INTO " + table.getName() + " VALUES ('[" + postgresValue + "]')");

            String trinoValue = IntStream.rangeClosed(1, 16000).mapToObj("REAL '%s'"::formatted).collect(joining(","));
            assertThat(query("SELECT v FROM " + table.getName()))
                    .matches("VALUES ARRAY[" + trinoValue + "]");
        }
    }

    @Test
    void testVectorUnsupportedDimension()
    {
        assertThatThrownBy(() -> postgreSqlServer.execute("CREATE TABLE test_vector_unsupported (v vector(-1))"))
                .hasMessageContaining("dimensions for type vector must be at least 1");
        assertThatThrownBy(() -> postgreSqlServer.execute("CREATE TABLE test_vector_unsupported (v vector(0))"))
                .hasMessageContaining("dimensions for type vector must be at least 1");
        assertThatThrownBy(() -> postgreSqlServer.execute("CREATE TABLE test_vector_unsupported (v vector(16001))"))
                .hasMessageContaining("dimensions for type vector cannot exceed 16000");
    }

    @Test
    void testEuclideanDistanceCompatibility()
    {
        // Verify compatibility between Trino euclidean_distance function and pgvector <-> operator
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_vector", "(id int, v vector(3))");
                TestView view = new TestView(postgreSqlServer::execute, "test_euclidean_distance", "SELECT v <-> '[7,8,9]' FROM " + table.getName())) {
            postgreSqlServer.execute("INSERT INTO " + table.getName() + " VALUES (1, '[1,2,3]'), (2, '[4,5,6]')");

            assertThat(query("SELECT euclidean_distance(v, ARRAY[7,8,9]) FROM " + table.getName()))
                    .matches("SELECT * FROM tpch." + view.getName())
                    .isFullyPushedDown();

            assertThat(query("SELECT id FROM " + table.getName() + " ORDER BY euclidean_distance(v, ARRAY[7,8,9]) LIMIT 1"))
                    .isFullyPushedDown();

            assertThatThrownBy(() -> postgreSqlServer.execute("SELECT v <-> '[NaN]' FROM " + table.getName()))
                    .hasMessageContaining("NaN not allowed in vector");
            assertThatThrownBy(() -> postgreSqlServer.execute("SELECT v <-> '[-Infinity]' FROM " + table.getName()))
                    .hasMessageContaining("infinite value not allowed in vector");
            assertThatThrownBy(() -> postgreSqlServer.execute("SELECT v <-> '[+Infinity]' FROM " + table.getName()))
                    .hasMessageContaining("infinite value not allowed in vector");
        }
    }

    @Test
    void testEuclideanDistanceUnsupportedPushdown()
    {
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_vector", "(id int, v vector(1))")) {
            postgreSqlServer.execute("INSERT INTO " + table.getName() + " VALUES (1, '[10]'), (2, '[20]')");

            // The connector doesn't support predicate pushdown with euclidean_distance function
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE euclidean_distance(v, ARRAY[1]) < 1"))
                    .isNotFullyPushedDown(FilterNode.class);

            // The connector doesn't pushdown these values because pgvector throws an exception
            assertThat(query("SELECT id FROM " + table.getName() + " ORDER BY euclidean_distance(v, ARRAY[DOUBLE '1.7976931348623157E+309']) LIMIT 1"))
                    .isNotFullyPushedDown(ProjectNode.class);
            assertThat(query("SELECT id FROM " + table.getName() + " ORDER BY euclidean_distance(v, ARRAY[DOUBLE '-1.7976931348623157E+308']) LIMIT 1"))
                    .isNotFullyPushedDown(ProjectNode.class);
            assertThat(query("SELECT id FROM " + table.getName() + " ORDER BY euclidean_distance(v, ARRAY[REAL 'Infinity']) LIMIT 1"))
                    .isNotFullyPushedDown(ProjectNode.class);
            assertThat(query("SELECT id FROM " + table.getName() + " ORDER BY euclidean_distance(v, ARRAY[REAL '-Infinity']) LIMIT 1"))
                    .isNotFullyPushedDown(ProjectNode.class);
            assertThat(query("SELECT id FROM " + table.getName() + " ORDER BY euclidean_distance(v, ARRAY[REAL 'NaN']) LIMIT 1"))
                    .isNotFullyPushedDown(ProjectNode.class);
            assertThat(query("SELECT id FROM " + table.getName() + " ORDER BY euclidean_distance(v, ARRAY[CAST(NULL AS REAL)]) LIMIT 1"))
                    .isNotFullyPushedDown(ProjectNode.class);
            assertThat(query("SELECT id FROM " + table.getName() + " ORDER BY euclidean_distance(v, NULL) LIMIT 1"))
                    .isNotFullyPushedDown(ProjectNode.class);
        }
    }

    @Test
    void testPgVectorUnsupportedEuclideanDistance()
    {
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_vector", "(v vector(1))")) {
            assertThatThrownBy(() -> postgreSqlServer.execute("SELECT v <-> '[1.7976931348623157E+309]' FROM " + table.getName()))
                    .hasMessageContaining("out of range for type vector");
            assertThatThrownBy(() -> postgreSqlServer.execute("SELECT v <-> '[NaN]' FROM " + table.getName()))
                    .hasMessageContaining("NaN not allowed in vector");
            assertThatThrownBy(() -> postgreSqlServer.execute("SELECT v <-> '[-Infinity]' FROM " + table.getName()))
                    .hasMessageContaining("infinite value not allowed in vector");
            assertThatThrownBy(() -> postgreSqlServer.execute("SELECT v <-> '[+Infinity]' FROM " + table.getName()))
                    .hasMessageContaining("infinite value not allowed in vector");
        }
    }

    @Test
    void testDotProductCompatibility()
    {
        // Verify compatibility between Trino dot_product function and pgvector <#> operator
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_vector", "(id int, v vector(3))");
                TestView view = new TestView(postgreSqlServer::execute, "test_dot_product", "SELECT v <#> '[7,8,9]' FROM " + table.getName())) {
            postgreSqlServer.execute("INSERT INTO " + table.getName() + " VALUES (1, '[1,2,3]'), (2, '[4,5,6]')");

            // The minus sign is needed because <#> returns the negative inner product. Postgres only supports ASC order index scans on operators.
            assertThat(query("SELECT -dot_product(v, ARRAY[7,8,9]) FROM " + table.getName()))
                    .matches("SELECT * FROM tpch." + view.getName())
                    .isFullyPushedDown();

            assertThat(query("SELECT id FROM " + table.getName() + " ORDER BY -dot_product(v, ARRAY[7,8,9]) LIMIT 1"))
                    .isFullyPushedDown();
        }
    }

    @Test
    void testPgVectorUnsupportedNegativeInnerProduct()
    {
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_vector", "(v vector(1))")) {
            assertThatThrownBy(() -> postgreSqlServer.execute("SELECT v <#> '[1.7976931348623157E+309]' FROM " + table.getName()))
                    .hasMessageContaining("out of range for type vector");
            assertThatThrownBy(() -> postgreSqlServer.execute("SELECT v <#> '[NaN]' FROM " + table.getName()))
                    .hasMessageContaining("NaN not allowed in vector");
            assertThatThrownBy(() -> postgreSqlServer.execute("SELECT v <#> '[-Infinity]' FROM " + table.getName()))
                    .hasMessageContaining("infinite value not allowed in vector");
            assertThatThrownBy(() -> postgreSqlServer.execute("SELECT v <#> '[+Infinity]' FROM " + table.getName()))
                    .hasMessageContaining("infinite value not allowed in vector");
        }
    }

    @Test
    void testDotProductUnsupportedPushdown()
    {
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_vector", "(id int, v vector(1))")) {
            postgreSqlServer.execute("INSERT INTO " + table.getName() + " VALUES (1, '[10]'), (2, '[20]')");

            // The connector doesn't support predicate pushdown with dot_product function
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE -dot_product(v, ARRAY[1]) < 1"))
                    .isNotFullyPushedDown(FilterNode.class);

            // The connector doesn't pushdown these values because pgvector throws an exception
            assertThat(query("SELECT id FROM " + table.getName() + " ORDER BY -dot_product(v, ARRAY[DOUBLE '1.7976931348623157E+309']) LIMIT 1"))
                    .isNotFullyPushedDown(ProjectNode.class);
            assertThat(query("SELECT id FROM " + table.getName() + " ORDER BY -dot_product(v, ARRAY[DOUBLE '-1.7976931348623157E+308']) LIMIT 1"))
                    .isNotFullyPushedDown(ProjectNode.class);
            assertThat(query("SELECT id FROM " + table.getName() + " ORDER BY -dot_product(v, ARRAY[REAL 'Infinity']) LIMIT 1"))
                    .isNotFullyPushedDown(ProjectNode.class);
            assertThat(query("SELECT id FROM " + table.getName() + " ORDER BY -dot_product(v, ARRAY[REAL '-Infinity']) LIMIT 1"))
                    .isNotFullyPushedDown(ProjectNode.class);
            assertThat(query("SELECT id FROM " + table.getName() + " ORDER BY -dot_product(v, ARRAY[REAL 'NaN']) LIMIT 1"))
                    .isNotFullyPushedDown(ProjectNode.class);
            assertThat(query("SELECT id FROM " + table.getName() + " ORDER BY -dot_product(v, ARRAY[CAST(NULL AS REAL)]) LIMIT 1"))
                    .isNotFullyPushedDown(ProjectNode.class);
            assertThat(query("SELECT id FROM " + table.getName() + " ORDER BY -dot_product(v, NULL) LIMIT 1"))
                    .isNotFullyPushedDown(ProjectNode.class);
        }
    }

    @Test
    void testCosineDistanceCompatibility()
    {
        // Verify compatibility between Trino cosine_distance function and pgvector <=> operator
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_vector", "(id int, v vector(3))");
                TestView view = new TestView(postgreSqlServer::execute, "test_cosine_distance", "SELECT v <=> '[7,8,9]' FROM " + table.getName())) {
            postgreSqlServer.execute("INSERT INTO " + table.getName() + " VALUES (1, '[1,2,3]'), (2, '[4,5,6]')");

            assertThat(query("SELECT cosine_distance(v, ARRAY[7,8,9]) FROM " + table.getName()))
                    .matches("SELECT * FROM tpch." + view.getName())
                    .isFullyPushedDown();

            assertThat(query("SELECT id FROM " + table.getName() + " ORDER BY cosine_distance(v, ARRAY[4,5,6]) LIMIT 1"))
                    .isFullyPushedDown();

            assertThatThrownBy(() -> postgreSqlServer.execute("SELECT v <=> '[NaN]' FROM " + table.getName()))
                    .hasMessageContaining("NaN not allowed in vector");
            assertThatThrownBy(() -> postgreSqlServer.execute("SELECT v <=> '[-Infinity]' FROM " + table.getName()))
                    .hasMessageContaining("infinite value not allowed in vector");
            assertThatThrownBy(() -> postgreSqlServer.execute("SELECT v <=> '[+Infinity]' FROM " + table.getName()))
                    .hasMessageContaining("infinite value not allowed in vector");
        }
    }

    @Test
    void testCosineDistanceUnsupportedPushdown()
    {
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_vector", "(id int, v vector(1))")) {
            postgreSqlServer.execute("INSERT INTO " + table.getName() + " VALUES (1, '[10]'), (2, '[20]')");

            // The connector doesn't support predicate pushdown with cosine_distance function
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE cosine_distance(v, ARRAY[1]) < 1"))
                    .isNotFullyPushedDown(FilterNode.class);

            // The connector doesn't pushdown these values because pgvector throws an exception
            assertThat(query("SELECT id FROM " + table.getName() + " ORDER BY cosine_distance(v, ARRAY[DOUBLE '1.7976931348623157E+309']) LIMIT 1"))
                    .isNotFullyPushedDown(ProjectNode.class);
            assertThat(query("SELECT id FROM " + table.getName() + " ORDER BY cosine_distance(v, ARRAY[DOUBLE '-1.7976931348623157E+308']) LIMIT 1"))
                    .isNotFullyPushedDown(ProjectNode.class);
            assertThat(query("SELECT id FROM " + table.getName() + " ORDER BY cosine_distance(v, ARRAY[REAL 'Infinity']) LIMIT 1"))
                    .isNotFullyPushedDown(ProjectNode.class);
            assertThat(query("SELECT id FROM " + table.getName() + " ORDER BY cosine_distance(v, ARRAY[REAL '-Infinity']) LIMIT 1"))
                    .isNotFullyPushedDown(ProjectNode.class);
            assertThat(query("SELECT id FROM " + table.getName() + " ORDER BY cosine_distance(v, ARRAY[REAL 'NaN']) LIMIT 1"))
                    .isNotFullyPushedDown(ProjectNode.class);
            assertQueryFails("SELECT id FROM " + table.getName() + " ORDER BY cosine_distance(v, ARRAY[CAST(NULL AS REAL)]) LIMIT 1",
                    "Vector magnitude cannot be zero");
            assertThat(query("SELECT id FROM " + table.getName() + " ORDER BY cosine_distance(v, NULL) LIMIT 1"))
                    .isNotFullyPushedDown(ProjectNode.class);
        }
    }

    @Test
    void testPgVectorUnsupportedCosineDistance()
    {
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_vector", "(v vector(1))")) {
            assertThatThrownBy(() -> postgreSqlServer.execute("SELECT v <=> '[1.7976931348623157E+309]' FROM " + table.getName()))
                    .hasMessageContaining("out of range for type vector");
            assertThatThrownBy(() -> postgreSqlServer.execute("SELECT v <=> '[-1.7976931348623157E+308]' FROM " + table.getName()))
                    .hasMessageContaining("out of range for type vector");
            assertThatThrownBy(() -> postgreSqlServer.execute("SELECT v <=> '[NaN]' FROM " + table.getName()))
                    .hasMessageContaining("NaN not allowed in vector");
            assertThatThrownBy(() -> postgreSqlServer.execute("SELECT v <=> '[-Infinity]' FROM " + table.getName()))
                    .hasMessageContaining("infinite value not allowed in vector");
            assertThatThrownBy(() -> postgreSqlServer.execute("SELECT v <=> '[+Infinity]' FROM " + table.getName()))
                    .hasMessageContaining("infinite value not allowed in vector");
            assertThatThrownBy(() -> postgreSqlServer.execute("SELECT v <=> '[NULL]' FROM " + table.getName()))
                    .hasMessageContaining("invalid input");
        }
    }

    @RepeatedTest(10) // Regression test for https://github.com/trinodb/trino/issues/23152
    void testDuplicateColumnWithUnion()
    {
        try (TestTable table = new TestTable(postgreSqlServer::execute, "test_union", "(id int, v vector(3))")) {
            postgreSqlServer.execute("INSERT INTO " + table.getName() + " VALUES (1, '[1,2,3]'), (2, '[4,5,6]')");

            assertThat(query("" +
                    "SELECT id FROM " + table.getName() +
                    " UNION ALL " +
                    "(SELECT id FROM " + table.getName() + " ORDER BY cosine_distance(v, ARRAY[4,5,6]) LIMIT 1)"))
                    .matches("VALUES 1, 2, 2");
        }
    }
}
