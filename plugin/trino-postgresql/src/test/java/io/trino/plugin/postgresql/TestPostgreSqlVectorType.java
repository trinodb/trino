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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TestView;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.IntStream;

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
        return PostgreSqlQueryRunner.builder(postgreSqlServer).build();
    }

    @BeforeAll
    void setExtensions()
    {
        onRemoteDatabase().execute("CREATE EXTENSION vector");
    }

    @Test
    void testVector()
    {
        try (TestTable table = new TestTable(onRemoteDatabase(), "test_vector", "(v vector(1))")) {
            onRemoteDatabase().execute("INSERT INTO " + table.getName() + " VALUES ('[1]')");
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES ARRAY[REAL '1.0']");
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE v = ARRAY[REAL '1.0']"))
                    .matches("VALUES ARRAY[REAL '1.0']");
            assertQueryReturnsEmptyResult("SELECT * FROM " + table.getName() + " WHERE v = ARRAY[REAL '2.0']");

            assertThatThrownBy(() -> onRemoteDatabase().execute("INSERT INTO " + table.getName() + " VALUES ('[NaN]')"))
                    .hasMessageContaining("NaN not allowed in vector");
            assertThatThrownBy(() -> onRemoteDatabase().execute("INSERT INTO " + table.getName() + " VALUES ('[-Infinity]')"))
                    .hasMessageContaining("infinite value not allowed in vector");
            assertThatThrownBy(() -> onRemoteDatabase().execute("INSERT INTO " + table.getName() + " VALUES ('[+Infinity]')"))
                    .hasMessageContaining("infinite value not allowed in vector");
        }

        try (TestTable table = new TestTable(onRemoteDatabase(), "test_vector", "(v vector(2))")) {
            onRemoteDatabase().execute("INSERT INTO " + table.getName() + " VALUES ('[1.1,2.2]')");
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES ARRAY[REAL '1.1', REAL '2.2']");
        }

        try (TestTable table = new TestTable(onRemoteDatabase(), "test_vector", "(v vector(3))")) {
            onRemoteDatabase().execute("INSERT INTO " + table.getName() + " VALUES ('[1.11,2.22,3.33]')");
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES ARRAY[REAL '1.11', REAL '2.22', REAL '3.33']");
        }
    }

    @Test
    void testVectorWrite()
    {
        try (TestTable table = new TestTable(onRemoteDatabase(), "test_vector_writes", "(v vector(1))")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES ARRAY[REAL '1.0'], ARRAY[REAL '3.4028235e+38f'], ARRAY[REAL '1.4E-45'], NULL", 4);
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("VALUES ARRAY[REAL '1.0'], ARRAY[REAL '3.4028235e+38f'], ARRAY[REAL '1.4E-45'], NULL");

            assertQueryFails("INSERT INTO " + table.getName() + " VALUES ARRAY[REAL '1.0', REAL '2.0']", ".*expected 1 dimensions, not 2.*");
            assertQueryFails("INSERT INTO " + table.getName() + " VALUES ARRAY[nan()]", "(?s).*NaN not allowed in vector.*");
            assertQueryFails("INSERT INTO " + table.getName() + " VALUES ARRAY[-infinity()]", "(?s).*infinite value not allowed in vector.*");
            assertQueryFails("INSERT INTO " + table.getName() + " VALUES ARRAY[+infinity()]", "(?s).*infinite value not allowed in vector.*");
        }
    }

    @Test
    void testVectorNull()
    {
        try (TestTable table = new TestTable(onRemoteDatabase(), "test_vector_null", "(id int, v vector(1))")) {
            onRemoteDatabase().execute("INSERT INTO " + table.getName() + " VALUES (1, NULL), (2, '[1]')");
            assertThat(query("SELECT v FROM " + table.getName()))
                    .matches("VALUES CAST(NULL AS ARRAY(REAL)), ARRAY[REAL '1.0']");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE v IS NULL"))
                    .matches("VALUES 1");
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE v IS NOT NULL"))
                    .matches("VALUES 2");

            assertThatThrownBy(() -> onRemoteDatabase().execute("INSERT INTO " + table.getName() + " VALUES (3, '[NULL]')"))
                    .hasMessageContaining("ERROR: invalid input syntax for type vector");
        }
    }

    @Test
    void testVectorArbitraryDimension()
    {
        try (TestTable table = new TestTable(onRemoteDatabase(), "test_vector_arbitrary", "(id int, v vector)")) {
            onRemoteDatabase().execute("INSERT INTO " + table.getName() + " VALUES (1, '[1]'), (2, '[1,2]'), (3, '[1,2,3]')");
            assertThat(query("SELECT v FROM " + table.getName()))
                    .matches("VALUES ARRAY[REAL '1.0'], ARRAY[REAL '1.0', REAL '2.0'], ARRAY[REAL '1.0', REAL '2.0', REAL '3.0']");

            onRemoteDatabase().execute("SELECT v <-> '[4,5,6]' FROM " + table.getName() + " WHERE id = 3");

            assertThatThrownBy(() -> onRemoteDatabase().execute("SELECT v <-> '[4,5,6]' FROM " + table.getName()))
                    .hasMessageContaining("different vector dimensions");
            assertThatThrownBy(() -> onRemoteDatabase().execute("INSERT INTO " + table.getName() + " VALUES (4, '[]')"))
                    .hasMessageContaining("vector must have at least 1 dimension");
        }
    }

    @Test
    void testVectorMaxDimension()
    {
        try (TestTable table = new TestTable(onRemoteDatabase(), "test_vector_max", "(v vector(16000))")) {
            String postgresValue = IntStream.rangeClosed(1, 16000).mapToObj(String::valueOf).collect(joining(","));
            onRemoteDatabase().execute("INSERT INTO " + table.getName() + " VALUES ('[" + postgresValue + "]')");

            String trinoValue = IntStream.rangeClosed(1, 16000).mapToObj("REAL '%s'"::formatted).collect(joining(","));
            assertThat(query("SELECT v FROM " + table.getName()))
                    .matches("VALUES ARRAY[" + trinoValue + "]");
        }
    }

    @Test
    void testVectorUnsupportedDimension()
    {
        assertThatThrownBy(() -> onRemoteDatabase().execute("CREATE TABLE test_vector_unsupported (v vector(-1))"))
                .hasMessageContaining("dimensions for type vector must be at least 1");
        assertThatThrownBy(() -> onRemoteDatabase().execute("CREATE TABLE test_vector_unsupported (v vector(0))"))
                .hasMessageContaining("dimensions for type vector must be at least 1");
        assertThatThrownBy(() -> onRemoteDatabase().execute("CREATE TABLE test_vector_unsupported (v vector(16001))"))
                .hasMessageContaining("dimensions for type vector cannot exceed 16000");
    }

    @Test
    void testEuclideanDistanceCompatibility()
    {
        // Verify compatibility between Trino euclidean_distance function and pgvector <-> operator
        try (TestTable table = new TestTable(onRemoteDatabase(), "test_vector", "(v vector(3))");
                TestView view = new TestView(onRemoteDatabase(), "test_euclidean_distance", "SELECT v <-> '[4,5,6]' FROM " + table.getName())) {
            onRemoteDatabase().execute("INSERT INTO " + table.getName() + " VALUES ('[1,2,3]')");

            assertThat(query("SELECT euclidean_distance(v, ARRAY[4,5,6]) FROM " + table.getName()))
                    .matches("SELECT * FROM tpch." + view.getName());

            assertThatThrownBy(() -> onRemoteDatabase().execute("SELECT v <-> '[NaN]' FROM " + table.getName()))
                    .hasMessageContaining("NaN not allowed in vector");
            assertThatThrownBy(() -> onRemoteDatabase().execute("SELECT v <-> '[-Infinity]' FROM " + table.getName()))
                    .hasMessageContaining("infinite value not allowed in vector");
            assertThatThrownBy(() -> onRemoteDatabase().execute("SELECT v <-> '[+Infinity]' FROM " + table.getName()))
                    .hasMessageContaining("infinite value not allowed in vector");
        }
    }

    @Test
    void testInnerProductCompatibility()
    {
        // Verify compatibility between Trino inner_product function and pgvector <#> operator
        try (TestTable table = new TestTable(onRemoteDatabase(), "test_vector", "(v vector(3))");
                TestView view = new TestView(onRemoteDatabase(), "test_inner_product", "SELECT v <#> '[4,5,6]' FROM " + table.getName())) {
            onRemoteDatabase().execute("INSERT INTO " + table.getName() + " VALUES ('[1,2,3]')");

            assertThat(query("SELECT inner_product(v, ARRAY[4,5,6]) FROM " + table.getName()))
                    .matches("SELECT * FROM tpch." + view.getName());

            assertThatThrownBy(() -> onRemoteDatabase().execute("SELECT v <#> '[NaN]' FROM " + table.getName()))
                    .hasMessageContaining("NaN not allowed in vector");
            assertThatThrownBy(() -> onRemoteDatabase().execute("SELECT v <#> '[-Infinity]' FROM " + table.getName()))
                    .hasMessageContaining("infinite value not allowed in vector");
            assertThatThrownBy(() -> onRemoteDatabase().execute("SELECT v <#> '[+Infinity]' FROM " + table.getName()))
                    .hasMessageContaining("infinite value not allowed in vector");
        }
    }

    @Test
    void testCosineDistanceCompatibility()
    {
        // Verify compatibility between Trino cosine_distance function and pgvector <=> operator
        try (TestTable table = new TestTable(onRemoteDatabase(), "test_vector", "(v vector(3))");
                TestView view = new TestView(onRemoteDatabase(), "test_cosine_distance", "SELECT v <=> '[4,5,6]' FROM " + table.getName())) {
            onRemoteDatabase().execute("INSERT INTO " + table.getName() + " VALUES ('[1,2,3]')");

            assertThat(query("SELECT cosine_distance(v, ARRAY[4,5,6]) FROM " + table.getName()))
                    .matches("SELECT * FROM tpch." + view.getName());

            assertThatThrownBy(() -> onRemoteDatabase().execute("SELECT v <=> '[NaN]' FROM " + table.getName()))
                    .hasMessageContaining("NaN not allowed in vector");
            assertThatThrownBy(() -> onRemoteDatabase().execute("SELECT v <=> '[-Infinity]' FROM " + table.getName()))
                    .hasMessageContaining("infinite value not allowed in vector");
            assertThatThrownBy(() -> onRemoteDatabase().execute("SELECT v <=> '[+Infinity]' FROM " + table.getName()))
                    .hasMessageContaining("infinite value not allowed in vector");
        }
    }

    @Test
    void testInvalidFunctionArguments()
    {
        assertQueryFails("SELECT euclidean_distance(ARRAY['1'], ARRAY['1'])", ".*Unexpected parameters.*");
        assertQueryFails("SELECT inner_product(ARRAY['1'], ARRAY['1'])", ".*Unexpected parameters.*");
        assertQueryFails("SELECT cosine_distance(ARRAY['1'], ARRAY['1'])", ".*Unexpected parameters.*");

        assertQueryFails("SELECT euclidean_distance()", ".*Unexpected parameters.*");
        assertQueryFails("SELECT inner_product()", ".*Unexpected parameters.*");
        assertQueryFails("SELECT cosine_distance()", ".*Unexpected parameters.*");

        assertQueryFails("SELECT euclidean_distance(ARRAY[1])", ".*Unexpected parameters.*");
        assertQueryFails("SELECT inner_product(ARRAY[1])", ".*Unexpected parameters.*");
        assertQueryFails("SELECT cosine_distance(ARRAY[1])", ".*Unexpected parameters.*");

        assertQueryFails("SELECT euclidean_distance(ARRAY[1], ARRAY[1], ARRAY[1])", ".*Unexpected parameters.*");
        assertQueryFails("SELECT inner_product(ARRAY[1], ARRAY[1], ARRAY[1])", ".*Unexpected parameters.*");
        assertQueryFails("SELECT cosine_distance(ARRAY[1], ARRAY[1], ARRAY[1])", ".*Unexpected parameters.*");
    }

    private SqlExecutor onRemoteDatabase()
    {
        return sql -> {
            try {
                try (Connection connection = DriverManager.getConnection(postgreSqlServer.getJdbcUrl(), postgreSqlServer.getProperties());
                        Statement statement = connection.createStatement()) {
                    statement.execute(sql);
                }
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }
}
