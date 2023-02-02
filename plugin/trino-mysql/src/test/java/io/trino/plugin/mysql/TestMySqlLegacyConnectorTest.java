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
package io.trino.plugin.mysql;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.mysql.MySqlQueryRunner.createMySqlQueryRunner;
import static io.trino.plugin.mysql.TestingMySqlServer.LEGACY_IMAGE;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMySqlLegacyConnectorTest
        extends BaseMySqlConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        mySqlServer = closeAfterClass(new TestingMySqlServer(LEGACY_IMAGE, false));
        return createMySqlQueryRunner(mySqlServer, ImmutableMap.of(), ImmutableMap.of(), REQUIRED_TPCH_TABLES);
    }

    @Test
    @Override
    public void testCreateTableAsSelectWithUnicode()
    {
        assertThatThrownBy(super::testCreateTableAsSelectWithUnicode)
                .hasStackTraceContaining("Failed to insert data: Incorrect string value: '\\xE2\\x98\\x83'");
    }

    @Test
    @Override
    public void testInsertUnicode()
    {
        // Create tables with CHARACTER SET utf8mb4 option in MySQL side to allow inserting unicode values
        try (TestTable table = new TestTable(onRemoteDatabase(), "tpch.test_insert_unicode_", "(test varchar(50) CHARACTER SET utf8mb4)")) {
            assertUpdate("INSERT INTO " + table.getName() + "(test) VALUES 'Hello', U&'hello\\6d4B\\8Bd5world\\7F16\\7801' ", 2);
            assertThat(computeActual("SELECT test FROM " + table.getName()).getOnlyColumnAsSet())
                    .containsExactlyInAnyOrder("Hello", "hello测试world编码");
        }

        try (TestTable table = new TestTable(onRemoteDatabase(), "tpch.test_insert_unicode_", "(test varchar(50) CHARACTER SET utf8mb4)")) {
            assertUpdate("INSERT INTO " + table.getName() + "(test) VALUES 'aa', 'bé'", 2);
            assertQuery("SELECT test FROM " + table.getName(), "VALUES 'aa', 'bé'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test = 'aa'", "VALUES 'aa'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test > 'ba'", "VALUES 'bé'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test < 'ba'", "VALUES 'aa'");
            assertQueryReturnsEmptyResult("SELECT test FROM " + table.getName() + " WHERE test = 'ba'");
        }

        try (TestTable table = new TestTable(onRemoteDatabase(), "tpch.test_insert_unicode_", "(test varchar(50) CHARACTER SET utf8mb4)")) {
            assertUpdate("INSERT INTO " + table.getName() + "(test) VALUES 'a', 'é'", 2);
            assertQuery("SELECT test FROM " + table.getName(), "VALUES 'a', 'é'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test = 'a'", "VALUES 'a'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test > 'b'", "VALUES 'é'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test < 'b'", "VALUES 'a'");
            assertQueryReturnsEmptyResult("SELECT test FROM " + table.getName() + " WHERE test = 'b'");
        }
    }

    @Test
    @Override
    public void testInsertHighestUnicodeCharacter()
    {
        // Create a table with CHARACTER SET utf8mb4 option in MySQL side to allow inserting unicode values
        try (TestTable table = new TestTable(onRemoteDatabase(), "tpch.test_insert_unicode_", "(test varchar(50) CHARACTER SET utf8mb4)")) {
            assertUpdate("INSERT INTO " + table.getName() + "(test) VALUES 'Hello', U&'hello\\6d4B\\8Bd5\\+10FFFFworld\\7F16\\7801' ", 2);
            assertThat(computeActual("SELECT test FROM " + table.getName()).getOnlyColumnAsSet())
                    .containsExactlyInAnyOrder("Hello", "hello测试􏿿world编码");
        }
    }

    @Test
    @Override
    public void testCountDistinctWithStringTypes()
    {
        // Create a table with CHARACTER SET utf8mb4 option in MySQL side to insert unicode values
        List<String> rows = Stream.of("a", "b", "A", "B", " a ", "a", "b", " b ", "ą")
                .map(value -> format("'%1$s', '%1$s'", value))
                .collect(toImmutableList());

        try (TestTable testTable = new TestTable(onRemoteDatabase(), "tpch.distinct_strings", "(t_char CHAR(5) CHARACTER SET utf8mb4, t_varchar VARCHAR(5) CHARACTER SET utf8mb4)", rows)) {
            // disabling hash generation to prevent extra projections in the plan which make it hard to write matchers for isNotFullyPushedDown
            Session optimizeHashGenerationDisabled = Session.builder(getSession())
                    .setSystemProperty("optimize_hash_generation", "false")
                    .build();

            // It is not captured in the `isNotFullyPushedDown` calls (can't do that) but depending on the connector in use some aggregations
            // still can be pushed down to connector.
            // the DISTINCT part of aggregation will still be pushed down to connector as `GROUP BY`. Only the `count` part will remain on the Trino side.
            assertThat(query(optimizeHashGenerationDisabled, "SELECT count(DISTINCT t_varchar) FROM " + testTable.getName()))
                    .matches("VALUES BIGINT '7'")
                    .isNotFullyPushedDown(AggregationNode.class);
            assertThat(query(optimizeHashGenerationDisabled, "SELECT count(DISTINCT t_char) FROM " + testTable.getName()))
                    .matches("VALUES BIGINT '7'")
                    .isNotFullyPushedDown(AggregationNode.class);

            assertThat(query("SELECT count(DISTINCT t_char), count(DISTINCT t_varchar) FROM " + testTable.getName()))
                    .matches("VALUES (BIGINT '7', BIGINT '7')")
                    .isNotFullyPushedDown(MarkDistinctNode.class, ExchangeNode.class, ExchangeNode.class, ProjectNode.class);
        }
    }

    @Test
    @Override
    public void testRenameColumn()
    {
        assertThatThrownBy(super::testRenameColumn)
                .hasMessageContaining("You have an error in your SQL syntax")
                .hasStackTraceContaining("RENAME COLUMN x TO before_y");
    }

    @Override
    public void testRenameColumnName(String columnName)
    {
        assertThatThrownBy(() -> super.testRenameColumnName(columnName))
                .hasMessageContaining("You have an error in your SQL syntax")
                .hasStackTraceContaining("RENAME COLUMN");
    }

    @Override
    public void testAlterTableRenameColumnToLongName()
    {
        assertThatThrownBy(super::testAlterTableRenameColumnToLongName)
                .hasMessageContaining("You have an error in your SQL syntax")
                .hasStackTraceContaining("RENAME COLUMN x");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        // CREATE TABLE AS SELECT unicode values fail and the negative case is covered in testCreateTableAsSelectWithUnicode
        if (typeName.equals("varchar")) {
            return Optional.empty();
        }
        return super.filterDataMappingSmokeTestData(dataMappingTestSetup);
    }

    @Override
    protected void verifyColumnNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("Identifier name .* is too long");
    }

    @Override
    public void testNativeQueryWithClause()
    {
        // Override because MySQL 5.x doesn't support WITH clause
        assertThatThrownBy(super::testNativeQueryWithClause)
                .hasStackTraceContaining("Failed to get table handle for prepared query. You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version");
    }
}
