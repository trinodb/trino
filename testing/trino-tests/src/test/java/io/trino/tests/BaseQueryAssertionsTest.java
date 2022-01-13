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
import io.trino.Session;
import io.trino.plugin.jdbc.JdbcPlugin;
import io.trino.plugin.jdbc.TestingH2JdbcModule;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.query.QueryAssertions.QueryAssert;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.AGGREGATION_PUSHDOWN_ENABLED;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test {@link io.trino.sql.query.QueryAssertions}.
 */
public abstract class BaseQueryAssertionsTest
        extends AbstractTestQueryFramework
{
    protected static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("jdbc")
                .setSchema("public")
                .build();
    }

    protected void configureCatalog(QueryRunner queryRunner)
    {
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());

        queryRunner.installPlugin(new JdbcPlugin("base-jdbc", new TestingH2JdbcModule()));
        Map<String, String> jdbcConfigurationProperties = TestingH2JdbcModule.createProperties();
        queryRunner.createCatalog("jdbc", "base-jdbc", jdbcConfigurationProperties);

        try (Connection connection = DriverManager.getConnection(jdbcConfigurationProperties.get("connection-url"));
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + "tpch");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }

        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, queryRunner.getDefaultSession(), List.of(NATION));

        Map<String, String> jdbcWithAggregationPushdownDisabledConfigurationProperties = ImmutableMap.<String, String>builder()
                .putAll(jdbcConfigurationProperties)
                .put("aggregation-pushdown.enabled", "false")
                .buildOrThrow();
        queryRunner.createCatalog("jdbc_with_aggregation_pushdown_disabled", "base-jdbc", jdbcWithAggregationPushdownDisabledConfigurationProperties);
    }

    @Test
    public void testMatches()
    {
        assertThat(query("SELECT name FROM nation WHERE nationkey = 3"))
                .matches("VALUES CAST('CANADA' AS varchar(25))");
    }

    @Test
    public void testWrongType()
    {
        QueryAssert queryAssert = assertThat(query("SELECT X'001234'"));
        assertThatThrownBy(() -> queryAssert.matches("VALUES '001234'"))
                .hasMessageContaining("[Output types for query [SELECT X'001234']] expected:<[var[char(6)]]> but was:<[var[binary]]>");
    }

    @Test
    public void testReturnsEmptyResult()
    {
        assertThat(query("SELECT 'foobar' WHERE false")).returnsEmptyResult();

        QueryAssert queryAssert = assertThat(query("VALUES 'foobar'"));
        assertThatThrownBy(queryAssert::returnsEmptyResult)
                .hasMessage("[Rows for query [VALUES 'foobar']] \nExpecting empty but was:<[[foobar]]>");

        queryAssert = assertThat(query("VALUES 'foo', 'bar'"));
        assertThatThrownBy(queryAssert::returnsEmptyResult)
                .hasMessage("[Rows for query [VALUES 'foo', 'bar']] \nExpecting empty but was:<[[foo], [bar]]>");
    }

    @Test
    public void testVarbinaryResult()
    {
        assertThat(query("SELECT X'001234'")).matches("VALUES X'001234'");

        QueryAssert queryAssert = assertThat(query("SELECT X'001234'"));
        assertThatThrownBy(() -> queryAssert.matches("VALUES X'001299'"))
                .hasMessageMatching("" +
                        // TODO the representation and thus messages should be the same regardless of query runner in use
                        // when using local query runner
                        "(?s).*" +
                        "(\\Q" +
                        "Expecting:\n" +
                        "  <(00 12 34)>\n" +
                        "to contain exactly in any order:\n" +
                        "  <[(00 12 99)]>\n" +
                        "elements not found:\n" +
                        "  <(00 12 99)>" +
                        "\\E|\\Q" +
                        // when using distributed query runner
                        "Expecting:\n" +
                        "  <([0, 18, 52])>\n" +
                        "to contain exactly in any order:\n" +
                        "  <[([0, 18, -103])]>" +
                        "\\E).*");
    }

    @Test
    public void testNestedVarbinaryResult()
    {
        assertThat(query("SELECT CAST(ROW(X'001234') AS ROW(foo varbinary))"))
                .matches("SELECT CAST(ROW(X'001234') AS ROW(foo varbinary))");

        QueryAssert queryAssert = assertThat(query("SELECT CAST(ROW(X'001234') AS ROW(foo varbinary))"));
        assertThatThrownBy(() -> queryAssert.matches("SELECT CAST(ROW(X'001299') AS ROW(foo varbinary))"))
                .hasMessageMatching(
                        // TODO the representation and thus messages should be the same regardless of query runner in use
                        getQueryRunner() instanceof LocalQueryRunner
                                ? "(?s).*" +
                                "\\Q" +
                                "Expecting:\n" +
                                "  <([00 12 34])>\n" +
                                "to contain exactly in any order:\n" +
                                "  <[([00 12 99])]>\n" +
                                "elements not found:\n" +
                                "  <([00 12 99])>\n" +
                                "and elements not expected:\n" +
                                "  <([00 12 34])>" +
                                "\\E.*"
                                : "(?s).*" +
                                "\\Q" +
                                "Expecting:\n" +
                                "  <([X'00 12 34'])>\n" +
                                "to contain exactly in any order:\n" +
                                "  <[([X'00 12 99'])]>\n" +
                                "elements not found:\n" +
                                "  <([X'00 12 99'])>\n" +
                                "and elements not expected:\n" +
                                "  <([X'00 12 34'])>" +
                                "\\E.*");
    }

    /**
     * Tests query runner with results of various precisions, and query assert.
     */
    @Test
    public void testTimeQueryResult()
    {
        assertThat(query("SELECT TIME '01:23:45.123'")).matches("SELECT TIME '01:23:45.123'");
        assertThat(query("SELECT TIME '01:23:45.123456'")).matches("SELECT TIME '01:23:45.123456'");
        assertThat(query("SELECT TIME '01:23:45.123456789'")).matches("SELECT TIME '01:23:45.123456789'");
        assertThat(query("SELECT TIME '01:23:45.123456789012'")).matches("SELECT TIME '01:23:45.123456789012'");

        QueryAssert queryAssert = assertThat(query("SELECT TIME '01:23:45.123456789012'"));
        assertThatThrownBy(() -> queryAssert.matches("SELECT TIME '01:23:45.123456789013'"))
                .hasMessageContaining("Expecting:\n" +
                        "  <(01:23:45.123456789012)>\n" +
                        "to contain exactly in any order:\n" +
                        "  <[(01:23:45.123456789013)]>");
    }

    /**
     * Tests query runner with results of various precisions, and query assert.
     */
    @Test
    public void testTimeWithTimeZoneQueryResult()
    {
        assertThat(query("SELECT TIME '01:23:45.123 +05:07'")).matches("SELECT TIME '01:23:45.123 +05:07'");
        assertThat(query("SELECT TIME '01:23:45.123456 +05:07'")).matches("SELECT TIME '01:23:45.123456 +05:07'");
        assertThat(query("SELECT TIME '01:23:45.123456789 +05:07'")).matches("SELECT TIME '01:23:45.123456789 +05:07'");
        assertThat(query("SELECT TIME '01:23:45.123456789012 +05:07'")).matches("SELECT TIME '01:23:45.123456789012 +05:07'");

        QueryAssert queryAssert = assertThat(query("SELECT TIME '01:23:45.123456789012 +05:07'"));
        // different second fraction
        assertThatThrownBy(() -> queryAssert.matches("SELECT TIME '01:23:45.123456789013 +05:07'"))
                .hasMessageContaining("Expecting:\n" +
                        "  <(01:23:45.123456789012+05:07)>\n" +
                        "to contain exactly in any order:\n" +
                        "  <[(01:23:45.123456789013+05:07)]>");
        // different zone
        assertThatThrownBy(() -> queryAssert.matches("SELECT TIME '01:23:45.123456789012 +05:42'"))
                .hasMessageContaining("Expecting:\n" +
                        "  <(01:23:45.123456789012+05:07)>\n" +
                        "to contain exactly in any order:\n" +
                        "  <[(01:23:45.123456789012+05:42)]>");
    }

    /**
     * Tests query runner with results of various precisions, and query assert.
     */
    @Test
    public void testTimestampQueryResult()
    {
        assertThat(query("SELECT TIMESTAMP '2017-01-02 09:12:34.123'")).matches("SELECT TIMESTAMP '2017-01-02 09:12:34.123'");
        assertThat(query("SELECT TIMESTAMP '2017-01-02 09:12:34.123456'")).matches("SELECT TIMESTAMP '2017-01-02 09:12:34.123456'");
        assertThat(query("SELECT TIMESTAMP '2017-01-02 09:12:34.123456789'")).matches("SELECT TIMESTAMP '2017-01-02 09:12:34.123456789'");
        assertThat(query("SELECT TIMESTAMP '2017-01-02 09:12:34.123456789012'")).matches("SELECT TIMESTAMP '2017-01-02 09:12:34.123456789012'");

        QueryAssert queryAssert = assertThat(query("SELECT TIMESTAMP '2017-01-02 09:12:34.123456789012'"));
        assertThatThrownBy(() -> queryAssert.matches("SELECT TIMESTAMP '2017-01-02 09:12:34.123456789013'"))
                .hasMessageContaining("Expecting:\n" +
                        "  <(2017-01-02 09:12:34.123456789012)>\n" +
                        "to contain exactly in any order:\n" +
                        "  <[(2017-01-02 09:12:34.123456789013)]>");
    }

    /**
     * Tests query runner with results of various precisions, and query assert.
     */
    @Test
    public void testTimestampWithTimeZoneQueryResult()
    {
        assertThat(query("SELECT TIMESTAMP '2017-01-02 09:12:34.123 Europe/Warsaw'")).matches("SELECT TIMESTAMP '2017-01-02 09:12:34.123 Europe/Warsaw'");
        assertThat(query("SELECT TIMESTAMP '2017-01-02 09:12:34.123456 Europe/Warsaw'")).matches("SELECT TIMESTAMP '2017-01-02 09:12:34.123456 Europe/Warsaw'");
        assertThat(query("SELECT TIMESTAMP '2017-01-02 09:12:34.123456789 Europe/Warsaw'")).matches("SELECT TIMESTAMP '2017-01-02 09:12:34.123456789 Europe/Warsaw'");
        assertThat(query("SELECT TIMESTAMP '2017-01-02 09:12:34.123456789012 Europe/Warsaw'")).matches("SELECT TIMESTAMP '2017-01-02 09:12:34.123456789012 Europe/Warsaw'");

        QueryAssert queryAssert = assertThat(query("SELECT TIMESTAMP '2017-01-02 09:12:34.123456789012 Europe/Warsaw'"));
        // different second fraction
        assertThatThrownBy(() -> queryAssert.matches("SELECT TIMESTAMP '2017-01-02 09:12:34.123456789013 Europe/Warsaw'"))
                .hasMessageContaining("Expecting:\n" +
                        "  <(2017-01-02 09:12:34.123456789012 Europe/Warsaw)>\n" +
                        "to contain exactly in any order:\n" +
                        "  <[(2017-01-02 09:12:34.123456789013 Europe/Warsaw)]>");
        // different zone
        assertThatThrownBy(() -> queryAssert.matches("SELECT TIMESTAMP '2017-01-02 09:12:34.123456789012 Europe/Paris'"))
                .hasMessageContaining("Expecting:\n" +
                        "  <(2017-01-02 09:12:34.123456789012 Europe/Warsaw)>\n" +
                        "to contain exactly in any order:\n" +
                        "  <[(2017-01-02 09:12:34.123456789012 Europe/Paris)]>");
    }

    @Test
    public void testIsFullyPushedDown()
    {
        assertThat(query("SELECT name FROM nation")).isFullyPushedDown();

        // Test that, in case of failure, there is no failure when rendering expected and actual plans
        assertThatThrownBy(() -> assertThat(query("SELECT name FROM nation WHERE rand() = 42")).isFullyPushedDown())
                .hasMessageContaining(
                        "Plan does not match, expected [\n" +
                                "\n" +
                                "- node(OutputNode)\n")
                .hasMessageContaining(
                        "\n" +
                                "\n" +
                                "] but found [\n" +
                                "\n" +
                                "Output[name]\n");
    }

    @Test
    public void testIsFullyPushedDownWithSession()
    {
        Session baseSession = Session.builder(getSession())
                .setCatalog("jdbc_with_aggregation_pushdown_disabled")
                .build();

        Session sessionWithAggregationPushdown = Session.builder(baseSession)
                .setCatalogSessionProperty("jdbc_with_aggregation_pushdown_disabled", AGGREGATION_PUSHDOWN_ENABLED, "true")
                .build();

        assertThat(query("SELECT count(*) FROM nation")).isFullyPushedDown();
        assertThat(query(baseSession, "SELECT count(*) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
        assertThat(query(sessionWithAggregationPushdown, "SELECT count(*) FROM nation")).isFullyPushedDown();

        // Test that, in case of failure, there is no failure when rendering expected and actual plans
        assertThatThrownBy(() -> assertThat(query(sessionWithAggregationPushdown, "SELECT count(*) FROM nation WHERE rand() = 42")).isFullyPushedDown())
                .hasMessageContaining(
                        "Plan does not match, expected [\n" +
                                "\n" +
                                "- node(OutputNode)\n")
                .hasMessageContaining(
                        "\n" +
                                "\n" +
                                "] but found [\n" +
                                "\n" +
                                "Output[_col0]\n");
    }

    @Test
    public void testIsNotFullyPushedDown()
    {
        assertThat(query("SELECT name FROM nation WHERE rand() = 42")).isNotFullyPushedDown(FilterNode.class);

        // Test that, in case of failure, there is no failure when rendering expected and actual plans
        assertThatThrownBy(() -> assertThat(query("SELECT name FROM nation")).isNotFullyPushedDown(FilterNode.class))
                .hasMessageContaining(
                        "Plan does not match, expected [\n" +
                                "\n" +
                                "- anyTree\n" +
                                "    - node(FilterNode)\n")
                .hasMessageContaining(
                        "\n" +
                                "\n" +
                                "] but found [\n" +
                                "\n" +
                                "Output[name]\n");
    }

    @Test
    public void testCustomMessages()
    {
        QueryAssert query = assertThat(query("VALUES 1"));

        assertThatThrownBy(() -> query
                .as("Custom message for result mismatch")
                .matches("VALUES 2"))
                .hasMessageStartingWith("[Custom message for result mismatch]");

        assertThatThrownBy(() -> query
                .as("Custom message for plan mismatch")
                .matches(PlanMatchPattern.values("1")))
                .hasMessageStartingWith("Custom message for plan mismatch: Plan does not match");

        assertThatThrownBy(() -> query
                .as("containsAll custom message")
                .containsAll("VALUES 2"))
                .hasMessageStartingWith("[containsAll custom message]");

        assertThatThrownBy(() -> query
                .as("Mismatched type custom message")
                .matches("VALUES 'abc'"))
                .hasMessageStartingWith("[Mismatched type custom message [Output types]]");

        assertThatThrownBy(() -> query
                .as("returnsEmptyResult custom message")
                .returnsEmptyResult())
                .hasMessageStartingWith("[returnsEmptyResult custom message]");
    }
}
