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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.jdbc.JdbcPlugin;
import io.trino.plugin.jdbc.TestingH2JdbcModule;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.query.QueryAssertions.QueryAssert;
import io.trino.testing.AbstractTestQueryFramework;
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
public abstract class AbstractQueryAssertionsTest
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
                .build();
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
                .hasMessageContaining("[Output types] expected:<[var[char(6)]]> but was:<[var[binary]]>");
    }

    @Test
    public void testTolerance()
    {
        // allow tolerance of +/- 50%, test against both ranges
        assertThat(query("VALUES (70),(150)")).ordered().withTolerancePercentages(ImmutableList.of(0.5)).matches("VALUES (100),(100)");

        //allow tolerance of +/- 10% for the first column and third column, rest exact matches
        assertThat(query("VALUES (91,100,109,100),(95,100,103,100)"))
                .ordered()
                .withTolerancePercentages(ImmutableList.of(0.1d, 1d, 0.1d))
                .matches("VALUES (100,100,100,100),(100,100,100,100)");

        //last exact match is mismatch
        final QueryAssert queryAssertMismatch = assertThat(query("VALUES (91,100,109,100),(95,100,103,100)"))
                .ordered()
                .withTolerancePercentages(ImmutableList.of(0.1d, 1d, 0.1d));
        assertThatThrownBy(() -> queryAssertMismatch.matches("VALUES (91,100,109,100),(95,100,103,101)"))
                .hasMessageContaining("expected [101] but found [100]");

        // value is outside of tolerance
        QueryAssert queryAssert = assertThat(query("VALUES (49)")).ordered().withTolerancePercentages(ImmutableList.of(0.5));
        assertThatThrownBy(() -> queryAssert.matches("VALUES (100)"))
                .hasMessageContaining("row [49] has field 49 that is not contained in expected actual range [50.0..150.0]");

        // value is of wrong type
        QueryAssert stringWithToleranceAssert = assertThat(query("VALUES ('str')")).ordered().withTolerancePercentages(ImmutableList.of(0.5));
        assertThatThrownBy(() -> stringWithToleranceAssert.matches("VALUES ('str')"))
                .hasMessageContaining("tolerance is specified for the column but column is not a number and comparable ");

        // expected has less rows
        final QueryAssert queryAssertExpectedHasLessRows = assertThat(query("VALUES(1),(2)")).ordered().withTolerancePercentages(ImmutableList.of(1.0d));
        assertThatThrownBy(() -> queryAssertExpectedHasLessRows.matches("VALUES (1)"))
                .hasMessageContaining("Expected 1 but encountered 2: expected RowCount 1 not equal to 2");

        // expected has more rows
        final QueryAssert queryAssertExpectedHasMoreRows = assertThat(query("VALUES(1)")).ordered().withTolerancePercentages(ImmutableList.of(1.0d));
        assertThatThrownBy(() -> queryAssertExpectedHasMoreRows.matches("VALUES (1),(2)"))
                .hasMessageContaining("Expected 2 but encountered 1: expected RowCount 2 not equal to 1");
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
                                "Output[name]\n")
                .hasMessageContaining(
                        "\n" +
                                "\n" +
                                "] which resolves to [\n" +
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
                                "Output[_col0]\n")
                .hasMessageContaining(
                        "\n" +
                                "\n" +
                                "] which resolves to [\n" +
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
                                "Output[name]\n")
                .hasMessageContaining(
                        "\n" +
                                "\n" +
                                "] which resolves to [\n" +
                                "\n" +
                                "Output[name]\n");
    }
}
