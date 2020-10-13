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
package io.prestosql.tests;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.plugin.jdbc.JdbcPlugin;
import io.prestosql.plugin.jdbc.TestingH2JdbcModule;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static io.prestosql.plugin.jdbc.JdbcMetadataSessionProperties.AGGREGATION_PUSHDOWN_ENABLED;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.testing.QueryAssertions.copyTpchTables;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test {@link io.prestosql.sql.query.QueryAssertions}.
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

        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), List.of(NATION));

        Map<String, String> jdbcWithAggregationPushdownDisabledConfigurationProperties = ImmutableMap.<String, String>builder()
                .putAll(jdbcConfigurationProperties)
                .put("allow-aggregation-pushdown", "false")
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
