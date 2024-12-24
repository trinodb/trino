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
package io.trino.plugin.vertica;

import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.plugin.jdbc.JoinOperator;
import io.trino.spi.connector.JoinCondition;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.toOptional;
import static io.trino.plugin.jdbc.JoinOperator.FULL_JOIN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_AGGREGATION_PUSHDOWN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_INEQUALITY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_LIMIT_PUSHDOWN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_TOPN_PUSHDOWN;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

public class TestVerticaConnectorTest
        extends BaseJdbcConnectorTest
{
    protected TestingVerticaServer verticaServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        verticaServer = closeAfterClass(new TestingVerticaServer());
        return VerticaQueryRunner.builder(verticaServer).setTables(REQUIRED_TPCH_TABLES).build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_JOIN_PUSHDOWN -> true;
            case SUPPORTS_ARRAY,
                 SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                 SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                 SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT,
                 SUPPORTS_DROP_COLUMN,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_RENAME_SCHEMA,
                 SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_TOPN_PUSHDOWN -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    // Overridden due to test case with a push down on a DOUBLE type
    // DOUBLE pushdown is disabled in Vertica due to precision issues
    @Test
    @Override
    public void testJoinPushdown()
    {
        for (JoinOperator joinOperator : JoinOperator.values()) {
            testJoinPushdown(joinOperator);
        }
    }

    private void testJoinPushdown(JoinOperator joinOperator)
    {
        Session session = joinPushdownEnabled(getSession());

        if (!hasBehavior(SUPPORTS_JOIN_PUSHDOWN)) {
            assertThat(query(session, "SELECT r.name, n.name FROM nation n JOIN region r ON n.regionkey = r.regionkey"))
                    .joinIsNotFullyPushedDown();
            return;
        }

        if (joinOperator == FULL_JOIN && !hasBehavior(SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN)) {
            // Covered by verifySupportsJoinPushdownWithFullJoinDeclaration
            return;
        }

        // Disable DF here for the sake of negative test cases' expected plan. With DF enabled, some operators return in DF's FilterNode and some do not.
        Session withoutDynamicFiltering = Session.builder(session)
                .setSystemProperty("enable_dynamic_filtering", "false")
                .build();

        String notDistinctOperator = "IS NOT DISTINCT FROM";
        List<String> nonEqualities = Stream.concat(
                        Stream.of(JoinCondition.Operator.values())
                                .filter(operator -> operator != JoinCondition.Operator.EQUAL && operator != JoinCondition.Operator.IDENTICAL)
                                .map(JoinCondition.Operator::getValue),
                        Stream.of(notDistinctOperator))
                .collect(toImmutableList());

        try (TestTable nationLowercaseTable = newTrinoTable(
                // If a connector supports Join pushdown, but does not allow CTAS, we need to make the table creation here overridable.
                "nation_lowercase",
                "AS SELECT nationkey, lower(name) name, regionkey FROM nation")) {
            // basic case
            assertThat(query(session, format("SELECT r.name, n.name FROM nation n %s region r ON n.regionkey = r.regionkey", joinOperator))).isFullyPushedDown();

            // join over different columns
            assertThat(query(session, format("SELECT r.name, n.name FROM nation n %s region r ON n.nationkey = r.regionkey", joinOperator))).isFullyPushedDown();

            // pushdown when using USING
            assertThat(query(session, format("SELECT r.name, n.name FROM nation n %s region r USING(regionkey)", joinOperator))).isFullyPushedDown();

            // varchar equality predicate
            assertJoinConditionallyPushedDown(
                    session,
                    format("SELECT n.name, n2.regionkey FROM nation n %s nation n2 ON n.name = n2.name", joinOperator),
                    hasBehavior(SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY));
            assertJoinConditionallyPushedDown(
                    session,
                    format("SELECT n.name, nl.regionkey FROM nation n %s %s nl ON n.name = nl.name", joinOperator, nationLowercaseTable.getName()),
                    hasBehavior(SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY));

            // multiple bigint predicates
            assertThat(query(session, format("SELECT n.name, c.name FROM nation n %s customer c ON n.nationkey = c.nationkey and n.regionkey = c.custkey", joinOperator)))
                    .isFullyPushedDown();

            // inequality
            for (String operator : nonEqualities) {
                // bigint inequality predicate
                assertJoinConditionallyPushedDown(
                        withoutDynamicFiltering,
                        format("SELECT r.name, n.name FROM nation n %s region r ON n.regionkey %s r.regionkey", joinOperator, operator),
                        expectJoinPushdown(operator) && expectJoinPushdownOnInequalityOperator(joinOperator));

                // varchar inequality predicate
                assertJoinConditionallyPushedDown(
                        withoutDynamicFiltering,
                        format("SELECT n.name, nl.name FROM nation n %s %s nl ON n.name %s nl.name", joinOperator, nationLowercaseTable.getName(), operator),
                        expectVarcharJoinPushdown(operator) && expectJoinPushdownOnInequalityOperator(joinOperator));
            }

            // inequality along with an equality, which constitutes an equi-condition and allows filter to remain as part of the Join
            for (String operator : nonEqualities) {
                assertJoinConditionallyPushedDown(
                        session,
                        format("SELECT n.name, c.name FROM nation n %s customer c ON n.nationkey = c.nationkey AND n.regionkey %s c.custkey", joinOperator, operator),
                        expectJoinPushdown(operator));
            }

            // varchar inequality along with an equality, which constitutes an equi-condition and allows filter to remain as part of the Join
            for (String operator : nonEqualities) {
                assertJoinConditionallyPushedDown(
                        session,
                        format("SELECT n.name, nl.name FROM nation n %s %s nl ON n.regionkey = nl.regionkey AND n.name %s nl.name", joinOperator, nationLowercaseTable.getName(), operator),
                        expectVarcharJoinPushdown(operator));
            }

            // Join over a (double) predicate
            /*
            assertThat(query(session, format("" +
                    "SELECT c.name, n.name " +
                    "FROM (SELECT * FROM customer WHERE acctbal > 8000) c " +
                    "%s nation n ON c.custkey = n.nationkey", joinOperator)))
                    .isFullyPushedDown();
            */

            // Join over a varchar equality predicate
            assertJoinConditionallyPushedDown(
                    session,
                    format("SELECT c.name, n.name FROM (SELECT * FROM customer WHERE address = 'TcGe5gaZNgVePxU5kRrvXBfkasDTea') c " +
                            "%s nation n ON c.custkey = n.nationkey", joinOperator),
                    hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY));

            // Join over a varchar inequality predicate
            assertJoinConditionallyPushedDown(
                    session,
                    format("SELECT c.name, n.name FROM (SELECT * FROM customer WHERE address < 'TcGe5gaZNgVePxU5kRrvXBfkasDTea') c " +
                            "%s nation n ON c.custkey = n.nationkey", joinOperator),
                    hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY));

            // join over aggregation
            assertJoinConditionallyPushedDown(
                    session,
                    format("SELECT * FROM (SELECT regionkey rk, count(nationkey) c FROM nation GROUP BY regionkey) n " +
                            "%s region r ON n.rk = r.regionkey", joinOperator),
                    hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN));

            // join over LIMIT
            assertJoinConditionallyPushedDown(
                    session,
                    format("SELECT * FROM (SELECT nationkey FROM nation LIMIT 30) n " +
                            "%s region r ON n.nationkey = r.regionkey", joinOperator),
                    hasBehavior(SUPPORTS_LIMIT_PUSHDOWN));

            // join over TopN
            assertJoinConditionallyPushedDown(
                    session,
                    format("SELECT * FROM (SELECT nationkey FROM nation ORDER BY regionkey LIMIT 5) n " +
                            "%s region r ON n.nationkey = r.regionkey", joinOperator),
                    hasBehavior(SUPPORTS_TOPN_PUSHDOWN));

            // join over join
            assertThat(query(session, "SELECT * FROM nation n, region r, customer c WHERE n.regionkey = r.regionkey AND r.regionkey = c.custkey"))
                    .isFullyPushedDown();
        }
    }

    @Override
    protected boolean expectJoinPushdown(String operator)
    {
        if ("IS NOT DISTINCT FROM".equals(operator)) {
            // TODO (https://github.com/trinodb/trino/issues/6967) support join pushdown for IS NOT DISTINCT FROM
            return false;
        }
        return switch (toJoinConditionOperator(operator)) {
            case EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL -> true;
            case IDENTICAL -> hasBehavior(SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM);
        };
    }

    private boolean expectVarcharJoinPushdown(String operator)
    {
        if ("IS NOT DISTINCT FROM".equals(operator)) {
            // TODO (https://github.com/trinodb/trino/issues/6967) support join pushdown for IS NOT DISTINCT FROM
            return false;
        }
        return switch (toJoinConditionOperator(operator)) {
            case EQUAL, NOT_EQUAL -> hasBehavior(SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY);
            case LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL -> hasBehavior(SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_INEQUALITY);
            case IDENTICAL -> hasBehavior(SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM) && hasBehavior(SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY);
        };
    }

    private JoinCondition.Operator toJoinConditionOperator(String operator)
    {
        return Stream.of(JoinCondition.Operator.values())
                .filter(joinOperator -> joinOperator.getValue().equals(operator))
                .collect(toOptional())
                .orElseThrow(() -> new IllegalArgumentException("Not found: " + operator));
    }

    @Test
    @Override
    public void testCreateTableAsSelectWithUnicode()
    {
        // Trino creates a table of varchar(1), This unicode occupies 3 chars in Vertica
        assertThatThrownBy(super::testCreateTableAsSelectWithUnicode)
                .hasStackTraceContaining("ERROR: String of 3 octets is too long for type Varchar(1) for column unicode");
        // Explicit CAST to VARCHAR(3) to make sure the value fits in Vertica's column
        assertCreateTableAsSelect(
                "SELECT CAST('\u2603' AS VARCHAR(3)) unicode",
                "SELECT 1");
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                this::execute,
                "tpch.table",
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Override
    protected MaterializedResult getDescribeOrdersResult()
    {
        // Vertica INTEGER type is a 64-bit type
        // JDBC INTEGER is not supported, only BIGINT
        // Overridden because shippriority's column type is BIGINT from Vertica
        return resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "bigint", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        // Vertica INTEGER type is a 64-bit type
        // JDBC INTEGER is not supported, only BIGINT
        // Overridden because shippriority's column type is BIGINT from Vertica
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("""
                        CREATE TABLE vertica.tpch.orders (
                           orderkey bigint,
                           custkey bigint,
                           orderstatus varchar(1),
                           totalprice double,
                           orderdate date,
                           orderpriority varchar(15),
                           clerk varchar(15),
                           shippriority bigint,
                           comment varchar(79)
                        )""");
    }

    @Test
    @Override
    public void testShowColumns()
    {
        // Vertica INTEGER type is a 64-bit type
        // JDBC INTEGER is not supported, only BIGINT
        // Overridden because shippriority's column type is BIGINT from Vertica
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");
        MaterializedResult expected = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "bigint", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    @Test
    @Override
    public void testDeleteWithLike()
    {
        assertThatThrownBy(super::testDeleteWithLike)
                .hasStackTraceContaining("TrinoException: This connector does not support modifying table rows");
    }

    @Test
    @Override
    public void testInsertIntoNotNullColumn()
    {
        abort("TODO Enable this test");
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(128);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("Maximum limit is 128 octets");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(128);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("Maximum limit is 128 octets");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("date")) {
            // Vertica adds 10 days during julian-gregorian switch
            if (dataMappingTestSetup.getSampleValueLiteral().equals("DATE '1582-10-05'")) {
                return Optional.empty();
            }
            return Optional.of(dataMappingTestSetup);
        }

        if (typeName.equals("time")
                || typeName.equals("time(6)")
                || typeName.equals("timestamp")
                || typeName.equals("timestamp(6)")
                || typeName.equals("timestamp(3) with time zone")
                || typeName.equals("timestamp(6) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        if (typeName.equals("real")
                || typeName.equals("double")) {
            // Vertica is not returning a value as precise as the inputs which causes the smoke tests to fail for these types
            // e.g. REAL '999999.999' is written but 1000000 is read
            // e.g. DOUBLE '1234567890123.123' is written but 1234567890123.12 is read
            return Optional.empty();
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return format(".*Cannot set a NOT NULL column \\(%s\\) to a NULL value in INSERT/UPDATE statement", columnName);
    }

    @Override
    protected void verifyAddNotNullColumnToNonEmptyTableFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching(
                "\\[Vertica]\\[VJDBC]\\(2505\\) ROLLBACK: Cannot set column \"b_varchar\" in table " +
                        "\"tpch.test_add_nn.*\" to NOT NULL since it contains null values");
    }

    @Override
    protected OptionalInt maxColumnNameLength()
    {
        return OptionalInt.of(128);
    }

    @Override
    protected void verifyColumnNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("Maximum limit is 128 octets");
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return verticaServer.getSqlExecutor();
    }

    private void execute(String sql)
    {
        verticaServer.execute(sql);
    }
}
