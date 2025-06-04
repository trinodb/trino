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
import com.google.common.collect.ImmutableSet;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.TestingTableFunctions;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.connector.MockConnector.MockConnectorSplit.MOCK_CONNECTOR_SPLIT;
import static io.trino.connector.TestingTableFunctions.ConstantFunction.getConstantFunctionSplitSource;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTableFunctionInvocation
        extends AbstractTestQueryFramework
{
    private static final String TESTING_CATALOG = "testing_catalog";
    private static final String TABLE_FUNCTION_SCHEMA = "table_function_schema";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog(TESTING_CATALOG)
                        .setSchema(TABLE_FUNCTION_SCHEMA)
                        .build())
                .setAdditionalSetup(queryRunner -> {
                    queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                            .withTableFunctions(ImmutableSet.of(
                                    new TestingTableFunctions.SimpleTableFunctionWithAccessControl(),
                                    new TestingTableFunctions.IdentityFunction(),
                                    new TestingTableFunctions.IdentityPassThroughFunction(),
                                    new TestingTableFunctions.RepeatFunction(),
                                    new TestingTableFunctions.EmptyOutputFunction(),
                                    new TestingTableFunctions.EmptyOutputWithPassThroughFunction(),
                                    new TestingTableFunctions.TestInputsFunction(),
                                    new TestingTableFunctions.PassThroughInputFunction(),
                                    new TestingTableFunctions.TestInputFunction(),
                                    new TestingTableFunctions.TestSingleInputRowSemanticsFunction(),
                                    new TestingTableFunctions.ConstantFunction(),
                                    new TestingTableFunctions.EmptySourceFunction()))
                            .withApplyTableFunction((session, handle) -> {
                                if (handle instanceof TestingTableFunctions.SimpleTableFunction.SimpleTableFunctionHandle functionHandle) {
                                    return Optional.of(new TableFunctionApplicationResult<>(functionHandle.getTableHandle(), functionHandle.getTableHandle().getColumns().orElseThrow()));
                                }
                                return Optional.empty();
                            })
                            .withFunctionProvider(Optional.of(new FunctionProvider()
                            {
                                @Override
                                public TableFunctionProcessorProvider getTableFunctionProcessorProvider(ConnectorTableFunctionHandle functionHandle)
                                {
                                    if (functionHandle instanceof TestingTableFunctions.TestingTableFunctionHandle handle) {
                                        return switch (handle.name().getFunctionName()) {
                                            case "identity_function" -> new TestingTableFunctions.IdentityFunction.IdentityFunctionProcessorProvider();
                                            case "identity_pass_through_function" ->
                                                    new TestingTableFunctions.IdentityPassThroughFunction.IdentityPassThroughFunctionProcessorProvider();
                                            case "empty_output" -> new TestingTableFunctions.EmptyOutputFunction.EmptyOutputProcessorProvider();
                                            case "empty_output_with_pass_through" ->
                                                    new TestingTableFunctions.EmptyOutputWithPassThroughFunction.EmptyOutputWithPassThroughProcessorProvider();
                                            case "test_inputs_function" -> new TestingTableFunctions.TestInputsFunction.TestInputsFunctionProcessorProvider();
                                            case "pass_through" -> new TestingTableFunctions.PassThroughInputFunction.PassThroughInputProcessorProvider();
                                            case "test_input" -> new TestingTableFunctions.TestInputFunction.TestInputProcessorProvider();
                                            case "test_single_input_function" ->
                                                    new TestingTableFunctions.TestSingleInputRowSemanticsFunction.TestSingleInputFunctionProcessorProvider();
                                            case "empty_source" -> new TestingTableFunctions.EmptySourceFunction.EmptySourceFunctionProcessorProvider();
                                            default -> throw new IllegalArgumentException("unexpected table function: " + handle.name());
                                        };
                                    }
                                    if (functionHandle instanceof TestingTableFunctions.RepeatFunction.RepeatFunctionHandle) {
                                        return new TestingTableFunctions.RepeatFunction.RepeatFunctionProcessorProvider();
                                    }
                                    if (functionHandle instanceof TestingTableFunctions.ConstantFunction.ConstantFunctionHandle) {
                                        return new TestingTableFunctions.ConstantFunction.ConstantFunctionProcessorProvider();
                                    }

                                    return null;
                                }
                            }))
                            .withTableFunctionSplitSources(functionHandle -> {
                                if (functionHandle instanceof TestingTableFunctions.ConstantFunction.ConstantFunctionHandle handle) {
                                    return getConstantFunctionSplitSource(handle);
                                }
                                if (functionHandle instanceof TestingTableFunctions.TestingTableFunctionHandle handle && handle.name().equals(new SchemaFunctionName("system", "empty_source"))) {
                                    return new FixedSplitSource(ImmutableList.of(MOCK_CONNECTOR_SPLIT));
                                }

                                return null;
                            })
                            .build()));
                    queryRunner.createCatalog(TESTING_CATALOG, "mock", ImmutableMap.of());

                    queryRunner.installPlugin(new TpchPlugin());
                    queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());
                })
                .build();
    }

    @Test
    public void testPrimitiveDefaultArgument()
    {
        assertThat(query("SELECT boolean_column FROM TABLE(system.simple_table_function(column => 'boolean_column', ignored => 1))"))
                .matches("SELECT true WHERE false");

        // skip the `ignored` argument.
        assertThat(query("SELECT boolean_column FROM TABLE(system.simple_table_function(column => 'boolean_column'))"))
                .matches("SELECT true WHERE false");
    }

    @Test
    public void testAccessControl()
    {
        assertAccessDenied(
                "SELECT boolean_column FROM TABLE(system.simple_table_function(column => 'boolean_column', ignored => 1))",
                "Cannot select from columns .*",
                privilege("simple_table.boolean_column", SELECT_COLUMN));

        assertAccessDenied(
                "SELECT boolean_column FROM TABLE(system.simple_table_function(column => 'boolean_column', ignored => 1))",
                "Cannot select from columns .*",
                privilege("simple_table", SELECT_COLUMN));
    }

    @Test
    public void testNoArgumentsPassed()
    {
        assertThat(query("SELECT col FROM TABLE(system.simple_table_function())"))
                .matches("SELECT true WHERE false");
    }

    @Test
    public void testIdentityFunction()
    {
        assertThat(query("SELECT b, a FROM TABLE(system.identity_function(input => TABLE(VALUES (1, 2), (3, 4), (5, 6)) T(a, b)))"))
                .matches("VALUES (2, 1), (4, 3), (6, 5)");

        assertThat(query("SELECT b, a FROM TABLE(system.identity_pass_through_function(input => TABLE(VALUES (1, 2), (3, 4), (5, 6)) T(a, b)))"))
                .matches("VALUES (2, 1), (4, 3), (6, 5)");

        // null partitioning value
        assertThat(query("SELECT i.b, a FROM TABLE(system.identity_function(input => TABLE(VALUES ('x', 1), ('y', 2), ('z', null)) T(a, b) PARTITION BY b)) i"))
                .matches("VALUES (1, 'x'), (2, 'y'), (null, 'z')");

        assertThat(query("SELECT b, a FROM TABLE(system.identity_pass_through_function(input => TABLE(VALUES ('x', 1), ('y', 2), ('z', null)) T(a, b) PARTITION BY b))"))
                .matches("VALUES (1, 'x'), (2, 'y'), (null, 'z')");

        // the identity_function copies all input columns and outputs them as proper columns.
        // the table tpch.tiny.orders has a hidden column row_number, which is not exposed to the function.
        assertThat(query("SELECT * FROM TABLE(system.identity_function(input => TABLE(tpch.tiny.orders)))"))
                .matches("SELECT * FROM tpch.tiny.orders");

        // the identity_pass_through_function passes all input columns on output using the pass-through mechanism (as opposed to producing proper columns).
        // the table tpch.tiny.orders has a hidden column row_number, which is exposed to the pass-through mechanism.
        // the passed-through column row_number preserves its hidden property.
        assertThat(query("SELECT row_number, * FROM TABLE(system.identity_pass_through_function(input => TABLE(tpch.tiny.orders)))"))
                .matches("SELECT row_number, * FROM tpch.tiny.orders");
    }

    @Test
    public void testRepeatFunction()
    {
        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.repeat(TABLE(VALUES (1, 2), (3, 4), (5, 6))))
                """))
                .matches("VALUES (1, 2), (1, 2), (3, 4), (3, 4), (5, 6), (5, 6)");

        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.repeat(
                                        TABLE(VALUES ('a', true), ('b', false)),
                                        4))
                """))
                .matches("VALUES ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false)");

        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.repeat(
                                        TABLE(VALUES ('a', true), ('b', false)) t(x, y) PARTITION BY x,
                                        4))
                """))
                .matches("VALUES ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false)");

        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.repeat(
                                        TABLE(VALUES ('a', true), ('b', false)) t(x, y) ORDER BY y,
                                        4))
                """))
                .matches("VALUES ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false)");

        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.repeat(
                                        TABLE(VALUES ('a', true), ('b', false)) t(x, y) PARTITION BY x ORDER BY y,
                                        4))
                """))
                .matches("VALUES ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false), ('a', true), ('b', false)");

        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.repeat(TABLE(tpch.tiny.part), 3))
                """))
                .matches("SELECT * FROM tpch.tiny.part UNION ALL TABLE tpch.tiny.part UNION ALL TABLE tpch.tiny.part");

        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.repeat(TABLE(tpch.tiny.part) PARTITION BY type, 3))
                """))
                .matches("SELECT * FROM tpch.tiny.part UNION ALL TABLE tpch.tiny.part UNION ALL TABLE tpch.tiny.part");

        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.repeat(TABLE(tpch.tiny.part) ORDER BY size, 3))
                """))
                .matches("SELECT * FROM tpch.tiny.part UNION ALL TABLE tpch.tiny.part UNION ALL TABLE tpch.tiny.part");

        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.repeat(TABLE(tpch.tiny.part) PARTITION BY type ORDER BY size, 3))
                """))
                .matches("SELECT * FROM tpch.tiny.part UNION ALL TABLE tpch.tiny.part UNION ALL TABLE tpch.tiny.part");
    }

    @Test
    public void testFunctionsReturningEmptyPages()
    {
        // the functions empty_output and empty_output_with_pass_through return an empty Page for each processed input Page. the argument has KEEP WHEN EMPTY property

        // non-empty input, no pass-trough columns
        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.empty_output(TABLE(tpch.tiny.orders)))
                """))
                .matches("SELECT true WHERE false");

        // non-empty input, pass-through partitioning column
        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.empty_output(TABLE(tpch.tiny.orders) PARTITION BY orderstatus))
                """))
                .matches("SELECT true, 'X' WHERE false");

        // non-empty input, argument has pass-trough columns
        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.empty_output_with_pass_through(TABLE(tpch.tiny.orders)))
                """))
                .matches("SELECT true, * FROM tpch.tiny.orders WHERE false");

        // non-empty input, argument has pass-trough columns, partitioning column present
        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.empty_output_with_pass_through(TABLE(tpch.tiny.orders) PARTITION BY orderstatus))
                """))
                .matches("SELECT true, * FROM tpch.tiny.orders WHERE false");

        // empty input, no pass-trough columns
        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.empty_output(TABLE(SELECT * FROM tpch.tiny.orders WHERE false)))
                """))
                .matches("SELECT true WHERE false");

        // empty input, pass-through partitioning column
        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.empty_output(TABLE(SELECT * FROM tpch.tiny.orders WHERE false) PARTITION BY orderstatus))
                """))
                .matches("SELECT true, 'X' WHERE false");

        // empty input, argument has pass-trough columns
        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.empty_output_with_pass_through(TABLE(SELECT * FROM tpch.tiny.orders WHERE false)))
                """))
                .matches("SELECT true, * FROM tpch.tiny.orders WHERE false");

        // empty input, argument has pass-trough columns, partitioning column present
        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.empty_output_with_pass_through(TABLE(SELECT * FROM tpch.tiny.orders WHERE false) PARTITION BY orderstatus))
                """))
                .matches("SELECT true, * FROM tpch.tiny.orders WHERE false");

        // function empty_source returns an empty Page for each Split it processes
        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.empty_source())
                """))
                .matches("SELECT true WHERE false");
    }

    @Test
    public void testInputPartitioning()
    {
        // table function test_inputs_function has four table arguments. input_1 has row semantics. input_2, input_3 and input_4 have set semantics.
        // the function outputs one row per each tuple of partition it processes. The row includes a true value, and partitioning values.
        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(VALUES 4, 5, 4, 5, 4) t2(x2) PARTITION BY x2,
                               input_3 => TABLE(VALUES 6, 7, 6) t3(x3) PARTITION BY x3,
                               input_4 => TABLE(VALUES 8, 9)))
                """))
                .matches("VALUES (true, 4, 6), (true, 4, 7), (true, 5, 6), (true, 5, 7)");

        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(VALUES 4, 5, 4, 5, 4) t2(x2) PARTITION BY x2,
                               input_3 => TABLE(VALUES 6, 7, 6) t3(x3) PARTITION BY x3,
                               input_4 => TABLE(VALUES 8, 9) t4(x4) PARTITION BY x4))
                """))
                .matches("VALUES (true, 4, 6, 8), (true, 4, 6, 9), (true, 4, 7, 8), (true, 4, 7, 9), (true, 5, 6, 8), (true, 5, 6, 9), (true, 5, 7, 8), (true, 5, 7, 9)");

        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(VALUES 4, 5, 4, 5, 4) t2(x2) PARTITION BY x2,
                               input_3 => TABLE(VALUES 6, 7, 6) t3(x3) PARTITION BY x3,
                               input_4 => TABLE(VALUES 8, 8) t4(x4) PARTITION BY x4))
                """))
                .matches("VALUES (true, 4, 6, 8), (true, 4, 7, 8), (true, 5, 6, 8), (true, 5, 7, 8)");

        // null partitioning values
        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, null),
                               input_2 => TABLE(VALUES 2, null, 2, null) t2(x2) PARTITION BY x2,
                               input_3 => TABLE(VALUES 3, null, 3, null) t3(x3) PARTITION BY x3,
                               input_4 => TABLE(VALUES null, null) t4(x4) PARTITION BY x4))
                """))
                .matches("VALUES (true, 2, 3, null), (true, 2, null, null), (true, null, 3, null), (true, null, null, null)");

        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(VALUES 4, 5, 4, 5, 4),
                               input_3 => TABLE(VALUES 6, 7, 6),
                               input_4 => TABLE(VALUES 8, 9)))
                """))
                .matches("VALUES true");

        assertThat(query(
                """
                SELECT DISTINCT regionkey, nationkey
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(tpch.tiny.nation),
                               input_2 => TABLE(tpch.tiny.nation) PARTITION BY regionkey ORDER BY name,
                               input_3 => TABLE(tpch.tiny.customer) PARTITION BY nationkey,
                               input_4 => TABLE(tpch.tiny.customer)))
                """))
                .matches("SELECT DISTINCT n.regionkey, c.nationkey FROM tpch.tiny.nation n, tpch.tiny.customer c");
    }

    @Test
    public void testEmptyPartitions()
    {
        // input_1 has row semantics, so it is prune when empty. input_2, input_3 and input_4 have set semantics, and are keep when empty by default
        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(SELECT 2 WHERE false),
                               input_3 => TABLE(SELECT 3 WHERE false),
                               input_4 => TABLE(SELECT 4 WHERE false)))
                """))
                .matches("VALUES true");

        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(SELECT 1 WHERE false),
                               input_2 => TABLE(VALUES 2),
                               input_3 => TABLE(VALUES 3),
                               input_4 => TABLE(VALUES 4)))
                """))
                .returnsEmptyResult();

        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(SELECT 2 WHERE false) t2(x2) PARTITION BY x2,
                               input_3 => TABLE(SELECT 3 WHERE false) t3(x3) PARTITION BY x3,
                               input_4 => TABLE(SELECT 4 WHERE false) t4(x4) PARTITION BY x4))
                """))
                .matches("VALUES (true, CAST(null AS integer), CAST(null AS integer), CAST(null AS integer))");

        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(SELECT 2 WHERE false) t2(x2) PARTITION BY x2,
                               input_3 => TABLE(VALUES 3, 4, 4) t3(x3) PARTITION BY x3,
                               input_4 => TABLE(VALUES 4, 4, 4, 5, 5, 5, 5) t4(x4) PARTITION BY x4))
                """))
                .matches("VALUES (true, CAST(null AS integer), 3, 4), (true, null, 4, 4), (true, null, 4, 5), (true, null, 3, 5)");

        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(SELECT 2 WHERE false) t2(x2) PARTITION BY x2,
                               input_3 => TABLE(SELECT 3 WHERE false) t3(x3) PARTITION BY x3,
                               input_4 => TABLE(VALUES 4, 5) t4(x4) PARTITION BY x4))
                """))
                .matches("VALUES (true, CAST(null AS integer), CAST(null AS integer), 4), (true, null, null, 5)");

        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(SELECT 2 WHERE false) t2(x2) PARTITION BY x2 PRUNE WHEN EMPTY,
                               input_3 => TABLE(SELECT 3 WHERE false) t3(x3) PARTITION BY x3,
                               input_4 => TABLE(VALUES 4, 5) t4(x4) PARTITION BY x4))
                """))
                .returnsEmptyResult();
    }

    @Test
    public void testCopartitioning()
    {
        // all tanbles are by default KEEP WHEN EMPTY. If there is no matching partition, it is null-completed
        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(VALUES 1, 1, 2, 2) t2(x2) PARTITION BY x2,
                               input_3 => TABLE(VALUES 4, 5) t3(x3),
                               input_4 => TABLE(VALUES 2, 2, 2, 3) t4(x4) PARTITION BY x4
                               COPARTITION (t2, t4)))
                """))
                .matches("VALUES (true, 1, null), (true, 2, 2), (true, null, 3)");

        // partition `3` from input_4 is pruned because there is no matching partition in input_2
        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(VALUES 1, 1, 2, 2) t2(x2) PARTITION BY x2 PRUNE WHEN EMPTY,
                               input_3 => TABLE(VALUES 4, 5) t3(x3),
                               input_4 => TABLE(VALUES 2, 2, 2, 3) t4(x4) PARTITION BY x4
                               COPARTITION (t2, t4)))
                """))
                .matches("VALUES (true, 1, null), (true, 2, 2)");

        // partition `1` from input_2 is pruned because there is no matching partition in input_4
        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(VALUES 1, 1, 2, 2) t2(x2) PARTITION BY x2,
                               input_3 => TABLE(VALUES 4, 5) t3(x3),
                               input_4 => TABLE(VALUES 2, 2, 2, 3) t4(x4) PARTITION BY x4 PRUNE WHEN EMPTY
                               COPARTITION (t2, t4)))
                """))
                .matches("VALUES (true, 2, 2), (true, null, 3)");

        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(VALUES 1, 1, 2, 2) t2(x2) PARTITION BY x2 PRUNE WHEN EMPTY,
                               input_3 => TABLE(VALUES 4, 5) t3(x3),
                               input_4 => TABLE(VALUES 2, 2, 2, 3) t4(x4) PARTITION BY x4 PRUNE WHEN EMPTY
                               COPARTITION (t2, t4)))
                """))
                .matches("VALUES (true, 2, 2)");

        // null partitioning values
        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(VALUES 1, 1, null, null, 2, 2) t2(x2) PARTITION BY x2,
                               input_3 => TABLE(VALUES 4, 5) t3(x3),
                               input_4 => TABLE(VALUES null, 2, 2, 2, 3) t4(x4) PARTITION BY x4
                               COPARTITION (t2, t4)))
                """))
                .matches("VALUES (true, 1, null), (true, 2, 2), (true, null, null), (true, null, 3)");

        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(VALUES 1, 1, null, null, 2, 2) t2(x2) PARTITION BY x2 PRUNE WHEN EMPTY,
                               input_3 => TABLE(VALUES 4, 5) t3(x3),
                               input_4 => TABLE(VALUES null, 2, 2, 2, 3) t4(x4) PARTITION BY x4 PRUNE WHEN EMPTY
                               COPARTITION (t2, t4)))
                """))
                .matches("VALUES (true, 2, 2), (true, null, null)");

        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(VALUES 1, 1, null, null) t2(x2) PARTITION BY x2,
                               input_3 => TABLE(VALUES 2, 2, null) t3(x3) PARTITION BY x3,
                               input_4 => TABLE(VALUES 2, 3, 3) t4(x4) PARTITION BY x4
                               COPARTITION (t2, t4, t3)))
                """))
                .matches("VALUES (true, 1, null, null), (true, null, null, null), (true, null, 2, 2), (true, null, null, 3)");

        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(VALUES 1, 1, null, null) t2(x2) PARTITION BY x2,
                               input_3 => TABLE(VALUES 2, 2, null) t3(x3) PARTITION BY x3 PRUNE WHEN EMPTY,
                               input_4 => TABLE(VALUES 2, 3, 3) t4(x4) PARTITION BY x4
                               COPARTITION (t2, t4, t3)))
                """))
                .matches("VALUES (true, CAST(null AS integer), null, null), (true, null, 2, 2)");

        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(VALUES 1, 1, null, null) t2(x2) PARTITION BY x2 PRUNE WHEN EMPTY,
                               input_3 => TABLE(VALUES 2, 2, null) t3(x3) PARTITION BY x3,
                               input_4 => TABLE(VALUES 2, 3, 3) t4(x4) PARTITION BY x4
                               COPARTITION (t2, t4, t3)))
                """))
                .matches("VALUES (true, 1, CAST(null AS integer), CAST(null AS integer)), (true, null, null, null)");

        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.test_inputs_function(
                               input_1 => TABLE(VALUES 1, 2, 3),
                               input_2 => TABLE(VALUES 1, 1, null, null) t2(x2) PARTITION BY x2 PRUNE WHEN EMPTY,
                               input_3 => TABLE(VALUES 2, 2, null) t3(x3) PARTITION BY x3,
                               input_4 => TABLE(VALUES 2, 3, 3) t4(x4) PARTITION BY x4 PRUNE WHEN EMPTY
                               COPARTITION (t2, t4, t3)))
                """))
                .returnsEmptyResult();
    }

    @Test
    public void testPassThroughWithEmptyPartitions()
    {
        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.pass_through(
                                            TABLE(VALUES (1, 'a'), (2, 'b')) t1(a1, b1) PARTITION BY a1,
                                            TABLE(VALUES (2, 'x'), (3, 'y')) t2(a2, b2) PARTITION BY a2
                                            COPARTITION (t1, t2)))
                """))
                .matches("VALUES (true, false, 1, 'a', null, null), (true, true, 2, 'b', 2, 'x'), (false, true, null, null, 3, 'y')");

        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.pass_through(
                                            TABLE(VALUES (1, 'a'), (2, 'b')) t1(a1, b1) PARTITION BY a1,
                                            TABLE(SELECT 2, 'x' WHERE false) t2(a2, b2) PARTITION BY a2
                                            COPARTITION (t1, t2)))
                """))
                .matches("VALUES (true, false, 1, 'a', CAST(null AS integer), CAST(null AS VARCHAR(1))), (true, false, 2, 'b', null, null)");

        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.pass_through(
                                            TABLE(VALUES (1, 'a'), (2, 'b')) t1(a1, b1) PARTITION BY a1,
                                            TABLE(SELECT 2, 'x' WHERE false) t2(a2, b2) PARTITION BY a2))
                """))
                .matches("VALUES (true, false, 1, 'a', CAST(null AS integer), CAST(null AS VARCHAR(1))), (true, false, 2, 'b', null, null)");
    }

    @Test
    public void testPassThroughWithEmptyInput()
    {
        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.pass_through(
                                            TABLE(SELECT 1, 'x' WHERE false) t1(a1, b1) PARTITION BY a1,
                                            TABLE(SELECT 2, 'y' WHERE false) t2(a2, b2) PARTITION BY a2
                                            COPARTITION (t1, t2)))
                """))
                .matches("VALUES (false, false, CAST(null AS integer), CAST(null AS VARCHAR(1)), CAST(null AS integer), CAST(null AS VARCHAR(1)))");

        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.pass_through(
                                            TABLE(SELECT 1, 'x' WHERE false) t1(a1, b1) PARTITION BY a1,
                                            TABLE(SELECT 2, 'y' WHERE false) t2(a2, b2) PARTITION BY a2))
                """))
                .matches("VALUES (false, false, CAST(null AS integer), CAST(null AS VARCHAR(1)), CAST(null AS integer), CAST(null AS VARCHAR(1)))");
    }

    @Test
    public void testInput()
    {
        assertThat(query(
                """
                SELECT got_input
                FROM TABLE(system.test_input(TABLE(VALUES 1)))
                """))
                .matches("VALUES true");

        assertThat(query(
                """
                SELECT got_input
                FROM TABLE(system.test_input(TABLE(VALUES 1, 2, 3) t(a) PARTITION BY a))
                """))
                .matches("VALUES true, true, true");

        assertThat(query(
                """
                SELECT got_input
                FROM TABLE(system.test_input(TABLE(SELECT 1 WHERE false)))
                """))
                .matches("VALUES false");

        assertThat(query(
                """
                SELECT got_input
                FROM TABLE(system.test_input(TABLE(SELECT 1 WHERE false) t(a) PARTITION BY a))
                """))
                .matches("VALUES false");

        assertThat(query(
                """
                SELECT got_input
                FROM TABLE(system.test_input(TABLE(SELECT * FROM tpch.tiny.orders WHERE false)))
                """))
                .matches("VALUES false");

        assertThat(query(
                """
                SELECT got_input
                FROM TABLE(system.test_input(TABLE(SELECT * FROM tpch.tiny.orders WHERE false) PARTITION BY orderstatus ORDER BY orderkey))
                """))
                .matches("VALUES false");
    }

    @Test
    public void testSingleSourceWithRowSemantics()
    {
        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.test_single_input_function(TABLE(VALUES (true), (false), (true))))
                """))
                .matches("VALUES true");
    }

    @Test
    public void testConstantFunction()
    {
        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.constant(5))
                """))
                .matches("VALUES 5");

        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.constant(2, 10))
                """))
                .matches("VALUES (2), (2), (2), (2), (2), (2), (2), (2), (2), (2)");

        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.constant(null, 3))
                """))
                .matches("VALUES (CAST(null AS integer)), (null), (null)");

        // value as constant expression
        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.constant(5 * 4, 3))
                """))
                .matches("VALUES (20), (20), (20)");

        // value out of range for INTEGER type: Integer.MAX_VALUE + 1
        assertThat(query(
                """
                SELECT *
                FROM TABLE(system.constant(2147483648, 3))
                """))
                .failure().hasMessage("line 2:28: Cannot cast type bigint to integer");

        assertThat(query(
                """
                SELECT count(*), count(DISTINCT constant_column), min(constant_column)
                FROM TABLE(system.constant(2, 1000000))
                """))
                .matches("VALUES (BIGINT '1000000', BIGINT '1', 2)");

        assertQueryReturnsEmptyResult(
                """
                SELECT *
                FROM TABLE(system.constant(5, 0))
                """);
    }

    @Test
    public void testPruneAllColumns()
    {
        // function identity_pass_through_function has no proper outputs. It outputs input columns using the pass-through mechanism.
        // in this case, no pass-through columns are referenced, so they are all pruned. The function effectively produces no columns.
        assertThat(query("SELECT 'a' FROM TABLE(system.identity_pass_through_function(input => TABLE(VALUES 1, 2, 3)))"))
                .matches("VALUES 'a', 'a', 'a'");

        // all pass-through columns are pruned. Also, the input is empty, and it has KEEP WHEN EMPTY property, so the function is executed on empty partition.
        assertThat(query("SELECT 'a' FROM TABLE(system.identity_pass_through_function(input => TABLE(SELECT 1 WHERE false)))"))
                .matches("SELECT 'a' WHERE false");

        // all pass-through columns are pruned. Also, the input is empty, and it has PRUNE WHEN EMPTY property, so the function is pruned out.
        assertThat(query("SELECT 'a' FROM TABLE(system.identity_pass_through_function(input => TABLE(SELECT 1 WHERE false) PRUNE WHEN EMPTY))"))
                .matches("SELECT 'a' WHERE false");
    }

    @Test
    public void testPrunePassThroughColumns()
    {
        // function pass_through has 2 proper columns, and it outputs all columns from both inputs using the pass-through mechanism.
        // all columns are referenced
        assertThat(query(
                """
                SELECT p1, p2, x1, x2, y1, y2
                FROM TABLE(system.pass_through(
                                            TABLE(VALUES (1, 'a'), (2, 'b'), (3, 'c')) t1(x1, x2),
                                            TABLE(VALUES (4, 'd'), (5, 'e')) t2(y1, y2))) t(p1, p2)
                """))
                .matches("VALUES (true, true, 3, 'c', 5, 'e')");

        // all pass-through columns are referenced. Proper columns are not referenced, but they are not pruned.
        assertThat(query(
                """
                SELECT x1, x2, y1, y2
                FROM TABLE(system.pass_through(
                                            TABLE(VALUES (1, 'a'), (2, 'b'), (3, 'c')) t1(x1, x2),
                                            TABLE(VALUES (4, 'd'), (5, 'e')) t2(y1, y2))) t(p1, p2)
                """))
                .matches("VALUES (3, 'c', 5, 'e')");

        // some pass-through columns are referenced. Unreferenced pass-through columns are pruned.
        assertThat(query(
                """
                SELECT x2, y2
                FROM TABLE(system.pass_through(
                                            TABLE(VALUES (1, 'a'), (2, 'b'), (3, 'c')) t1(x1, x2),
                                            TABLE(VALUES (4, 'd'), (5, 'e')) t2(y1, y2))) t(p1, p2)
                """))
                .matches("VALUES ('c', 'e')");

        assertThat(query(
                """
                SELECT y1, y2
                FROM TABLE(system.pass_through(
                                            TABLE(VALUES (1, 'a'), (2, 'b'), (3, 'c')) t1(x1, x2),
                                            TABLE(VALUES (4, 'd'), (5, 'e')) t2(y1, y2))) t(p1, p2)
                """))
                .matches("VALUES (5, 'e')");

        // no pass-through columns are referenced. Unreferenced pass-through columns are pruned.
        assertThat(query(
                """
                SELECT 'x'
                FROM TABLE(system.pass_through(
                                            TABLE(VALUES (1, 'a'), (2, 'b'), (3, 'c')) t1(x1, x2),
                                            TABLE(VALUES (4, 'd'), (5, 'e')) t2(y1, y2))) t(p1, p2)
                """))
                .matches("VALUES ('x')");
    }

    @Test
    public void testPrunePassThroughColumnsWithEmptyInput()
    {
        // function pass_through has 2 proper columns, and it outputs all columns from both inputs using the pass-through mechanism.
        // all columns are referenced
        assertThat(query(
                """
                SELECT p1, p2, x1, x2, y1, y2
                FROM TABLE(system.pass_through(
                                            TABLE(SELECT 1, 'a' WHERE FALSE) t1(x1, x2),
                                            TABLE(SELECT 4, 'd' WHERE FALSE) t2(y1, y2))) t(p1, p2)
                """))
                .matches("VALUES (false, false, CAST(null AS integer), CAST(null AS varchar(1)), CAST(null AS integer), CAST(null AS varchar(1)))");

        // all pass-through columns are referenced. Proper columns are not referenced, but they are not pruned.
        assertThat(query(
                """
                SELECT x1, x2, y1, y2
                FROM TABLE(system.pass_through(
                                            TABLE(SELECT 1, 'a' WHERE FALSE) t1(x1, x2),
                                            TABLE(SELECT 4, 'd' WHERE FALSE) t2(y1, y2))) t(p1, p2)
                """))
                .matches("VALUES (CAST(null AS integer), CAST(null AS varchar(1)), CAST(null AS integer), CAST(null AS varchar(1)))");

        // some pass-through columns are referenced. Unreferenced pass-through columns are pruned.
        assertThat(query(
                """
                SELECT x2, y2
                FROM TABLE(system.pass_through(
                                            TABLE(SELECT 1, 'a' WHERE FALSE) t1(x1, x2),
                                            TABLE(SELECT 4, 'd' WHERE FALSE) t2(y1, y2))) t(p1, p2)
                """))
                .matches("VALUES (CAST(null AS varchar(1)), CAST(null AS varchar(1)))");

        assertThat(query(
                """
                SELECT y1, y2
                FROM TABLE(system.pass_through(
                                            TABLE(SELECT 1, 'a' WHERE FALSE) t1(x1, x2),
                                            TABLE(SELECT 4, 'd' WHERE FALSE) t2(y1, y2))) t(p1, p2)
                """))
                .matches("VALUES (CAST(null AS integer), CAST(null AS varchar(1)))");

        // no pass-through columns are referenced. Unreferenced pass-through columns are pruned.
        assertThat(query(
                """
                SELECT 'x'
                FROM TABLE(system.pass_through(
                                            TABLE(SELECT 1, 'a' WHERE FALSE) t1(x1, x2),
                                            TABLE(SELECT 4, 'd' WHERE FALSE) t2(y1, y2))) t(p1, p2)
                """))
                .matches("VALUES ('x')");
    }
}
