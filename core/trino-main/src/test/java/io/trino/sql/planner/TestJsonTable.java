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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.execution.warnings.WarningCollector;
import io.trino.json.ir.IrJsonPath;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.table.json.JsonTable;
import io.trino.operator.table.json.JsonTablePlanCross;
import io.trino.operator.table.json.JsonTablePlanLeaf;
import io.trino.operator.table.json.JsonTablePlanNode;
import io.trino.operator.table.json.JsonTablePlanSingle;
import io.trino.operator.table.json.JsonTablePlanUnion;
import io.trino.operator.table.json.JsonTableQueryColumn;
import io.trino.operator.table.json.JsonTableValueColumn;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.TableFunctionNode;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.JsonQuery;
import io.trino.sql.tree.JsonValue;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SymbolReference;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.operator.scalar.json.JsonQueryFunction.JSON_QUERY_FUNCTION_NAME;
import static io.trino.operator.scalar.json.JsonValueFunction.JSON_VALUE_FUNCTION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.sql.analyzer.ExpressionAnalyzer.JSON_NO_PARAMETERS_ROW_TYPE;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.JsonTablePlanComparator.planComparator;
import static io.trino.sql.planner.LogicalPlanner.Stage.CREATED;
import static io.trino.sql.planner.PathNodes.contextVariable;
import static io.trino.sql.planner.PathNodes.literal;
import static io.trino.sql.planner.PathNodes.memberAccessor;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.dataType;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictOutput;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.assertions.TableFunctionMatcher.TableArgumentValue.Builder.tableArgument;
import static io.trino.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static io.trino.type.Json2016Type.JSON_2016;
import static io.trino.type.TestJsonPath2016TypeSerialization.JSON_PATH_2016;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJsonTable
        extends BasePlanTest
{
    private static final ResolvedFunction JSON_VALUE_FUNCTION = new TestingFunctionResolution().resolveFunction(
            JSON_VALUE_FUNCTION_NAME,
            fromTypes(JSON_2016, JSON_PATH_2016, JSON_NO_PARAMETERS_ROW_TYPE, TINYINT, BIGINT, TINYINT, BIGINT));

    private static final ResolvedFunction JSON_QUERY_FUNCTION = new TestingFunctionResolution().resolveFunction(
            JSON_QUERY_FUNCTION_NAME,
            fromTypes(JSON_2016, JSON_PATH_2016, JSON_NO_PARAMETERS_ROW_TYPE, TINYINT, TINYINT, TINYINT));

    @Test
    public void testJsonTableInitialPlan()
    {
        assertPlan(
                """
                        SELECT *
                        FROM (SELECT '[1, 2, 3]', 4) t(json_col, int_col), JSON_TABLE(
                            json_col,
                            'lax $'  AS root_path PASSING int_col AS id, '[ala]' FORMAT JSON AS name
                            COLUMNS(
                                bigint_col BIGINT DEFAULT 5 ON EMPTY DEFAULT int_col ON ERROR,
                                varchar_col VARCHAR FORMAT JSON ERROR ON ERROR)
                            EMPTY ON ERROR)
                            """,
                CREATED,
                strictOutput(// left-side columns first, json_table columns next
                        ImmutableList.of("json_col", "int_col", "bigint_col", "formatted_varchar_col"),
                        anyTree(
                                project(
                                        ImmutableMap.of("formatted_varchar_col", expression(new FunctionCall(QualifiedName.of("$json_to_varchar"), ImmutableList.of(new SymbolReference("varchar_col"), new GenericLiteral("tinyint", "1"), FALSE_LITERAL)))),
                                        tableFunction(builder -> builder
                                                        .name("$json_table")
                                                        .addTableArgument(
                                                                "$input",
                                                                tableArgument(0)
                                                                        .rowSemantics()
                                                                        .passThroughColumns()
                                                                        .passThroughSymbols(ImmutableSet.of("json_col", "int_col")))
                                                        .properOutputs(ImmutableList.of("bigint_col", "varchar_col")),
                                                project(
                                                        ImmutableMap.of(
                                                                "context_item", expression(new FunctionCall(QualifiedName.of("$varchar_to_json"), ImmutableList.of(new SymbolReference("json_col_coerced"), FALSE_LITERAL))), // apply input function to context item
                                                                "parameters_row", expression(new Cast(new Row(ImmutableList.of(new SymbolReference("int_col"), new FunctionCall(QualifiedName.of("$varchar_to_json"), ImmutableList.of(new SymbolReference("name_coerced"), FALSE_LITERAL)))), dataType("row(id integer, name json2016)")))), // apply input function to formatted path parameter and gather path parameters in a row
                                                        project(// coerce context item, path parameters and default expressions
                                                                ImmutableMap.of(
                                                                        "name_coerced", expression(new Cast(new SymbolReference("name"), dataType("varchar"))), // cast formatted path parameter to VARCHAR for the input function
                                                                        "default_value_coerced", expression(new Cast(new SymbolReference("default_value"), dataType("bigint"))), // cast default value to BIGINT to match declared return type for the column
                                                                        "json_col_coerced", expression(new Cast(new SymbolReference("json_col"), dataType("varchar"))), // cast context item to VARCHAR for the input function
                                                                        "int_col_coerced", expression(new Cast(new SymbolReference("int_col"), dataType("bigint")))), // cast default value to BIGINT to match declared return type for the column
                                                                project(// pre-project context item, path parameters and default expressions
                                                                        ImmutableMap.of(
                                                                                "name", expression(new StringLiteral("[ala]")),
                                                                                "default_value", expression(new LongLiteral("5"))),
                                                                        anyTree(
                                                                                project(
                                                                                        ImmutableMap.of(
                                                                                                "json_col", expression(new StringLiteral("[1, 2, 3]")),
                                                                                                "int_col", expression(new LongLiteral("4"))),
                                                                                        values(1)))))))))));
    }

    @Test
    public void testImplicitColumnPath()
    {
        assertJsonTablePlan(
                """
                        SELECT *
                        FROM (SELECT 1, 2, 3), JSON_TABLE(
                            '[1, 2, 3]',
                            'lax $' AS root_path
                            COLUMNS(
                                first_col BIGINT,
                                "Second_Col" BIGINT,
                                "_""_'_?_" BIGINT))
                            """,
                new JsonTablePlanLeaf(
                        new IrJsonPath(true, contextVariable()),
                        ImmutableList.of(
                                valueColumn(0, new IrJsonPath(true, memberAccessor(contextVariable(), "FIRST_COL"))),
                                valueColumn(1, new IrJsonPath(true, memberAccessor(contextVariable(), "Second_Col"))),
                                valueColumn(2, new IrJsonPath(true, memberAccessor(contextVariable(), "_\"_'_?_"))))));
    }

    @Test
    public void testExplicitColumnPath()
    {
        assertJsonTablePlan(
                """
                        SELECT *
                        FROM (SELECT 1, 2, 3), JSON_TABLE(
                            '[1, 2, 3]',
                            'lax $' AS root_path
                            COLUMNS(
                                first_col BIGINT PATH 'lax $.a',
                                "Second_Col" BIGINT PATH 'lax $.B',
                                "_""_'_?_" BIGINT PATH 'lax false'))
                            """,
                new JsonTablePlanLeaf(
                        new IrJsonPath(true, contextVariable()),
                        ImmutableList.of(
                                valueColumn(0, new IrJsonPath(true, memberAccessor(contextVariable(), "a"))),
                                valueColumn(1, new IrJsonPath(true, memberAccessor(contextVariable(), "B"))),
                                valueColumn(2, new IrJsonPath(true, literal(BOOLEAN, false))))));
    }

    @Test
    public void testColumnOutputIndex()
    {
        // output indexes follow the declaration order: [a, b, c, d]
        assertJsonTablePlan(
                """
                        SELECT *
                        FROM (SELECT 1, 2, 3), JSON_TABLE(
                            '[1, 2, 3]',
                            'lax $' AS root_path
                            COLUMNS(
                                a BIGINT,
                                NESTED PATH 'lax $.x' COLUMNS(
                                    b BIGINT,
                                    NESTED PATH 'lax $.y' COLUMNS(
                                        c BIGINT)),
                                d BIGINT))
                            """,
                new JsonTablePlanSingle(
                        new IrJsonPath(true, contextVariable()),
                        ImmutableList.of(
                                valueColumn(0, new IrJsonPath(true, memberAccessor(contextVariable(), "A"))),
                                valueColumn(3, new IrJsonPath(true, memberAccessor(contextVariable(), "D")))),
                        true,
                        new JsonTablePlanSingle(
                                new IrJsonPath(true, memberAccessor(contextVariable(), "x")),
                                ImmutableList.of(valueColumn(1, new IrJsonPath(true, memberAccessor(contextVariable(), "B")))),
                                true,
                                new JsonTablePlanLeaf(
                                        new IrJsonPath(true, memberAccessor(contextVariable(), "y")),
                                        ImmutableList.of(valueColumn(2, new IrJsonPath(true, memberAccessor(contextVariable(), "C"))))))));
    }

    @Test
    public void testColumnBehavior()
    {
        assertJsonTablePlan(
                """
                        SELECT *
                        FROM (SELECT 1, 2, 3), JSON_TABLE(
                            '[1, 2, 3]',
                            'lax $' AS root_path
                            COLUMNS(
                                a BIGINT,
                                b BIGINT NULL ON EMPTY ERROR ON ERROR,
                                c BIGINT DEFAULT 1 ON EMPTY DEFAULT 2 ON ERROR,
                                d VARCHAR FORMAT JSON,
                                e VARCHAR FORMAT JSON WITH CONDITIONAL ARRAY WRAPPER NULL ON EMPTY ERROR ON ERROR,
                                f VARCHAR FORMAT JSON OMIT QUOTES EMPTY ARRAY ON EMPTY EMPTY OBJECT ON ERROR))
                            """,
                new JsonTablePlanLeaf(
                        new IrJsonPath(true, contextVariable()),
                        ImmutableList.of(
                                valueColumn(
                                        0,
                                        new IrJsonPath(true, memberAccessor(contextVariable(), "A")),
                                        JsonValue.EmptyOrErrorBehavior.NULL,
                                        -1,
                                        JsonValue.EmptyOrErrorBehavior.NULL,
                                        -1),
                                valueColumn(
                                        1,
                                        new IrJsonPath(true, memberAccessor(contextVariable(), "B")),
                                        JsonValue.EmptyOrErrorBehavior.NULL,
                                        -1,
                                        JsonValue.EmptyOrErrorBehavior.ERROR,
                                        -1),
                                valueColumn(
                                        2,
                                        new IrJsonPath(true, memberAccessor(contextVariable(), "C")),
                                        JsonValue.EmptyOrErrorBehavior.DEFAULT,
                                        2,
                                        JsonValue.EmptyOrErrorBehavior.DEFAULT,
                                        3),
                                queryColumn(
                                        3,
                                        new IrJsonPath(true, memberAccessor(contextVariable(), "D")),
                                        JsonQuery.ArrayWrapperBehavior.WITHOUT,
                                        JsonQuery.EmptyOrErrorBehavior.NULL,
                                        JsonQuery.EmptyOrErrorBehavior.NULL),
                                queryColumn(
                                        4,
                                        new IrJsonPath(true, memberAccessor(contextVariable(), "E")),
                                        JsonQuery.ArrayWrapperBehavior.CONDITIONAL,
                                        JsonQuery.EmptyOrErrorBehavior.NULL,
                                        JsonQuery.EmptyOrErrorBehavior.ERROR),
                                queryColumn(
                                        5,
                                        new IrJsonPath(true, memberAccessor(contextVariable(), "F")),
                                        JsonQuery.ArrayWrapperBehavior.WITHOUT,
                                        JsonQuery.EmptyOrErrorBehavior.EMPTY_ARRAY,
                                        JsonQuery.EmptyOrErrorBehavior.EMPTY_OBJECT))));
    }

    @Test
    public void testInheritedErrorBehavior()
    {
        // the column has no explicit error behavior, and json_table has no explicit error behavior. The default behavior for column is NULL ON ERROR.
        assertJsonTablePlan(
                """
                        SELECT *
                        FROM (SELECT 1, 2, 3), JSON_TABLE(
                            '[1, 2, 3]',
                            'lax $' AS root_path
                            COLUMNS(
                                a BIGINT))
                            """,
                new JsonTablePlanLeaf(
                        new IrJsonPath(true, contextVariable()),
                        ImmutableList.of(
                                valueColumn(
                                        0,
                                        new IrJsonPath(true, memberAccessor(contextVariable(), "A")),
                                        JsonValue.EmptyOrErrorBehavior.NULL,
                                        -1,
                                        JsonValue.EmptyOrErrorBehavior.NULL,
                                        -1))));

        // the column has no explicit error behavior, and json_table has explicit ERROR ON ERROR. The default behavior for column is ERROR ON ERROR.
        assertJsonTablePlan(
                """
                        SELECT *
                        FROM (SELECT 1, 2, 3), JSON_TABLE(
                            '[1, 2, 3]',
                            'lax $' AS root_path
                            COLUMNS(
                                a BIGINT)
                            ERROR ON ERROR)
                            """,
                new JsonTablePlanLeaf(
                        new IrJsonPath(true, contextVariable()),
                        ImmutableList.of(
                                valueColumn(
                                        0,
                                        new IrJsonPath(true, memberAccessor(contextVariable(), "A")),
                                        JsonValue.EmptyOrErrorBehavior.NULL,
                                        -1,
                                        JsonValue.EmptyOrErrorBehavior.ERROR,
                                        -1))));

        // the column has no explicit error behavior, and json_table has explicit EMPTY ON ERROR. The default behavior for column is NULL ON ERROR.
        assertJsonTablePlan(
                """
                        SELECT *
                        FROM (SELECT 1, 2, 3), JSON_TABLE(
                            '[1, 2, 3]',
                            'lax $' AS root_path
                            COLUMNS(
                                a BIGINT)
                            EMPTY ON ERROR)
                            """,
                new JsonTablePlanLeaf(
                        new IrJsonPath(true, contextVariable()),
                        ImmutableList.of(
                                valueColumn(
                                        0,
                                        new IrJsonPath(true, memberAccessor(contextVariable(), "A")),
                                        JsonValue.EmptyOrErrorBehavior.NULL,
                                        -1,
                                        JsonValue.EmptyOrErrorBehavior.NULL,
                                        -1))));

        // the column has  explicit NULL ON ERROR behavior, and json_table has no explicit ERROR ON ERROR. The behavior for column is the one explicitly specified.
        assertJsonTablePlan(
                """
                        SELECT *
                        FROM (SELECT 1, 2, 3), JSON_TABLE(
                            '[1, 2, 3]',
                            'lax $' AS root_path
                            COLUMNS(
                                a BIGINT NULL ON ERROR)
                            ERROR ON ERROR)
                            """,
                new JsonTablePlanLeaf(
                        new IrJsonPath(true, contextVariable()),
                        ImmutableList.of(
                                valueColumn(
                                        0,
                                        new IrJsonPath(true, memberAccessor(contextVariable(), "A")),
                                        JsonValue.EmptyOrErrorBehavior.NULL,
                                        -1,
                                        JsonValue.EmptyOrErrorBehavior.NULL,
                                        -1))));
    }

    @Test
    public void testImplicitDefaultPlan()
    {
        // implicit plan settings are OUTER, UNION
        assertJsonTablePlan(
                """
                        SELECT *
                        FROM (SELECT 1, 2, 3), JSON_TABLE(
                            '[1, 2, 3]',
                            'lax $' AS root_path
                            COLUMNS(
                                NESTED PATH 'lax $.a' COLUMNS(col_1 BIGINT),
                                NESTED PATH 'lax $.b' COLUMNS(
                                    NESTED PATH 'lax $.c' COLUMNS(col_2 BIGINT),
                                    NESTED PATH 'lax $.d' COLUMNS(col_3 BIGINT)),
                                NESTED PATH 'lax $.e' COLUMNS(col_4 BIGINT)))
                            """,
                new JsonTablePlanSingle(
                        new IrJsonPath(true, contextVariable()),
                        ImmutableList.of(),
                        true,
                        new JsonTablePlanUnion(ImmutableList.of(
                                new JsonTablePlanLeaf(
                                        new IrJsonPath(true, memberAccessor(contextVariable(), "a")),
                                        ImmutableList.of(valueColumn(0, new IrJsonPath(true, memberAccessor(contextVariable(), "COL_1"))))),
                                new JsonTablePlanSingle(
                                        new IrJsonPath(true, memberAccessor(contextVariable(), "b")),
                                        ImmutableList.of(),
                                        true,
                                        new JsonTablePlanUnion(ImmutableList.of(
                                                new JsonTablePlanLeaf(
                                                        new IrJsonPath(true, memberAccessor(contextVariable(), "c")),
                                                        ImmutableList.of(valueColumn(1, new IrJsonPath(true, memberAccessor(contextVariable(), "COL_2"))))),
                                                new JsonTablePlanLeaf(
                                                        new IrJsonPath(true, memberAccessor(contextVariable(), "d")),
                                                        ImmutableList.of(valueColumn(2, new IrJsonPath(true, memberAccessor(contextVariable(), "COL_3")))))))),
                                new JsonTablePlanLeaf(
                                        new IrJsonPath(true, memberAccessor(contextVariable(), "e")),
                                        ImmutableList.of(valueColumn(3, new IrJsonPath(true, memberAccessor(contextVariable(), "COL_4")))))))));
    }

    @Test
    public void testExplicitDefaultPlan()
    {
        assertJsonTablePlan(
                """
                        SELECT *
                        FROM (SELECT 1, 2, 3), JSON_TABLE(
                            '[1, 2, 3]',
                            'lax $' AS root_path
                            COLUMNS(
                                NESTED PATH 'lax $.a' AS a COLUMNS(col_1 BIGINT),
                                NESTED PATH 'lax $.b' AS b COLUMNS(
                                    NESTED PATH 'lax $.c' AS c COLUMNS(col_2 BIGINT),
                                    NESTED PATH 'lax $.d' AS d COLUMNS(col_3 BIGINT)),
                                NESTED PATH 'lax $.e' AS e COLUMNS(col_4 BIGINT))
                            PLAN DEFAULT (INNER, CROSS))
                            """,
                new JsonTablePlanSingle(
                        new IrJsonPath(true, contextVariable()),
                        ImmutableList.of(),
                        false,
                        new JsonTablePlanCross(ImmutableList.of(
                                new JsonTablePlanLeaf(
                                        new IrJsonPath(true, memberAccessor(contextVariable(), "a")),
                                        ImmutableList.of(valueColumn(0, new IrJsonPath(true, memberAccessor(contextVariable(), "COL_1"))))),
                                new JsonTablePlanSingle(
                                        new IrJsonPath(true, memberAccessor(contextVariable(), "b")),
                                        ImmutableList.of(),
                                        false,
                                        new JsonTablePlanCross(ImmutableList.of(
                                                new JsonTablePlanLeaf(
                                                        new IrJsonPath(true, memberAccessor(contextVariable(), "c")),
                                                        ImmutableList.of(valueColumn(1, new IrJsonPath(true, memberAccessor(contextVariable(), "COL_2"))))),
                                                new JsonTablePlanLeaf(
                                                        new IrJsonPath(true, memberAccessor(contextVariable(), "d")),
                                                        ImmutableList.of(valueColumn(2, new IrJsonPath(true, memberAccessor(contextVariable(), "COL_3")))))))),
                                new JsonTablePlanLeaf(
                                        new IrJsonPath(true, memberAccessor(contextVariable(), "e")),
                                        ImmutableList.of(valueColumn(3, new IrJsonPath(true, memberAccessor(contextVariable(), "COL_4")))))))));

        assertJsonTablePlan(
                """
                        SELECT *
                        FROM (SELECT 1, 2, 3), JSON_TABLE(
                            '[1, 2, 3]',
                            'lax $' AS root_path
                            COLUMNS(
                                NESTED PATH 'lax $.a' AS a COLUMNS(col_1 BIGINT),
                                NESTED PATH 'lax $.b' AS b COLUMNS(
                                    NESTED PATH 'lax $.c' AS c COLUMNS(col_2 BIGINT),
                                    NESTED PATH 'lax $.d' AS d COLUMNS(col_3 BIGINT)),
                                NESTED PATH 'lax $.e' AS e COLUMNS(col_4 BIGINT))
                            PLAN DEFAULT (CROSS))
                            """,
                new JsonTablePlanSingle(
                        new IrJsonPath(true, contextVariable()),
                        ImmutableList.of(),
                        true,
                        new JsonTablePlanCross(ImmutableList.of(
                                new JsonTablePlanLeaf(
                                        new IrJsonPath(true, memberAccessor(contextVariable(), "a")),
                                        ImmutableList.of(valueColumn(0, new IrJsonPath(true, memberAccessor(contextVariable(), "COL_1"))))),
                                new JsonTablePlanSingle(
                                        new IrJsonPath(true, memberAccessor(contextVariable(), "b")),
                                        ImmutableList.of(),
                                        true,
                                        new JsonTablePlanCross(ImmutableList.of(
                                                new JsonTablePlanLeaf(
                                                        new IrJsonPath(true, memberAccessor(contextVariable(), "c")),
                                                        ImmutableList.of(valueColumn(1, new IrJsonPath(true, memberAccessor(contextVariable(), "COL_2"))))),
                                                new JsonTablePlanLeaf(
                                                        new IrJsonPath(true, memberAccessor(contextVariable(), "d")),
                                                        ImmutableList.of(valueColumn(2, new IrJsonPath(true, memberAccessor(contextVariable(), "COL_3")))))))),
                                new JsonTablePlanLeaf(
                                        new IrJsonPath(true, memberAccessor(contextVariable(), "e")),
                                        ImmutableList.of(valueColumn(3, new IrJsonPath(true, memberAccessor(contextVariable(), "COL_4")))))))));
    }

    @Test
    public void testSpecificPlan()
    {
        assertJsonTablePlan(
                """
                        SELECT *
                        FROM (SELECT 1, 2, 3), JSON_TABLE(
                            '[1, 2, 3]',
                            'lax $' AS root_path
                            COLUMNS(
                                NESTED PATH 'lax $.a' AS a COLUMNS(col_1 BIGINT),
                                NESTED PATH 'lax $.b' AS b COLUMNS(
                                    NESTED PATH 'lax $.c' AS c COLUMNS(col_2 BIGINT),
                                    NESTED PATH 'lax $.d' AS d COLUMNS(col_3 BIGINT)),
                                NESTED PATH 'lax $.e' AS e COLUMNS(col_4 BIGINT))
                            PLAN (ROOT_PATH INNER (((B OUTER (D CROSS C)) UNION E) CROSS A)))
                            """,
                new JsonTablePlanSingle(
                        new IrJsonPath(true, contextVariable()),
                        ImmutableList.of(),
                        false,
                        new JsonTablePlanCross(ImmutableList.of(
                                new JsonTablePlanUnion(ImmutableList.of(
                                        new JsonTablePlanSingle(
                                                new IrJsonPath(true, memberAccessor(contextVariable(), "b")),
                                                ImmutableList.of(),
                                                true,
                                                new JsonTablePlanCross(ImmutableList.of(
                                                        new JsonTablePlanLeaf(
                                                                new IrJsonPath(true, memberAccessor(contextVariable(), "d")),
                                                                ImmutableList.of(valueColumn(2, new IrJsonPath(true, memberAccessor(contextVariable(), "COL_3"))))),
                                                        new JsonTablePlanLeaf(
                                                                new IrJsonPath(true, memberAccessor(contextVariable(), "c")),
                                                                ImmutableList.of(valueColumn(1, new IrJsonPath(true, memberAccessor(contextVariable(), "COL_2")))))))),
                                        new JsonTablePlanLeaf(
                                                new IrJsonPath(true, memberAccessor(contextVariable(), "e")),
                                                ImmutableList.of(valueColumn(3, new IrJsonPath(true, memberAccessor(contextVariable(), "COL_4"))))))),
                                new JsonTablePlanLeaf(
                                        new IrJsonPath(true, memberAccessor(contextVariable(), "a")),
                                        ImmutableList.of(valueColumn(0, new IrJsonPath(true, memberAccessor(contextVariable(), "COL_1")))))))));
    }

    private static JsonTableValueColumn valueColumn(int outputIndex, IrJsonPath path)
    {
        return valueColumn(outputIndex, path, JsonValue.EmptyOrErrorBehavior.NULL, -1, JsonValue.EmptyOrErrorBehavior.NULL, -1);
    }

    private static JsonTableValueColumn valueColumn(int outputIndex, IrJsonPath path, JsonValue.EmptyOrErrorBehavior emptyBehavior, int emptyDefaultInput, JsonValue.EmptyOrErrorBehavior errorBehavior, int errorDefaultInput)
    {
        return new JsonTableValueColumn(outputIndex, JSON_VALUE_FUNCTION, path, emptyBehavior.ordinal(), emptyDefaultInput, errorBehavior.ordinal(), errorDefaultInput);
    }

    private static JsonTableQueryColumn queryColumn(int outputIndex, IrJsonPath path, JsonQuery.ArrayWrapperBehavior wrapperBehavior, JsonQuery.EmptyOrErrorBehavior emptyBehavior, JsonQuery.EmptyOrErrorBehavior errorBehavior)
    {
        return new JsonTableQueryColumn(outputIndex, JSON_QUERY_FUNCTION, path, wrapperBehavior.ordinal(), emptyBehavior.ordinal(), errorBehavior.ordinal());
    }

    private void assertJsonTablePlan(@Language("SQL") String sql, JsonTablePlanNode expectedPlan)
    {
        try {
            getPlanTester().inTransaction(transactionSession -> {
                Plan queryPlan = getPlanTester().createPlan(transactionSession, sql, ImmutableList.of(), CREATED, WarningCollector.NOOP, createPlanOptimizersStatsCollector());
                TableFunctionNode tableFunctionNode = getOnlyElement(PlanNodeSearcher.searchFrom(queryPlan.getRoot()).where(TableFunctionNode.class::isInstance).findAll());
                JsonTablePlanNode actualPlan = ((JsonTable.JsonTableFunctionHandle) tableFunctionNode.getHandle().getFunctionHandle()).processingPlan();
                assertThat(actualPlan)
                        .usingComparator(planComparator())
                        .isEqualTo(expectedPlan);
                return null;
            });
        }
        catch (Throwable e) {
            e.addSuppressed(new Exception("Query: " + sql));
            throw e;
        }
    }
}
