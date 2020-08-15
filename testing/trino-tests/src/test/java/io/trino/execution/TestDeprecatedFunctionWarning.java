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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.client.Warning;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.operator.aggregation.state.LongAndDoubleState;
import io.trino.operator.window.RankFunction;
import io.trino.spi.WarningCode;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.StandardWarningCode;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Description;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.function.WindowFunctionSignature;
import io.trino.spi.type.StandardTypes;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.execution.TestQueryRunnerUtil.createQueryRunner;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.fail;

public class TestDeprecatedFunctionWarning
{
    private static final WarningCode DEPRECATED_FUNCTION_WARNING_CODE = StandardWarningCode.DEPRECATED_FUNCTION.toWarningCode();
    private static final String EXPECTED_WARNING_MSG = "(DEPRECATED) Use foo() instead.";

    private QueryRunner queryRunner;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        queryRunner = createQueryRunner(ImmutableMap.of());
        ImmutableList.of(
                TestScalaFunction.class,
                TestDeprecatedParametericScalaFunction.class,
                TestNonDeprecatedParametericScalaFunction.class,
                TestDeprecatedAggregation.class,
                TestNonDeprecatedAggregation.class,
                TestDeprecatedWindow.class,
                TestNonDeprecatedWindow.class)
                .forEach(udfClass -> queryRunner.addFunctions(InternalFunctionBundle.builder().functions(udfClass).build()));
        queryRunner.installPlugin(new DeprecatedFunctionsPlugin());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
    }

    @Test
    public void testDeprecatedScalaFunction()
    {
        assertPlannerWarnings(queryRunner,
                "SELECT 123",
                ImmutableList.of());
        assertPlannerWarnings(queryRunner,
                "SELECT lower('A')",
                ImmutableList.of());
        assertPlannerWarnings(queryRunner,
                "SELECT deprecated_scalar()",
                ImmutableList.of(DEPRECATED_FUNCTION_WARNING_CODE));
        assertPlannerWarnings(queryRunner,
                "SELECT non_deprecated_scalar()",
                ImmutableList.of());
    }

    @Test
    public void testDeprecatedDescription()
    {
        String sql = "SELECT deprecated_scalar()";
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();
        List<Warning> warnings = queryRunner.execute(session, sql).getWarnings();
        if (warnings.size() != 1 || !warnings.get(0).getMessage().contains(EXPECTED_WARNING_MSG)) {
            fail("Expected warning: " + EXPECTED_WARNING_MSG);
        }
    }

    @Test
    public void testPluginDeprecatedScalaFunction()
    {
        assertPlannerWarnings(queryRunner,
                "SELECT plugin_deprecated_scalar()",
                ImmutableList.of(DEPRECATED_FUNCTION_WARNING_CODE));
        assertPlannerWarnings(queryRunner,
                "SELECT plugin_non_deprecated_scalar()",
                ImmutableList.of());
    }

    @Test
    public void testDeprecatedParametericScalaFunction()
    {
        assertPlannerWarnings(queryRunner,
                "SELECT deprecated_parameteric_scala(1.2)",
                ImmutableList.of(DEPRECATED_FUNCTION_WARNING_CODE));
        assertPlannerWarnings(queryRunner,
                "SELECT deprecated_parameteric_scala(123)",
                ImmutableList.of(DEPRECATED_FUNCTION_WARNING_CODE));
        assertPlannerWarnings(queryRunner,
                "SELECT non_deprecated_parameteric_scala(1.2)",
                ImmutableList.of());
        assertPlannerWarnings(queryRunner,
                "SELECT non_deprecated_parameteric_scala(123)",
                ImmutableList.of());
    }

    @Test
    public void testDeprecatedAggregationFunction()
    {
        assertPlannerWarnings(queryRunner,
                "SELECT deprecated_aggregation(val) from (VALUES (1),(2),(3)) AS t(val)",
                ImmutableList.of(DEPRECATED_FUNCTION_WARNING_CODE));
        assertPlannerWarnings(queryRunner,
                "SELECT non_deprecated_aggregation(val) from (VALUES (1),(2),(3)) AS t(val)",
                ImmutableList.of());
    }

    @Test
    public void testDeprecatedWindowFunction()
    {
        assertPlannerWarnings(queryRunner,
                "SELECT deprecated_window() OVER (PARTITION BY id) FROM (VALUES (1,10),(2,20),(3,30)) AS t(id, val)",
                ImmutableList.of(DEPRECATED_FUNCTION_WARNING_CODE));
        assertPlannerWarnings(queryRunner,
                "SELECT non_deprecated_window() OVER (PARTITION BY id) FROM (VALUES (1,10),(2,20),(3,30)) AS t(id, val)",
                ImmutableList.of());
    }

    private static void assertPlannerWarnings(QueryRunner queryRunner, @Language("SQL") String sql, List<WarningCode> expectedWarnings)
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();
        Set<Integer> warnings = queryRunner.execute(session, sql).getWarnings().stream()
                .map(Warning::getWarningCode)
                .map(Warning.Code::getCode)
                .collect(toImmutableSet());
        expectedWarnings.stream()
                .map(WarningCode::getCode)
                .forEach(expectedWarning -> {
                    if (!warnings.contains(expectedWarning)) {
                        fail("Expected warning: " + expectedWarning);
                    }
                });
        if (expectedWarnings.isEmpty() && !warnings.isEmpty()) {
            fail("Expect no warning");
        }
    }

    public static class TestScalaFunction
    {
        @Deprecated
        @ScalarFunction("deprecated_scalar")
        @Description(EXPECTED_WARNING_MSG)
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean deprecatedScalar()
        {
            return true;
        }

        @ScalarFunction("non_deprecated_scalar")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean nonDeprecatedScalar()
        {
            return true;
        }
    }

    @Deprecated
    @ScalarFunction("deprecated_parameteric_scala")
    public static class TestDeprecatedParametericScalaFunction
    {
        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean deprecatedParametericScalaDouble(@SqlNullable @SqlType("T") Double value)
        {
            return (value == null);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean deprecatedParametericScalaLong(@SqlNullable @SqlType("T") Long value)
        {
            return (value == null);
        }
    }

    @ScalarFunction("non_deprecated_parameteric_scala")
    public static class TestNonDeprecatedParametericScalaFunction
    {
        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean deprecatedParametericScalaDouble(@SqlNullable @SqlType("T") Double value)
        {
            return (value == null);
        }

        @TypeParameter("T")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean deprecatedParametericScalaLong(@SqlNullable @SqlType("T") Long value)
        {
            return (value == null);
        }
    }

    @Deprecated
    @AggregationFunction("deprecated_aggregation")
    public static class TestDeprecatedAggregation
    {
        @InputFunction
        public static void input(LongAndDoubleState state, @SqlType(StandardTypes.DOUBLE) double value)
        {
        }

        @CombineFunction
        public static void combine(LongAndDoubleState state, LongAndDoubleState otherState)
        {
        }

        @OutputFunction(StandardTypes.DOUBLE)
        public static void output(LongAndDoubleState state, BlockBuilder out)
        {
            DOUBLE.writeDouble(out, 1.0);
        }
    }

    @AggregationFunction("non_deprecated_aggregation")
    public static class TestNonDeprecatedAggregation
    {
        @InputFunction
        public static void input(LongAndDoubleState state, @SqlType(StandardTypes.DOUBLE) double value)
        {
        }

        @CombineFunction
        public static void combine(LongAndDoubleState state, LongAndDoubleState otherState)
        {
        }

        @OutputFunction(StandardTypes.DOUBLE)
        public static void output(LongAndDoubleState state, BlockBuilder out)
        {
            DOUBLE.writeDouble(out, 1.0);
        }
    }

    @Deprecated
    @WindowFunctionSignature(name = "deprecated_window", returnType = "bigint")
    public static class TestDeprecatedWindow
            extends RankFunction
    {
    }

    @WindowFunctionSignature(name = "non_deprecated_window", returnType = "bigint")
    public static class TestNonDeprecatedWindow
            extends RankFunction
    {
    }
}
