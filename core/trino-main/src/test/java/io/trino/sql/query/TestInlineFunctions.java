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
package io.trino.sql.query;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.Plugin;
import io.trino.spi.TrinoException;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.LanguageFunctionEngine;
import io.trino.spi.function.ScalarFunctionAdapter;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.Type;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.tpch.TpchConnectorFactory.TPCH_SPLITS_PER_NODE;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.util.Reflection.methodHandle;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestInlineFunctions
{
    private final QueryAssertions assertions;

    public TestInlineFunctions()
    {
        Session session = testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        QueryRunner runner = new StandaloneQueryRunner(session);
        runner.installPlugin(new TpchPlugin());
        runner.createCatalog(TEST_CATALOG_NAME, "tpch", ImmutableMap.of(TPCH_SPLITS_PER_NODE, "1"));
        runner.installPlugin(new TestingLanguageEnginePlugin());

        assertions = new QueryAssertions(runner);
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testSqlFunction()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION my_func(x bigint)
                    RETURNS bigint
                    RETURN x * 2
                SELECT my_func(nationkey)
                FROM nation
                WHERE nationkey = 1
                """))
                .matches("VALUES BIGINT '2'");

        assertThat(assertions.query(
                """
                WITH FUNCTION my_func(x bigint)
                    RETURNS bigint
                    RETURN x * 2
                SELECT my_func(nationkey)
                FROM nation
                WHERE nationkey >= 1
                """))
                .matches("SELECT nationkey * 2 FROM nation WHERE nationkey >= 1");

        assertThat(assertions.query(
                """
                WITH FUNCTION my_func(x bigint)
                    RETURNS bigint
                    RETURN x * 2
                SELECT my_func(nationkey)
                FROM nation
                """))
                .matches("SELECT nationkey * 2 FROM nation");
    }

    @Test
    public void testLanguageEngineFunction()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION twice(x bigint)
                RETURNS bigint
                LANGUAGE TESTING
                WITH (handler = 'correct')
                AS $$
                magic
                $$
                SELECT twice(nationkey)
                FROM nation
                WHERE nationkey = 1
                """))
                .matches("VALUES BIGINT '2'");

        assertThat(assertions.query(
                """
                WITH FUNCTION twice(x bigint)
                RETURNS bigint
                LANGUAGE TESTING
                WITH (handler = 'correct')
                AS $$
                magic
                $$
                SELECT twice(nationkey)
                FROM nation
                WHERE nationkey >= 1
                """))
                .matches("SELECT nationkey * 2 FROM nation WHERE nationkey >= 1");

        assertThat(assertions.query(
                """
                WITH FUNCTION twice(x bigint)
                RETURNS bigint
                LANGUAGE TESTING
                WITH (handler = 'correct')
                AS $$
                magic
                $$
                SELECT twice(nationkey)
                FROM nation
                """))
                .matches("SELECT nationkey * 2 FROM nation");
    }

    @Test
    public void testLanguageEngineFunctionProperties()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION twice(x bigint)
                RETURNS varchar
                LANGUAGE TESTING
                WITH (handler = 'test', oops = 'abc')
                AS $$
                magic
                $$
                VALUES 123
                """))
                .failure()
                .hasErrorCode(INVALID_FUNCTION_PROPERTY)
                .hasMessage("line 4:25: Function language TESTING property 'oops' does not exist");

        assertThat(assertions.query(
                """
                WITH FUNCTION twice(x bigint)
                RETURNS varchar
                LANGUAGE TESTING
                WITH (handler = 888)
                AS $$
                magic
                $$
                VALUES 123
                """))
                .failure()
                .hasErrorCode(INVALID_FUNCTION_PROPERTY)
                .hasMessage("line 4:7: Invalid value for function language TESTING property 'handler': Cannot convert [888] to varchar");
    }

    @Test
    public void testLanguageEngineFunctionValidation()
    {
        assertThat(assertions.query(
                """
                WITH FUNCTION twice(x bigint)
                RETURNS varchar
                LANGUAGE TESTING
                WITH (handler = 'correct')
                AS $$
                magic
                $$
                VALUES 123
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:6: Invalid function 'twice': Invalid return type: varchar");

        assertThat(assertions.query(
                """
                WITH FUNCTION twice(x varchar)
                RETURNS bigint
                LANGUAGE TESTING
                WITH (handler = 'correct')
                AS $$
                magic
                $$
                VALUES 123
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:6: Invalid function 'twice': Invalid argument types: [varchar]");

        assertThat(assertions.query(
                """
                WITH FUNCTION twice(x bigint)
                RETURNS bigint
                LANGUAGE TESTING
                AS $$
                magic
                $$
                VALUES 123
                """))
                .failure()
                .hasErrorCode(INVALID_FUNCTION_PROPERTY)
                .hasMessage("line 1:6: Invalid function 'twice': Handler is required.");

        assertThat(assertions.query(
                """
                WITH FUNCTION twice(x bigint)
                RETURNS bigint
                LANGUAGE TESTING
                WITH (handler = 'abc')
                AS $$
                magic
                $$
                VALUES 123
                """))
                .failure()
                .hasErrorCode(INVALID_FUNCTION_PROPERTY)
                .hasMessage("line 1:6: Invalid function 'twice': Invalid handler value: abc");

        assertThat(assertions.query(
                """
                WITH FUNCTION twice(x bigint)
                RETURNS bigint
                LANGUAGE TESTING
                WITH (handler = 'correct')
                AS $$
                oops
                $$
                VALUES 123
                """))
                .failure()
                .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                .hasMessage("line 1:6: Invalid function 'twice': Invalid definition: oops");
    }

    public static class TestingLanguageEnginePlugin
            implements Plugin
    {
        @Override
        public Iterable<LanguageFunctionEngine> getLanguageFunctionEngines()
        {
            return List.of(new TestingLanguageFunctionEngine());
        }
    }

    public static class TestingLanguageFunctionEngine
            implements LanguageFunctionEngine
    {
        private static final MethodHandle HANDLE = methodHandle(TestingLanguageFunctionEngine.class, "twice", long.class);

        @Override
        public String getLanguage()
        {
            return "TESTING";
        }

        @Override
        public List<PropertyMetadata<?>> getFunctionProperties()
        {
            return List.of(stringProperty("handler", "handler", "", false));
        }

        @Override
        public void validateScalarFunction(Type returnType, List<Type> argumentTypes, String definition, Map<String, Object> properties)
        {
            if (!returnType.equals(BIGINT)) {
                throw new TrinoException(NOT_SUPPORTED, "Invalid return type: " + returnType);
            }

            if (!argumentTypes.equals(List.of(BIGINT))) {
                throw new TrinoException(NOT_SUPPORTED, "Invalid argument types: " + argumentTypes);
            }

            String handler = (String) properties.get("handler");
            if (handler.isEmpty()) {
                throw new TrinoException(INVALID_FUNCTION_PROPERTY, "Handler is required.");
            }
            if (!handler.equals("correct")) {
                throw new TrinoException(INVALID_FUNCTION_PROPERTY, "Invalid handler value: " + handler);
            }

            if (!definition.strip().equals("magic")) {
                throw new TrinoException(FUNCTION_IMPLEMENTATION_ERROR, "Invalid definition: " + definition.strip());
            }
        }

        @Override
        public ScalarFunctionImplementation getScalarFunctionImplementation(
                Type returnType,
                List<Type> argumentTypes,
                String definition,
                Map<String, Object> properties,
                InvocationConvention invocationConvention)
        {
            checkState(returnType.equals(BIGINT));
            checkState(argumentTypes.equals(List.of(BIGINT)));
            checkState(properties.equals(Map.of("handler", "correct")));
            checkState(definition.strip().equals("magic"));

            MethodHandle adapted = ScalarFunctionAdapter.adapt(
                    HANDLE,
                    returnType,
                    argumentTypes,
                    simpleConvention(FAIL_ON_NULL, NEVER_NULL),
                    invocationConvention);

            return ScalarFunctionImplementation.builder()
                    .methodHandle(adapted)
                    .build();
        }

        @UsedByGeneratedCode
        public static long twice(long x)
        {
            return x * 2;
        }
    }
}
