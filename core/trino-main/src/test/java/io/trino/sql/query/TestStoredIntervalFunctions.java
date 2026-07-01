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

import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.testing.TestingMetadata;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.SYNTAX_ERROR;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

final class TestStoredIntervalFunctions
{
    @Test
    void testShowCreateInvalidStoredFunction()
    {
        FunctionMetadata metadata = new FunctionMetadata();
        metadata.functions.put("()", new LanguageFunction("()", "FUNCTION mock.test.f() RETURNS", List.of(), Optional.empty()));
        try (QueryAssertions assertions = createAssertions(metadata)) {
            assertTrinoExceptionThrownBy(() -> assertions.getQueryRunner().execute("SHOW CREATE FUNCTION mock.test.f"))
                    .hasErrorCode(SYNTAX_ERROR)
                    .hasMessageContaining("Failed parsing stored function 'mock.test.f'");
        }
    }

    @Test
    void testShowCreateLegacyFunction()
    {
        FunctionMetadata metadata = new FunctionMetadata();
        metadata.functions.put("(interval day to second,array(interval year to month))", new LanguageFunction(
                "(interval day to second,array(interval year to month))",
                """
                FUNCTION mock.test.f(x INTERVAL DAY TO SECOND, y ARRAY(INTERVAL YEAR TO MONTH))
                RETURNS INTERVAL DAY TO SECOND
                SECURITY INVOKER
                BEGIN
                    DECLARE result INTERVAL DAY TO SECOND DEFAULT CAST('340 00:00:00' AS INTERVAL DAY TO SECOND);
                    RETURN result + x;
                END
                """,
                List.of(),
                Optional.empty()));
        try (QueryAssertions assertions = createAssertions(metadata)) {
            var runner = assertions.getQueryRunner();
            String query = "SELECT CAST(mock.test.f(INTERVAL '1' DAY, ARRAY[INTERVAL '1' YEAR]) AS VARCHAR)";
            assertThat(assertions.query(query)).matches("VALUES VARCHAR '341 00:00:00.000'");
            String sql = (String) runner.execute("SHOW CREATE FUNCTION mock.test.f").getOnlyValue();
            assertThat(sql)
                    .contains("x INTERVAL DAY(9) TO SECOND(3)", "y ARRAY(INTERVAL YEAR(9) TO MONTH)", "RETURNS INTERVAL DAY(9) TO SECOND(3)")
                    .contains("DECLARE result INTERVAL DAY(9) TO SECOND(3)", "CAST('340 00:00:00' AS INTERVAL DAY(9) TO SECOND(3))");
            runner.execute("DROP FUNCTION mock.test.f(INTERVAL DAY TO SECOND, ARRAY(INTERVAL YEAR TO MONTH))");
            assertThat(metadata.functions).isEmpty();
            runner.execute(sql);
            assertThat(metadata.functions).hasSize(1);
            assertThat(assertions.query(query)).matches("VALUES VARCHAR '341 00:00:00.000'");
        }
    }

    @Test
    void testReplaceLegacyFunction()
    {
        for (String[] types : List.of(
                new String[] {"INTERVAL DAY TO SECOND", "interval day(9) to second(3)", "interval day(2) to second(6)"},
                new String[] {"ARRAY(INTERVAL YEAR TO MONTH)", "array(interval year(9) to month)", "array(interval year(2) to month)"})) {
            for (boolean currentExists : List.of(false, true)) {
                FunctionMetadata metadata = new FunctionMetadata();
                String legacyToken = "(" + types[0].toLowerCase(ENGLISH) + ")";
                metadata.functions.put(legacyToken, new LanguageFunction(
                        legacyToken,
                        "FUNCTION mock.test.f(x %s) RETURNS VARCHAR SECURITY INVOKER RETURN 'old'".formatted(types[0]),
                        List.of(),
                        Optional.empty()));
                try (QueryAssertions assertions = createAssertions(metadata)) {
                    var runner = assertions.getQueryRunner();
                    if (currentExists) {
                        runner.execute("CREATE FUNCTION mock.test.f(x %s) RETURNS VARCHAR SECURITY INVOKER RETURN 'current'".formatted(types[0]));
                    }
                    String replacement =
                            """
                            CREATE OR REPLACE FUNCTION mock.test.f(x %s)
                            RETURNS INTERVAL DAY TO SECOND
                            SECURITY INVOKER
                            RETURN CAST('1' AS INTERVAL DAY TO SECOND)
                            """.formatted(types[0]);
                    runner.execute(replacement);
                    runner.execute(replacement);
                    assertThat(metadata.functions).hasSize(currentExists ? 2 : 1);
                    String selectedType = currentExists ? types[2] : types[1];
                    String selectedToken = currentExists ? "(" + types[2] + ")" : legacyToken;
                    assertThat(metadata.functions.get(selectedToken).sql())
                            .containsIgnoringCase("x " + selectedType)
                            .contains("RETURNS INTERVAL DAY(2) TO SECOND(6)", "CAST('1' AS INTERVAL DAY(2) TO SECOND(6))");
                    assertThat(assertions.query("SELECT CAST(mock.test.f(CAST(NULL AS %s)) AS VARCHAR)".formatted(selectedType)))
                            .matches("VALUES VARCHAR '1 00:00:00.000000'");
                    if (currentExists) {
                        assertThat(assertions.query("SELECT mock.test.f(CAST(NULL AS %s))".formatted(types[1])))
                                .matches("VALUES VARCHAR 'old'");
                    }
                    runner.execute("DROP FUNCTION mock.test.f(%s)".formatted(selectedType));
                    assertThat(metadata.functions).hasSize(currentExists ? 1 : 0);
                }
            }
        }
    }

    private static QueryAssertions createAssertions(FunctionMetadata metadata)
    {
        QueryAssertions assertions = new QueryAssertions(testSessionBuilder().setCatalog("mock").setSchema("test").build());
        var runner = assertions.getQueryRunner();
        runner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withMetadataWrapper(_ -> metadata)
                .build()));
        runner.createCatalog("mock", "mock", Map.of());
        return assertions;
    }

    private static final class FunctionMetadata
            extends TestingMetadata
    {
        private final Map<String, LanguageFunction> functions = new HashMap<>();

        @Override
        public List<String> listSchemaNames(ConnectorSession session)
        {
            return List.of("test");
        }

        @Override
        public Collection<LanguageFunction> getLanguageFunctions(ConnectorSession session, SchemaFunctionName name)
        {
            return name.equals(new SchemaFunctionName("test", "f")) ? List.copyOf(functions.values()) : List.of();
        }

        @Override
        public void createLanguageFunction(ConnectorSession session, SchemaFunctionName name, LanguageFunction function, boolean replace)
        {
            assertThat(functions.containsKey(function.signatureToken())).isEqualTo(replace);
            functions.put(function.signatureToken(), function);
        }

        @Override
        public void dropLanguageFunction(ConnectorSession session, SchemaFunctionName name, String signatureToken)
        {
            assertThat(functions.remove(signatureToken)).isNotNull();
        }
    }
}
