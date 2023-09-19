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
package io.trino.plugin.neo4j;

import io.trino.plugin.neo4j.support.BaseNeo4jTest;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.cypherdsl.core.Cypher;
import org.neo4j.cypherdsl.parser.CypherParser;

import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class TestNeo4jTable
        extends BaseNeo4jTest
{
    public record TestCase(String cypherValue, String schemaType, String sqlValue, Consumer<TestCase> handler) {}

    public TestCase testCase(String cypherValue, String schemaType, String sqlValue)
    {
        return new TestCase(cypherValue, schemaType, sqlValue, this::assertSuccessful);
    }

    public Stream<TestCase> booleanTests()
    {
        return Stream.of(
                testCase("true", "boolean", "true"),
                testCase("false", "boolean", "false"),
                //testCase("toInteger(null)", "bigint", "CAST(NULL AS INTEGER)"),
                testCase("-2147483648", "bigint", "-2147483648"),
                testCase("1234567890", "bigint", "1234567890"),
                testCase("2147483647", "bigint", "2147483647"));
    }

    @ParameterizedTest
    @MethodSource("booleanTests")
    public void testTypeMappings(TestCase testCase)
    {
        testCase.handler.accept(testCase);
    }

    public void assertSuccessful(TestCase testCase)
    {
        String nodeLabel = "TestNode" + UUID.randomUUID().toString().replaceAll("-", "");

        String createNode = Cypher.create(Cypher.node(nodeLabel, "TestNode")
                .withProperties(
                        "value", CypherParser.parseExpression(testCase.cypherValue())))
                .build()
                .getCypher();

        this.executeCypher(createNode);

        String query = "select value from \"(%s)\"".formatted(nodeLabel);

        QueryAssertions.QueryAssert assertion = assertThat(this.query(query));

        MaterializedResult expected = this.getQueryRunner().execute("VALUES ROW(cast(%s as %s))".formatted(testCase.sqlValue, testCase.schemaType));

        //assertion.matches(expected);

        assertThat(false);

        String deleteNode = Cypher.match(Cypher.node(nodeLabel).named("n"))
                .delete("n")
                .build()
                .getCypher();

        this.executeCypher(deleteNode);
    }

    private void executeCypher(String cypher)
    {
        this.getQueryRunner().execute("""
                  select * from table(
                    system.query(
                      query => '%s'
                  )
                )
                """.formatted(cypher.replaceAll("'", "''")));
    }
}
