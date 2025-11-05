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
package io.trino.sql.parser;

import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.CreateFunction;
import io.trino.sql.tree.DeterministicCharacteristic;
import io.trino.sql.tree.FunctionSpecification;
import io.trino.sql.tree.LanguageCharacteristic;
import io.trino.sql.tree.ParameterDeclaration;
import io.trino.sql.tree.PropertiesCharacteristic;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.ReturnsClause;
import io.trino.sql.tree.StringLiteral;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.sql.parser.ParserAssert.assertStatementIsInvalid;
import static io.trino.sql.parser.ParserAssert.statement;
import static io.trino.sql.parser.TreeNodes.identifier;
import static io.trino.sql.parser.TreeNodes.location;
import static io.trino.sql.parser.TreeNodes.qualifiedName;
import static io.trino.sql.parser.TreeNodes.simpleType;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSqlParserFunctions
{
    @Test
    public void testLanguageEngineFunction()
    {
        assertThat(statement(
                """
                CREATE FUNCTION hello(s varchar)
                RETURNS varchar
                LANGUAGE PYTHON
                DETERMINISTIC
                WITH (handler = 'hello')
                AS $$
                def hello(s):
                    return 'Hello, ' + s + '!'
                $$
                """))
                .isEqualTo(new CreateFunction(
                        location(1, 1),
                        new FunctionSpecification(
                                location(1, 8),
                                qualifiedName(location(1, 17), "hello"),
                                ImmutableList.of(new ParameterDeclaration(
                                        location(1, 23),
                                        Optional.of(identifier(location(1, 23), "s")),
                                        simpleType(location(1, 25), "varchar"))),
                                new ReturnsClause(location(2, 1), simpleType(location(2, 9), "varchar")),
                                ImmutableList.of(
                                        new LanguageCharacteristic(location(3, 1), identifier(location(3, 10), "PYTHON")),
                                        new DeterministicCharacteristic(location(4, 1), true),
                                        new PropertiesCharacteristic(location(5, 1), ImmutableList.of(new Property(
                                                location(5, 7),
                                                identifier(location(5, 7), "handler"),
                                                new StringLiteral(location(5, 17), "hello"))))),
                                Optional.empty(),
                                Optional.of(new StringLiteral(location(6, 4),
                                        """
                                        def hello(s):
                                            return 'Hello, ' + s + '!'
                                        """))),
                        false));
    }

    @Test
    public void testDefinitionWithoutNewline()
    {
        assertStatementIsInvalid(
                """
                CREATE FUNCTION hello(s varchar)
                RETURNS varchar
                LANGUAGE TEST
                DETERMINISTIC
                AS $$ what $$
                """)
                .withMessage("line 5:4: Function definition must start with a newline after opening quotes");
    }
}
