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
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SessionSpecification;
import io.trino.sql.tree.StringLiteral;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.sql.QueryUtil.selectList;
import static io.trino.sql.parser.ParserAssert.statement;
import static org.assertj.core.api.Assertions.assertThat;

public class TestWithSession
{
    @Test
    void testInlineSession()
    {
        assertThat(statement("""
                WITH
                    SESSION session_key = 'property',
                    SESSION catalog.catalog_session_key = 'catalog_property',
                    SESSION catalog.catalog_session_key2 = true
                SELECT foo()
                """))
                .ignoringLocation()
                .isEqualTo(query(ImmutableList.of(
                                new SessionSpecification(QualifiedName.of("session_key"), stringLiteral("property")),
                                new SessionSpecification(QualifiedName.of("catalog", "catalog_session_key"), stringLiteral("catalog_property")),
                                new SessionSpecification(QualifiedName.of("catalog", "catalog_session_key2"), booleanLiteral(true))),
                        selectList(new FunctionCall(QualifiedName.of("foo"), ImmutableList.of()))));
    }

    private static Query query(List<SessionSpecification> sessionSpecifications, Select select)
    {
        return new Query(
                ImmutableList.of(),
                sessionSpecifications,
                Optional.empty(),
                new QuerySpecification(
                        select,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private static StringLiteral stringLiteral(String literal)
    {
        return new StringLiteral(literal);
    }

    private static BooleanLiteral booleanLiteral(boolean value)
    {
        return new BooleanLiteral(Boolean.toString(value));
    }
}
