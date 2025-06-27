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

import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.JoinOn;
import io.trino.sql.tree.JoinUsing;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Table;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.sql.QueryUtil.equal;
import static io.trino.sql.QueryUtil.nameReference;
import static io.trino.sql.QueryUtil.selectList;
import static io.trino.sql.QueryUtil.simpleQuery;
import static io.trino.sql.parser.ParserAssert.statement;
import static io.trino.sql.parser.TreeNodes.location;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for ASOF JOIN syntax.
 * These tests are disabled until the ASOF JOIN feature is implemented.
 */
public class TestAsofJoin
{
    @Test
    public void testAsofJoinWithExplicitCondition()
    {
        assertThat(statement("SELECT * FROM a ASOF JOIN b ON a.key = b.key AND a.ts >= b.ts"))
                .ignoringLocation()
                .isEqualTo(simpleQuery(
                        selectList(new AllColumns()),
                        new Join(
                                Join.Type.ASOF,
                                new Table(QualifiedName.of("a")),
                                new Table(QualifiedName.of("b")),
                                Optional.of(new JoinOn(
                                        new LogicalExpression(
                                                location(1, 32),
                                                LogicalExpression.Operator.AND,
                                                List.of(
                                                        equal(nameReference("a", "key"), nameReference("b", "key")),
                                                        new ComparisonExpression(
                                                                ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL,
                                                                nameReference("a", "ts"),
                                                                nameReference("b", "ts")))))))));
    }

    @Test
    public void testAsofJoinWithUsingClause()
    {
        assertThat(statement("SELECT * FROM a ASOF JOIN b USING (key, ts)"))
                .ignoringLocation()
                .isEqualTo(simpleQuery(
                        selectList(new AllColumns()),
                        new Join(
                                Join.Type.ASOF,
                                new Table(QualifiedName.of("a")),
                                new Table(QualifiedName.of("b")),
                                Optional.of(new JoinUsing(
                                        List.of(
                                                new Identifier("key"),
                                                new Identifier("ts")))))));
    }
}
