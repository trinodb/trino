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
package io.trino.plugin.mongodb;

import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.mongodb.expression.MongoConnectorExpressionRewriterBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import org.bson.Document;
import org.json.JSONException;
import org.junit.Test;
import org.mockito.Mockito;
import org.skyscreamer.jsonassert.JSONAssert;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertTrue;

public class TestMongoExpressionRewrite
{
    private final ConnectorExpressionRewriter<Document> connectorExpressionRewriter;

    public TestMongoExpressionRewrite()
    {
        this.connectorExpressionRewriter = MongoConnectorExpressionRewriterBuilder.build();
    }

    @Test
    public void testContainsFunctionRewrite()
            throws JSONException
    {
        ConnectorExpression expression = buildContainsExpression("col", 1L);

        Optional<Document> predicate = this.connectorExpressionRewriter.rewrite(
                Mockito.mock(ConnectorSession.class),
                expression,
                Map.of("col", new MongoColumnHandle("col", List.of(), new ArrayType(BigintType.BIGINT), false, false, Optional.empty())));

        assertTrue(predicate.isPresent());
        JSONAssert.assertEquals("{\"col\": 1}", predicate.get().toJson(), false);
    }

    private static Call buildContainsExpression(String variableName, Long value)
    {
        return new Call(
                BooleanType.BOOLEAN,
                new FunctionName("contains"),
                List.of(
                        new Variable(variableName, new ArrayType(BigintType.BIGINT)),
                        new Constant(value, BigintType.BIGINT)));
    }

    @Test
    public void testNotContainsRewrite()
            throws JSONException
    {
        ConnectorExpression expression = buildContainsExpression("col", 1L);
        ConnectorExpression notExpression = new Call(
                BooleanType.BOOLEAN,
                new FunctionName("$not"),
                List.of(expression));
        Optional<Document> predicate = this.connectorExpressionRewriter.rewrite(
                Mockito.mock(ConnectorSession.class),
                notExpression,
                Map.of("col", new MongoColumnHandle("col", List.of(), new ArrayType(BigintType.BIGINT), false, false, Optional.empty())));

        assertTrue(predicate.isPresent());
        JSONAssert.assertEquals("{\"$nor\": [{\"col\": 1}]}", predicate.get().toJson(), false);
    }

    @Test
    public void testOrContainsRewrite()
            throws JSONException
    {
        ConnectorExpression expression1 = buildContainsExpression("col", 1L);
        ConnectorExpression expression2 = buildContainsExpression("col", 2L);
        ConnectorExpression expression3 = buildContainsExpression("col", 3L);
        ConnectorExpression notExpression = new Call(
                BooleanType.BOOLEAN,
                new FunctionName("$or"),
                List.of(expression1, expression2, expression3));
        Optional<Document> predicate = this.connectorExpressionRewriter.rewrite(
                Mockito.mock(ConnectorSession.class),
                notExpression,
                Map.of("col", new MongoColumnHandle("col", List.of(), new ArrayType(BigintType.BIGINT), false, false, Optional.empty())));

        assertTrue(predicate.isPresent());
        JSONAssert.assertEquals("{\"$or\": [{\"col\": 1},{\"col\": 2},{\"col\": 3}]}", predicate.get().toJson(), false);
    }
}
