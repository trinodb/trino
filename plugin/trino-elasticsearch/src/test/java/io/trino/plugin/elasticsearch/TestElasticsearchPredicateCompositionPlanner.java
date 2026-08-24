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
package io.trino.plugin.elasticsearch;

import io.trino.plugin.elasticsearch.client.IndexMetadata.PrimitiveType;
import io.trino.plugin.elasticsearch.decoders.IntegerDecoder;
import io.trino.plugin.elasticsearch.decoders.VarcharDecoder;
import io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.elasticsearch.FullTextPushdownMode.SAFE;
import static io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Enforcement.PREFILTER;
import static io.trino.spi.expression.Constant.TRUE;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestElasticsearchPredicateCompositionPlanner
{
    @Test
    public void testExactArrayOrNormalizesToTerms()
    {
        ArrayType arrayType = new ArrayType(INTEGER);
        ElasticsearchColumnHandle column = integerArrayColumn("Numbers");
        Call first = contains("numbers", arrayType, 1L);
        Call second = contains("numbers", arrayType, 2L);
        Call expression = new Call(BOOLEAN, OR_FUNCTION_NAME, List.of(first, second));

        ElasticsearchPredicatePushdownPlanner.Result result = plan(expression, Map.of("numbers", column));

        assertThat(result.remainingConstraint().getExpression()).isEqualTo(TRUE);
        assertThat(result.residualExpressions()).isEmpty();
        assertThat(result.remotePredicate()).contains(new ElasticsearchRemotePredicate.Terms("Numbers", List.of(1L, 2L)));
    }

    @Test
    public void testDocumentScopeArrayAndKeepsIndependentTerms()
    {
        ArrayType arrayType = new ArrayType(INTEGER);
        ElasticsearchColumnHandle column = integerArrayColumn("Numbers");
        Call first = contains("numbers", arrayType, 1L);
        Call second = contains("numbers", arrayType, 2L);
        Call expression = new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(first, second));

        ElasticsearchPredicatePushdownPlanner.Result result = plan(expression, Map.of("numbers", column));

        assertThat(result.remainingConstraint().getExpression()).isEqualTo(TRUE);
        assertThat(result.residualExpressions()).isEmpty();
        assertThat(result.remotePredicate()).contains(new ElasticsearchRemotePredicate.And(List.of(
                new ElasticsearchRemotePredicate.Term("Numbers", 1L),
                new ElasticsearchRemotePredicate.Term("Numbers", 2L))));
    }

    @Test
    public void testPartialOrBecomesPlannerOwnedResidual()
    {
        ArrayType arrayType = new ArrayType(INTEGER);
        ElasticsearchColumnHandle column = integerArrayColumn("Numbers");
        Call exact = contains("numbers", arrayType, 1L);
        Call unsupported = new Call(
                BOOLEAN,
                new FunctionName("unsupported_predicate"),
                List.of(new Variable("numbers", arrayType)));
        Call expression = new Call(BOOLEAN, OR_FUNCTION_NAME, List.of(exact, unsupported));

        ElasticsearchPredicatePushdownPlanner.Result result = plan(expression, Map.of("numbers", column));

        assertThat(result.remotePredicate()).isEmpty();
        assertThat(result.remainingConstraint().getExpression()).isEqualTo(TRUE);
        assertThat(result.residualExpressions()).containsExactly(expression);
    }

    @Test
    public void testPartialAndStillUsesSafeExactCandidate()
    {
        ArrayType arrayType = new ArrayType(INTEGER);
        ElasticsearchColumnHandle column = integerArrayColumn("Numbers");
        Call exact = contains("numbers", arrayType, 1L);
        Call unsupported = new Call(
                BOOLEAN,
                new FunctionName("unsupported_predicate"),
                List.of(new Variable("numbers", arrayType)));
        Call expression = new Call(BOOLEAN, AND_FUNCTION_NAME, List.of(exact, unsupported));

        ElasticsearchPredicatePushdownPlanner.Result result = plan(expression, Map.of("numbers", column));

        assertThat(result.remotePredicate()).contains(new ElasticsearchRemotePredicate.Enforced(
                new ElasticsearchRemotePredicate.Term("Numbers", 1L),
                PREFILTER));
        assertThat(result.remainingConstraint().getExpression()).isEqualTo(unsupported);
        assertThat(result.residualExpressions()).isEmpty();
    }

    @Test
    public void testSafeAnalyzedFullTextOrRemainsPlannerOwnedResidual()
    {
        ElasticsearchColumnHandle column = analyzedTextColumn("message");
        Call first = like("message", "fatal%");
        Call second = like("message", "error%");
        Call expression = new Call(BOOLEAN, OR_FUNCTION_NAME, List.of(first, second));

        ElasticsearchPredicatePushdownPlanner.Result result = plan(expression, Map.of("message", column));

        assertThat(result.remainingConstraint().getExpression()).isEqualTo(TRUE);
        assertThat(result.residualExpressions()).containsExactly(expression);
        assertThat(result.remotePredicate()).isEmpty();
    }

    @Test
    public void testMixedExactAndProvenSafePrefilterOrKeepsWholeOrResidual()
    {
        ElasticsearchColumnHandle status = keywordColumn("status");
        ElasticsearchColumnHandle message = keywordColumn("message");
        Call exact = like("status", "active%");
        Call prefilter = regexp("message", "fatal");
        Call expression = new Call(BOOLEAN, OR_FUNCTION_NAME, List.of(exact, prefilter));

        ElasticsearchPredicatePushdownPlanner.Result result = plan(expression, Map.of(
                "status", status,
                "message", message));

        assertThat(result.remainingConstraint().getExpression()).isEqualTo(TRUE);
        assertThat(result.residualExpressions()).containsExactly(expression);
        ElasticsearchRemotePredicate remotePredicate = result.remotePredicate().orElseThrow();
        assertThat(remotePredicate).isInstanceOf(ElasticsearchRemotePredicate.Or.class);
        assertThat(((ElasticsearchRemotePredicate.Or) remotePredicate).predicates())
                .containsExactly(
                        new ElasticsearchRemotePredicate.Prefix("status", "active"),
                        new ElasticsearchRemotePredicate.Enforced(
                                new ElasticsearchRemotePredicate.Regexp("message", ".*(fatal).*"),
                                PREFILTER));
    }

    @Test
    public void testNotBecomesPlannerOwnedResidualUntilNullSemanticsAreProven()
    {
        ArrayType arrayType = new ArrayType(INTEGER);
        ElasticsearchColumnHandle column = integerArrayColumn("Numbers");
        ConnectorExpression inner = contains("numbers", arrayType, 1L);
        Call expression = new Call(BOOLEAN, NOT_FUNCTION_NAME, List.of(inner));

        ElasticsearchPredicatePushdownPlanner.Result result = plan(expression, Map.of("numbers", column));

        assertThat(result.remotePredicate()).isEmpty();
        assertThat(result.remainingConstraint().getExpression()).isEqualTo(TRUE);
        assertThat(result.residualExpressions()).containsExactly(expression);
    }

    private static ElasticsearchPredicatePushdownPlanner.Result plan(
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments)
    {
        return ElasticsearchPredicatePushdownPlanner.plan(
                TestingConnectorSession.builder().build(),
                new Constraint(TupleDomain.all(), expression, assignments),
                SAFE);
    }

    private static Call contains(String variableName, ArrayType arrayType, long value)
    {
        return new Call(
                BOOLEAN,
                new FunctionName("contains"),
                List.of(
                        new Variable(variableName, arrayType),
                        new Constant(value, INTEGER)));
    }

    private static Call like(String variableName, String pattern)
    {
        return new Call(
                BOOLEAN,
                LIKE_FUNCTION_NAME,
                List.of(
                        new Variable(variableName, VARCHAR),
                        new Constant(utf8Slice(pattern), VARCHAR)));
    }

    private static Call regexp(String variableName, String pattern)
    {
        return new Call(
                BOOLEAN,
                new FunctionName("regexp_like"),
                List.of(
                        new Variable(variableName, VARCHAR),
                        new Constant(utf8Slice(pattern), VARCHAR)));
    }

    private static ElasticsearchColumnHandle integerArrayColumn(String remoteName)
    {
        return new ElasticsearchColumnHandle(
                List.of(remoteName),
                new ArrayType(INTEGER),
                new PrimitiveType("integer"),
                new IntegerDecoder.Descriptor(remoteName),
                false);
    }

    private static ElasticsearchColumnHandle keywordColumn(String remoteName)
    {
        return new ElasticsearchColumnHandle(
                List.of(remoteName),
                VARCHAR,
                new PrimitiveType("keyword"),
                new VarcharDecoder.Descriptor(remoteName),
                true);
    }

    private static ElasticsearchColumnHandle analyzedTextColumn(String remoteName)
    {
        return new ElasticsearchColumnHandle(
                List.of(remoteName),
                VARCHAR,
                new PrimitiveType("text"),
                new VarcharDecoder.Descriptor(remoteName),
                false);
    }
}
