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

import io.trino.plugin.base.expression.ConnectorExpressions;
import io.trino.plugin.elasticsearch.ElasticsearchPredicateTranslation.Reason;
import io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static org.assertj.core.api.Assertions.assertThat;

public class TestElasticsearchPredicateCompositionPolicy
{
    private static final ConnectorExpression A = new Variable("a", BOOLEAN);
    private static final ConnectorExpression B = new Variable("b", BOOLEAN);
    private static final ConnectorExpression C = new Variable("c", BOOLEAN);
    private static final int LARGE_QUERY_BUDGET = 1_048_576;

    @Test
    public void testExactTermsAreBatchedWithoutChangingSemanticNormalizer()
    {
        List<ElasticsearchPredicateTranslation<ConnectorExpression>> children = List.of(
                exact("a"),
                exact("b"),
                exact("c"));

        ElasticsearchPredicateTranslation<ConnectorExpression> result = ElasticsearchPredicateComposer.or(
                ConnectorExpressions.or(List.of(A, B, C)),
                children,
                new ElasticsearchPredicateCompositionPolicy(10, 2, 10, LARGE_QUERY_BUDGET));

        assertThat(result.remotePredicate()).contains(new ElasticsearchRemotePredicate.Or(List.of(
                new ElasticsearchRemotePredicate.Terms("status", List.of("a", "b")),
                new ElasticsearchRemotePredicate.Term("status", "c"))));
        assertThat(result.remaining()).isEmpty();
        assertThat(result.residual()).isEmpty();
    }

    @Test
    public void testTermsValueBudgetFallsBackToOwnedResidual()
    {
        ConnectorExpression source = ConnectorExpressions.or(List.of(A, B, C));
        List<ElasticsearchPredicateTranslation<ConnectorExpression>> children = List.of(
                exact("a"),
                exact("b"),
                exact("c"));

        ElasticsearchPredicateTranslation<ConnectorExpression> result = ElasticsearchPredicateComposer.or(
                source,
                children,
                new ElasticsearchPredicateCompositionPolicy(2, 2, 10, LARGE_QUERY_BUDGET));

        assertThat(result.remotePredicate()).isEmpty();
        assertThat(result.remaining()).isEmpty();
        assertThat(result.residual()).contains(source);
    }

    @Test
    public void testBooleanClauseBudgetFallsBackToOwnedResidual()
    {
        ConnectorExpression source = ConnectorExpressions.or(List.of(A, B, C));
        List<ElasticsearchPredicateTranslation<ConnectorExpression>> children = List.of(
                ElasticsearchPredicateTranslation.exact(new ElasticsearchRemotePredicate.Term("a", 1L), Reason.EXACT_DOMAIN),
                ElasticsearchPredicateTranslation.exact(new ElasticsearchRemotePredicate.Term("b", 1L), Reason.EXACT_DOMAIN),
                ElasticsearchPredicateTranslation.exact(new ElasticsearchRemotePredicate.Term("c", 1L), Reason.EXACT_DOMAIN));

        ElasticsearchPredicateTranslation<ConnectorExpression> result = ElasticsearchPredicateComposer.or(
                source,
                children,
                new ElasticsearchPredicateCompositionPolicy(10, 2, 2, LARGE_QUERY_BUDGET));

        assertThat(result.remotePredicate()).isEmpty();
        assertThat(result.remaining()).isEmpty();
        assertThat(result.residual()).contains(source);
    }

    @Test
    public void testSemanticDisjunctionDoesNotUndoDynamicFilterBatches()
    {
        ElasticsearchRemotePredicate first = new ElasticsearchRemotePredicate.Terms("key", List.of(1L, 2L));
        ElasticsearchRemotePredicate second = new ElasticsearchRemotePredicate.Terms("key", List.of(3L, 4L));

        assertThat(ElasticsearchRemotePredicateNormalizer.or(List.of(first, second)))
                .contains(new ElasticsearchRemotePredicate.Or(List.of(first, second)));
    }

    private static ElasticsearchPredicateTranslation<ConnectorExpression> exact(String value)
    {
        return ElasticsearchPredicateTranslation.exact(
                new ElasticsearchRemotePredicate.Term("status", value),
                Reason.EXACT_DOMAIN);
    }
}
