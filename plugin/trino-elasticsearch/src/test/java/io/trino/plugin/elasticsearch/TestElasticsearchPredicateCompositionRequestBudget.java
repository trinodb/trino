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

public class TestElasticsearchPredicateCompositionRequestBudget
{
    @Test
    public void testOversizedOrFallsBackToOwnedResidualInsteadOfSendingRemoteRequest()
    {
        ConnectorExpression left = new Variable("left", BOOLEAN);
        ConnectorExpression right = new Variable("right", BOOLEAN);
        ConnectorExpression source = ConnectorExpressions.or(List.of(left, right));
        String largeValue = "x".repeat(1_000);

        ElasticsearchPredicateTranslation<ConnectorExpression> result = ElasticsearchPredicateComposer.or(
                source,
                List.of(
                        ElasticsearchPredicateTranslation.exact(
                                new ElasticsearchRemotePredicate.Term("status", largeValue),
                                Reason.EXACT_DOMAIN),
                        ElasticsearchPredicateTranslation.exact(
                                new ElasticsearchRemotePredicate.Term("status", "small"),
                                Reason.EXACT_DOMAIN)),
                new ElasticsearchPredicateCompositionPolicy(10, 10, 10, 128));

        assertThat(result.remotePredicate()).isEmpty();
        assertThat(result.remaining()).isEmpty();
        assertThat(result.residual()).contains(source);
    }
}
