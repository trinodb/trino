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

import io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Enforcement.PREFILTER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestElasticsearchRemotePredicateNormalizer
{
    @Test
    public void testNestedAndIsFlattenedAndDeduplicated()
    {
        ElasticsearchRemotePredicate first = new ElasticsearchRemotePredicate.Term("status", "active");
        ElasticsearchRemotePredicate second = new ElasticsearchRemotePredicate.Term("tenant", "blue");

        assertThat(ElasticsearchRemotePredicateNormalizer.and(List.of(
                first,
                new ElasticsearchRemotePredicate.And(List.of(second, first)))))
                .contains(new ElasticsearchRemotePredicate.And(List.of(first, second)));
    }

    @Test
    public void testNestedOrIsFlattenedAndDeduplicated()
    {
        ElasticsearchRemotePredicate first = new ElasticsearchRemotePredicate.Term("status", "active");
        ElasticsearchRemotePredicate second = new ElasticsearchRemotePredicate.Term("status", "pending");

        assertThat(ElasticsearchRemotePredicateNormalizer.or(List.of(
                first,
                new ElasticsearchRemotePredicate.Or(List.of(second, first)))))
                .contains(new ElasticsearchRemotePredicate.Or(List.of(first, second)));
    }

    @Test
    public void testSingleChildBooleanNodesCollapse()
    {
        ElasticsearchRemotePredicate predicate = new ElasticsearchRemotePredicate.Term("status", "active");

        assertThat(ElasticsearchRemotePredicateNormalizer.normalize(new ElasticsearchRemotePredicate.And(List.of(predicate))))
                .isEqualTo(predicate);
        assertThat(ElasticsearchRemotePredicateNormalizer.normalize(new ElasticsearchRemotePredicate.Or(List.of(predicate))))
                .isEqualTo(predicate);
    }

    @Test
    public void testIndependentSameFieldRangesAreNotFusedAtDocumentScope()
    {
        ElasticsearchRemotePredicate.Range greaterThanTen = range("numbers", 10L, false, null, false);
        ElasticsearchRemotePredicate.Range lessThanTwenty = range("numbers", null, false, 20L, false);

        // For a multivalued Elasticsearch field, [5, 25] satisfies these two independent clauses using different
        // values. Fusing them to one 10 < x < 20 Range would introduce a false negative. The general IR normalizer has
        // no same-value proof and must preserve the document-scope conjunction.
        assertThat(ElasticsearchRemotePredicateNormalizer.and(List.of(greaterThanTen, lessThanTwenty)))
                .contains(new ElasticsearchRemotePredicate.And(List.of(greaterThanTen, lessThanTwenty)));
    }

    @Test
    public void testDuplicateRangeCanBeRemovedWithoutChangingScope()
    {
        ElasticsearchRemotePredicate.Range range = range("score", 10L, true, 20L, false);

        assertThat(ElasticsearchRemotePredicateNormalizer.and(List.of(range, range)))
                .contains(range);
    }

    @Test
    public void testEnforcedPredicateKeepsEnforcementWhileNormalizingInnerTree()
    {
        ElasticsearchRemotePredicate first = new ElasticsearchRemotePredicate.Term("status", "active");
        ElasticsearchRemotePredicate.Enforced input = new ElasticsearchRemotePredicate.Enforced(
                new ElasticsearchRemotePredicate.And(List.of(first, first)),
                PREFILTER);

        assertThat(ElasticsearchRemotePredicateNormalizer.normalize(input))
                .isEqualTo(new ElasticsearchRemotePredicate.Enforced(first, PREFILTER));
    }

    @Test
    public void testNotNormalizesItsChildWithoutChangingNegationScope()
    {
        ElasticsearchRemotePredicate first = new ElasticsearchRemotePredicate.Term("status", "active");
        ElasticsearchRemotePredicate input = new ElasticsearchRemotePredicate.Not(
                new ElasticsearchRemotePredicate.Or(List.of(first, first)));

        assertThat(ElasticsearchRemotePredicateNormalizer.normalize(input))
                .isEqualTo(new ElasticsearchRemotePredicate.Not(first));
    }

    private static ElasticsearchRemotePredicate.Range range(
            String field,
            Object lower,
            boolean lowerInclusive,
            Object upper,
            boolean upperInclusive)
    {
        return new ElasticsearchRemotePredicate.Range(
                field,
                Optional.ofNullable(lower).map(value -> new ElasticsearchRemotePredicate.Bound(value, lowerInclusive)),
                Optional.ofNullable(upper).map(value -> new ElasticsearchRemotePredicate.Bound(value, upperInclusive)));
    }
}
