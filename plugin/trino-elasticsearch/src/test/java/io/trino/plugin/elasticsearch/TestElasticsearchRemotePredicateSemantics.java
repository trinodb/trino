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

import static io.trino.plugin.elasticsearch.ElasticsearchRemotePredicateSemantics.effectiveEnforcement;
import static io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Enforcement.APPROXIMATE;
import static io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Enforcement.EXACT;
import static io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Enforcement.PREFILTER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestElasticsearchRemotePredicateSemantics
{
    @Test
    public void testExactTreeIsExact()
    {
        ElasticsearchRemotePredicate predicate = new ElasticsearchRemotePredicate.And(List.of(
                new ElasticsearchRemotePredicate.Term("status", "active"),
                new ElasticsearchRemotePredicate.Range(
                        "score",
                        Optional.of(new ElasticsearchRemotePredicate.Bound(10L, true)),
                        Optional.empty())));

        assertThat(effectiveEnforcement(predicate)).isEqualTo(EXACT);
    }

    @Test
    public void testPrefilterPropagatesThroughBooleanTree()
    {
        ElasticsearchRemotePredicate predicate = new ElasticsearchRemotePredicate.Or(List.of(
                new ElasticsearchRemotePredicate.Term("status", "active"),
                new ElasticsearchRemotePredicate.Enforced(
                        new ElasticsearchRemotePredicate.Regexp("message", ".*fatal.*"),
                        PREFILTER)));

        assertThat(effectiveEnforcement(predicate)).isEqualTo(PREFILTER);
    }

    @Test
    public void testApproximateDominatesPrefilter()
    {
        ElasticsearchRemotePredicate predicate = new ElasticsearchRemotePredicate.And(List.of(
                new ElasticsearchRemotePredicate.Enforced(
                        new ElasticsearchRemotePredicate.Regexp("message", ".*fatal.*"),
                        PREFILTER),
                new ElasticsearchRemotePredicate.Enforced(
                        new ElasticsearchRemotePredicate.MatchPhrase("description", "fatal"),
                        APPROXIMATE)));

        assertThat(effectiveEnforcement(predicate)).isEqualTo(APPROXIMATE);
    }
}
