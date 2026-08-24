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

import io.trino.plugin.elasticsearch.ElasticsearchPredicateTranslation.Reason;
import io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.elasticsearch.ElasticsearchRemotePredicateSemantics.effectiveEnforcement;
import static io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Enforcement.APPROXIMATE;
import static io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Enforcement.EXACT;
import static io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Enforcement.PREFILTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestElasticsearchPredicateTranslation
{
    @Test
    public void testExactTranslationOwnsWholeSubtree()
    {
        ElasticsearchRemotePredicate predicate = new ElasticsearchRemotePredicate.Term("status", "active");

        ElasticsearchPredicateTranslation<String> translation = ElasticsearchPredicateTranslation.exact(
                predicate,
                Reason.EXACT_DOMAIN);

        assertThat(translation.remotePredicate()).contains(predicate);
        assertThat(translation.enforcement()).contains(EXACT);
        assertThat(translation.remaining()).isEmpty();
        assertThat(translation.residual()).isEmpty();
        assertThat(translation.decision().reason()).isEqualTo(Reason.EXACT_DOMAIN);
        assertThat(translation.decision().children()).isEmpty();
    }

    @Test
    public void testPrefilterRetainsConnectorOwnedResidual()
    {
        ElasticsearchRemotePredicate predicate = new ElasticsearchRemotePredicate.MatchPhrase("message", "fatal error");

        ElasticsearchPredicateTranslation<String> translation = ElasticsearchPredicateTranslation.prefilter(
                predicate,
                "message = 'fatal error'",
                Reason.FULL_TEXT_SAFE_PREFILTER);

        assertThat(translation.remotePredicate()).contains(new ElasticsearchRemotePredicate.Enforced(predicate, PREFILTER));
        assertThat(translation.enforcement()).contains(PREFILTER);
        assertThat(translation.remaining()).isEmpty();
        assertThat(translation.residual()).contains("message = 'fatal error'");
    }

    @Test
    public void testApproximateTranslationMaterializesEffectiveEnforcementInRemotePredicate()
    {
        ElasticsearchRemotePredicate predicate = new ElasticsearchRemotePredicate.MatchPhrase("message", "fatal error");

        ElasticsearchPredicateTranslation<String> translation = ElasticsearchPredicateTranslation.approximate(
                predicate,
                Reason.FULL_TEXT_UNSAFE_APPROXIMATE);

        assertThat(translation.remotePredicate()).contains(new ElasticsearchRemotePredicate.Enforced(predicate, APPROXIMATE));
        assertThat(translation.enforcement()).contains(APPROXIMATE);
        assertThat(translation.remaining()).isEmpty();
        assertThat(translation.residual()).isEmpty();
        assertThat(effectiveEnforcement(translation.remotePredicate().orElseThrow())).isEqualTo(translation.enforcement().orElseThrow());
    }

    @Test
    public void testUnsupportedAndOwnedResidualAreDifferentOutcomes()
    {
        ElasticsearchPredicateTranslation<String> unsupported = ElasticsearchPredicateTranslation.unsupported(
                "legacy-compatible-expression",
                Reason.UNSUPPORTED_EXPRESSION);
        ElasticsearchPredicateTranslation<String> residual = ElasticsearchPredicateTranslation.residual(
                "composer-owned-expression",
                Reason.BOOLEAN_OR);

        assertThat(unsupported.remotePredicate()).isEmpty();
        assertThat(unsupported.remaining()).contains("legacy-compatible-expression");
        assertThat(unsupported.residual()).isEmpty();

        assertThat(residual.remotePredicate()).isEmpty();
        assertThat(residual.remaining()).isEmpty();
        assertThat(residual.residual()).contains("composer-owned-expression");
    }

    @Test
    public void testCompositionPreservesDecisionTreeAndMaterializesRootEnforcement()
    {
        ElasticsearchRemotePredicate predicate = new ElasticsearchRemotePredicate.Term("status", "active");
        ElasticsearchPredicateTranslation<String> exact = ElasticsearchPredicateTranslation.exact(predicate, Reason.EXACT_DOMAIN);
        ElasticsearchPredicateTranslation<String> residual = ElasticsearchPredicateTranslation.residual(
                "score > custom_function()",
                Reason.UNSUPPORTED_EXPRESSION);

        ElasticsearchPredicateTranslation<String> translation = ElasticsearchPredicateTranslation.composed(
                Optional.of(predicate),
                Optional.of(PREFILTER),
                Optional.empty(),
                Optional.of("score > custom_function()"),
                Reason.BOOLEAN_AND,
                List.of(exact, residual));

        assertThat(translation.remotePredicate()).contains(new ElasticsearchRemotePredicate.Enforced(predicate, PREFILTER));
        assertThat(translation.enforcement()).contains(PREFILTER);
        assertThat(translation.decision().reason()).isEqualTo(Reason.BOOLEAN_AND);
        assertThat(translation.decision().children())
                .extracting(ElasticsearchPredicateTranslation.Decision::reason)
                .containsExactly(Reason.EXACT_DOMAIN, Reason.UNSUPPORTED_EXPRESSION);
    }

    @Test
    public void testRemotePredicateRequiresEnforcement()
    {
        ElasticsearchRemotePredicate predicate = new ElasticsearchRemotePredicate.Term("status", "active");

        assertThatThrownBy(() -> new ElasticsearchPredicateTranslation<>(
                Optional.of(predicate),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Reason.EXACT_DOMAIN))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("remote predicate and enforcement");
    }

    @Test
    public void testDeclaredEnforcementMustMatchRemotePredicateIr()
    {
        ElasticsearchRemotePredicate predicate = new ElasticsearchRemotePredicate.Term("status", "active");

        assertThatThrownBy(() -> new ElasticsearchPredicateTranslation<>(
                Optional.of(predicate),
                Optional.of(PREFILTER),
                Optional.empty(),
                Optional.of("status = 'active'"),
                Reason.FULL_TEXT_SAFE_PREFILTER))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("declared enforcement does not match Remote Predicate IR enforcement");
    }

    @Test
    public void testExactCannotHaveRemainingOrResidualState()
    {
        ElasticsearchRemotePredicate predicate = new ElasticsearchRemotePredicate.Term("status", "active");

        assertThatThrownBy(() -> new ElasticsearchPredicateTranslation<>(
                Optional.of(predicate),
                Optional.of(EXACT),
                Optional.of("remaining"),
                Optional.empty(),
                Reason.EXACT_DOMAIN))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("EXACT translation cannot have remaining or residual state");

        assertThatThrownBy(() -> new ElasticsearchPredicateTranslation<>(
                Optional.of(predicate),
                Optional.of(EXACT),
                Optional.empty(),
                Optional.of("residual"),
                Reason.EXACT_DOMAIN))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("EXACT translation cannot have remaining or residual state");
    }

    @Test
    public void testPrefilterRequiresRemainingOrResidualState()
    {
        ElasticsearchRemotePredicate predicate = new ElasticsearchRemotePredicate.Enforced(
                new ElasticsearchRemotePredicate.MatchPhrase("message", "fatal"),
                PREFILTER);

        assertThatThrownBy(() -> new ElasticsearchPredicateTranslation<>(
                Optional.of(predicate),
                Optional.of(PREFILTER),
                Optional.empty(),
                Optional.empty(),
                Reason.FULL_TEXT_SAFE_PREFILTER))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("PREFILTER translation requires remaining or residual state");
    }
}
