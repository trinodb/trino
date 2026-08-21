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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.expression.ConnectorExpressions;
import io.trino.plugin.elasticsearch.client.IndexMetadata;
import io.trino.plugin.elasticsearch.decoders.VarcharDecoder;
import io.trino.spi.connector.Constraint;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.expression.Constant.TRUE;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRuleBasedElasticsearchMetadata
{
    @Test
    public void testUnsafeContainsLikeOnAnalyzedTextBecomesMatchPhraseDomain()
    {
        ElasticsearchColumnHandle column = analyzedTextColumn();
        Constraint constraint = likeConstraint(column, "%ngô%");

        Constraint rewritten = RuleBasedElasticsearchMetadata.rewriteUnsafeFullTextConstraint(
                TestingConnectorSession.builder().build(),
                constraint);

        assertThat(rewritten.getExpression()).isEqualTo(TRUE);
        assertThat(rewritten.getSummary()).isEqualTo(TupleDomain.withColumnDomains(
                Map.of(column, Domain.singleValue(VARCHAR, utf8Slice("ngô")))));
    }

    @Test
    public void testUnsafeLiteralLikeOnAnalyzedTextBecomesMatchPhraseDomain()
    {
        ElasticsearchColumnHandle column = analyzedTextColumn();
        Constraint constraint = likeConstraint(column, "ngô");

        Constraint rewritten = RuleBasedElasticsearchMetadata.rewriteUnsafeFullTextConstraint(
                TestingConnectorSession.builder().build(),
                constraint);

        assertThat(rewritten.getExpression()).isEqualTo(TRUE);
        assertThat(rewritten.getSummary()).isEqualTo(TupleDomain.withColumnDomains(
                Map.of(column, Domain.singleValue(VARCHAR, utf8Slice("ngô")))));
    }

    @Test
    public void testUnsafeMultiTokenContainsLikeBecomesMatchPhraseDomain()
    {
        ElasticsearchColumnHandle column = analyzedTextColumn();
        Constraint constraint = likeConstraint(column, "%ngô văn%");

        Constraint rewritten = RuleBasedElasticsearchMetadata.rewriteUnsafeFullTextConstraint(
                TestingConnectorSession.builder().build(),
                constraint);

        assertThat(rewritten.getExpression()).isEqualTo(TRUE);
        assertThat(rewritten.getSummary()).isEqualTo(TupleDomain.withColumnDomains(
                Map.of(column, Domain.singleValue(VARCHAR, utf8Slice("ngô văn")))));
    }

    @Test
    public void testUnsafeMultiTokenLiteralLikeBecomesMatchPhraseDomain()
    {
        ElasticsearchColumnHandle column = analyzedTextColumn();
        Constraint constraint = likeConstraint(column, "ngô văn");

        Constraint rewritten = RuleBasedElasticsearchMetadata.rewriteUnsafeFullTextConstraint(
                TestingConnectorSession.builder().build(),
                constraint);

        assertThat(rewritten.getExpression()).isEqualTo(TRUE);
        assertThat(rewritten.getSummary()).isEqualTo(TupleDomain.withColumnDomains(
                Map.of(column, Domain.singleValue(VARCHAR, utf8Slice("ngô văn")))));
    }

    @Test
    public void testPrefixLikeKeepsLegacyMatchPhrasePrefixPath()
    {
        ElasticsearchColumnHandle column = analyzedTextColumn();
        Constraint constraint = likeConstraint(column, "ngô văn%");

        assertThat(RuleBasedElasticsearchMetadata.rewriteUnsafeFullTextConstraint(
                TestingConnectorSession.builder().build(),
                constraint))
                .isSameAs(constraint);
    }

    @Test
    public void testSyntheticPrefixLikeDomainIsRemoved()
    {
        ElasticsearchColumnHandle column = analyzedTextColumn();
        Call like = likeCall("value", "ngô văn%");
        Domain syntheticLikeDomain = RuleBasedElasticsearchMetadata.createLikePrefixDomain(VARCHAR, utf8Slice("ngô văn"))
                .orElseThrow();
        Constraint constraint = new Constraint(
                TupleDomain.withColumnDomains(Map.of(column, syntheticLikeDomain)),
                like,
                Map.of("value", column));

        Constraint rewritten = RuleBasedElasticsearchMetadata.rewriteUnsafeFullTextConstraint(
                TestingConnectorSession.builder().build(),
                constraint);

        assertThat(rewritten.getSummary()).isEqualTo(TupleDomain.all());
        assertThat(rewritten.getExpression()).isEqualTo(like);
    }

    @Test
    public void testNonAsciiPrefixDoesNotHaveSyntheticDomain()
    {
        // DomainTranslator deliberately does not create a prefix range when there is no ASCII code point it can
        // increment without changing the UTF-8 byte length. The connector must mirror that rule exactly.
        assertThat(RuleBasedElasticsearchMetadata.createLikePrefixDomain(VARCHAR, utf8Slice("中文")))
                .isEmpty();
    }

    @Test
    public void testDifferentPrefixDomainIsPreserved()
    {
        ElasticsearchColumnHandle column = analyzedTextColumn();
        Call like = likeCall("value", "ngô văn%");
        Constraint constraint = new Constraint(
                TupleDomain.withColumnDomains(Map.of(column, Domain.singleValue(VARCHAR, utf8Slice("ngô văn x")))),
                like,
                Map.of("value", column));

        assertThat(RuleBasedElasticsearchMetadata.rewriteUnsafeFullTextConstraint(
                TestingConnectorSession.builder().build(),
                constraint))
                .isSameAs(constraint);
    }

    @Test
    public void testSyntheticPrefixDomainIsPreservedWhenAnotherPredicateReferencesColumn()
    {
        ElasticsearchColumnHandle column = analyzedTextColumn();
        Domain syntheticLikeDomain = RuleBasedElasticsearchMetadata.createLikePrefixDomain(VARCHAR, utf8Slice("abc"))
                .orElseThrow();
        Constraint constraint = new Constraint(
                TupleDomain.withColumnDomains(Map.of(column, syntheticLikeDomain)),
                ConnectorExpressions.and(ImmutableList.of(
                        likeCall("value", "abc%"),
                        testPredicate("value"))),
                Map.of("value", column));

        // Even when the combined TupleDomain is indistinguishable from the LIKE-derived range, another conjunct on
        // the same column may carry independent SQL semantics. Preserve the range instead of guessing its provenance.
        assertThat(RuleBasedElasticsearchMetadata.rewriteUnsafeFullTextConstraint(
                TestingConnectorSession.builder().build(),
                constraint))
                .isSameAs(constraint);
    }

    @Test
    public void testComplexWildcardKeepsLegacyPath()
    {
        ElasticsearchColumnHandle column = analyzedTextColumn();
        Constraint constraint = likeConstraint(column, "%ng_%");

        assertThat(RuleBasedElasticsearchMetadata.rewriteUnsafeFullTextConstraint(
                TestingConnectorSession.builder().build(),
                constraint))
                .isSameAs(constraint);
    }

    @Test
    public void testKeywordFieldKeepsExactLikePath()
    {
        ElasticsearchColumnHandle column = new ElasticsearchColumnHandle(
                ImmutableList.of("value"),
                VARCHAR,
                new IndexMetadata.PrimitiveType("keyword"),
                new VarcharDecoder.Descriptor("value"),
                true);
        Constraint constraint = likeConstraint(column, "%ngô%");

        assertThat(RuleBasedElasticsearchMetadata.rewriteUnsafeFullTextConstraint(
                TestingConnectorSession.builder().build(),
                constraint))
                .isSameAs(constraint);
    }

    @Test
    public void testTextWithKeywordSubfieldKeepsExactLikePath()
    {
        ElasticsearchColumnHandle column = new ElasticsearchColumnHandle(
                ImmutableList.of("value"),
                VARCHAR,
                new IndexMetadata.PrimitiveType("text", Optional.of("keyword")),
                new VarcharDecoder.Descriptor("value"),
                true);
        Constraint constraint = likeConstraint(column, "%ngô%");

        assertThat(RuleBasedElasticsearchMetadata.rewriteUnsafeFullTextConstraint(
                TestingConnectorSession.builder().build(),
                constraint))
                .isSameAs(constraint);
    }

    @Test
    public void testExistingDomainOnSameColumnIsNotCollapsedByTemporaryDomainLowering()
    {
        ElasticsearchColumnHandle column = analyzedTextColumn();
        Call like = likeCall("value", "%ngô%");
        Constraint constraint = new Constraint(
                TupleDomain.withColumnDomains(Map.of(column, Domain.singleValue(VARCHAR, utf8Slice("other")))),
                like,
                Map.of("value", column));

        // The current lowering bridge stores MATCH_PHRASE in TupleDomain, which can represent only one domain per
        // column. Until the table handle carries a list of remote predicates, preserve both conjuncts rather than
        // corrupting A AND B into one synthetic equality domain.
        assertThat(RuleBasedElasticsearchMetadata.rewriteUnsafeFullTextConstraint(
                TestingConnectorSession.builder().build(),
                constraint))
                .isSameAs(constraint);
    }

    @Test
    public void testMultipleLikePredicatesOnSameColumnAreNotCollapsedByTemporaryDomainLowering()
    {
        ElasticsearchColumnHandle column = analyzedTextColumn();
        Constraint constraint = new Constraint(
                TupleDomain.all(),
                ConnectorExpressions.and(ImmutableList.of(
                        likeCall("value", "%ngô%"),
                        likeCall("value", "%văn%"))),
                Map.of("value", column));

        // This is a temporary representation limitation, not the desired UNSAFE policy. A future remote-predicate IR
        // should lower these to two match_phrase clauses under bool.must and fully push both conjuncts.
        assertThat(RuleBasedElasticsearchMetadata.rewriteUnsafeFullTextConstraint(
                TestingConnectorSession.builder().build(),
                constraint))
                .isSameAs(constraint);
    }

    private static Constraint likeConstraint(ElasticsearchColumnHandle column, String pattern)
    {
        return new Constraint(
                TupleDomain.all(),
                likeCall("value", pattern),
                Map.of("value", column));
    }

    private static Call likeCall(String variableName, String pattern)
    {
        return new Call(
                BOOLEAN,
                LIKE_FUNCTION_NAME,
                ImmutableList.of(
                        new Variable(variableName, VARCHAR),
                        new Constant(utf8Slice(pattern), VARCHAR)));
    }

    private static Call testPredicate(String variableName)
    {
        return new Call(
                BOOLEAN,
                new FunctionName("test_predicate"),
                ImmutableList.of(new Variable(variableName, VARCHAR)));
    }

    private static ElasticsearchColumnHandle analyzedTextColumn()
    {
        return new ElasticsearchColumnHandle(
                ImmutableList.of("value"),
                VARCHAR,
                new IndexMetadata.PrimitiveType("text"),
                new VarcharDecoder.Descriptor("value"),
                false);
    }
}
