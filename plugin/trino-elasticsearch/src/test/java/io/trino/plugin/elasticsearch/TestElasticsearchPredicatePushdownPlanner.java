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
import io.trino.plugin.elasticsearch.client.IndexMetadata.PrimitiveType;
import io.trino.plugin.elasticsearch.decoders.IntegerDecoder;
import io.trino.plugin.elasticsearch.decoders.VarcharDecoder;
import io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.elasticsearch.FullTextPushdownMode.SAFE;
import static io.trino.plugin.elasticsearch.FullTextPushdownMode.UNSAFE;
import static io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Enforcement.APPROXIMATE;
import static io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Enforcement.EXACT;
import static io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Enforcement.PREFILTER;
import static io.trino.spi.expression.Constant.TRUE;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestElasticsearchPredicatePushdownPlanner
{
    private static final ElasticsearchColumnHandle USER_ID = new ElasticsearchColumnHandle(
            ImmutableList.of("UserID"),
            INTEGER,
            new PrimitiveType("integer"),
            new IntegerDecoder.Descriptor("UserID"),
            true);

    @Test
    public void testExactDiscreteDomainMovesDirectlyToIr()
    {
        Constraint constraint = new Constraint(
                TupleDomain.withColumnDomains(Map.<ColumnHandle, Domain>of(USER_ID, Domain.multipleValues(INTEGER, List.of(1L, 2L, 3L)))),
                TRUE,
                Map.of());

        ElasticsearchPredicatePushdownPlanner.Result result = ElasticsearchPredicatePushdownPlanner.plan(
                TestingConnectorSession.builder().build(),
                constraint,
                SAFE);

        assertThat(result.remainingConstraint().getSummary().isAll()).isTrue();
        assertThat(result.residualFilter().isAll()).isTrue();
        ElasticsearchRemotePredicate predicate = result.remotePredicate().orElseThrow();
        assertThat(predicate).isEqualTo(new ElasticsearchRemotePredicate.Terms("UserID", List.of(1L, 2L, 3L)));
        assertThat(predicate.enforcement()).isEqualTo(EXACT);
    }

    @Test
    public void testExactRangeMovesDirectlyToIr()
    {
        Domain domain = Domain.create(
                ValueSet.ofRanges(Range.range(INTEGER, 10L, true, 20L, false)),
                false);
        Constraint constraint = new Constraint(
                TupleDomain.withColumnDomains(Map.<ColumnHandle, Domain>of(USER_ID, domain)),
                TRUE,
                Map.of());

        ElasticsearchPredicatePushdownPlanner.Result result = ElasticsearchPredicatePushdownPlanner.plan(
                TestingConnectorSession.builder().build(),
                constraint,
                SAFE);

        assertThat(result.remainingConstraint().getSummary().isAll()).isTrue();
        assertThat(result.remotePredicate()).contains(new ElasticsearchRemotePredicate.Range(
                "UserID",
                Optional.of(new ElasticsearchRemotePredicate.Bound(10L, true)),
                Optional.of(new ElasticsearchRemotePredicate.Bound(20L, false))));
    }

    @Test
    public void testExactLikePrefixMovesDirectlyToIr()
    {
        ElasticsearchColumnHandle column = keywordColumn();
        Constraint constraint = expressionConstraint(column, like("value", "Alpha%"));

        ElasticsearchPredicatePushdownPlanner.Result result = ElasticsearchPredicatePushdownPlanner.plan(
                TestingConnectorSession.builder().build(),
                constraint,
                SAFE);

        assertThat(result.remainingConstraint().getExpression()).isEqualTo(TRUE);
        assertThat(result.residualExpressions()).isEmpty();
        assertThat(result.remotePredicate()).contains(new ElasticsearchRemotePredicate.Prefix("value", "Alpha"));
    }

    @Test
    public void testStartsWithMovesDirectlyToIr()
    {
        ElasticsearchColumnHandle column = keywordColumn();
        Call startsWith = new Call(
                BOOLEAN,
                new FunctionName("starts_with"),
                ImmutableList.of(
                        new Variable("value", VARCHAR),
                        new Constant(utf8Slice("AbC"), VARCHAR)));

        ElasticsearchPredicatePushdownPlanner.Result result = ElasticsearchPredicatePushdownPlanner.plan(
                TestingConnectorSession.builder().build(),
                expressionConstraint(column, startsWith),
                SAFE);

        assertThat(result.remainingConstraint().getExpression()).isEqualTo(TRUE);
        assertThat(result.remotePredicate()).contains(new ElasticsearchRemotePredicate.Prefix("value", "AbC"));
    }

    @Test
    public void testSubstringPrefixMovesDirectlyToIr()
    {
        ElasticsearchColumnHandle column = keywordColumn();
        Call substring = new Call(
                VARCHAR,
                new FunctionName("substring"),
                ImmutableList.of(
                        new Variable("value", VARCHAR),
                        new Constant(1L, BIGINT),
                        new Constant(3L, BIGINT)));
        Call equals = new Call(
                BOOLEAN,
                EQUAL_OPERATOR_FUNCTION_NAME,
                ImmutableList.of(
                        substring,
                        new Constant(utf8Slice("AbC"), VARCHAR)));

        ElasticsearchPredicatePushdownPlanner.Result result = ElasticsearchPredicatePushdownPlanner.plan(
                TestingConnectorSession.builder().build(),
                expressionConstraint(column, equals),
                SAFE);

        assertThat(result.remainingConstraint().getExpression()).isEqualTo(TRUE);
        assertThat(result.remotePredicate()).contains(new ElasticsearchRemotePredicate.Prefix("value", "AbC"));
    }

    @Test
    public void testSafeAnalyzedDiscreteDomainKeepsResidual()
    {
        ElasticsearchColumnHandle column = analyzedTextColumn();
        Domain domain = Domain.singleValue(VARCHAR, utf8Slice("Alpha Beta"));
        Constraint constraint = new Constraint(
                TupleDomain.withColumnDomains(Map.<ColumnHandle, Domain>of(column, domain)),
                TRUE,
                Map.of());

        ElasticsearchPredicatePushdownPlanner.Result result = ElasticsearchPredicatePushdownPlanner.plan(
                TestingConnectorSession.builder().build(),
                constraint,
                SAFE);

        assertThat(result.remainingConstraint().getSummary().isAll()).isTrue();
        assertThat(result.residualFilter()).isEqualTo(TupleDomain.withColumnDomains(Map.<ColumnHandle, Domain>of(column, domain)));
        assertThat(result.remotePredicate()).contains(new ElasticsearchRemotePredicate.Enforced(
                new ElasticsearchRemotePredicate.MatchPhrase("value", "Alpha Beta"),
                PREFILTER));
    }

    @Test
    public void testUnsafeAnalyzedPrefixLikeRemovesSyntheticRange()
    {
        ElasticsearchColumnHandle column = analyzedTextColumn();
        Domain syntheticDomain = RuleBasedElasticsearchMetadata.createLikePrefixDomain(VARCHAR, utf8Slice("Alpha"))
                .orElseThrow();
        Constraint constraint = new Constraint(
                TupleDomain.withColumnDomains(Map.<ColumnHandle, Domain>of(column, syntheticDomain)),
                like("value", "Alpha%"),
                Map.of("value", column));

        ElasticsearchPredicatePushdownPlanner.Result result = ElasticsearchPredicatePushdownPlanner.plan(
                TestingConnectorSession.builder().build(),
                constraint,
                UNSAFE);

        assertThat(result.remainingConstraint().getSummary().isAll()).isTrue();
        assertThat(result.remainingConstraint().getExpression()).isEqualTo(TRUE);
        assertThat(result.residualFilter().isAll()).isTrue();
        assertThat(result.residualExpressions()).isEmpty();
        assertThat(result.remotePredicate()).contains(new ElasticsearchRemotePredicate.Enforced(
                new ElasticsearchRemotePredicate.MatchPhrasePrefix("value", "Alpha"),
                APPROXIMATE));
    }

    private static Constraint expressionConstraint(ElasticsearchColumnHandle column, Call expression)
    {
        return new Constraint(
                TupleDomain.all(),
                expression,
                Map.of("value", column));
    }

    private static Call like(String variableName, String pattern)
    {
        return new Call(
                BOOLEAN,
                LIKE_FUNCTION_NAME,
                ImmutableList.of(
                        new Variable(variableName, VARCHAR),
                        new Constant(utf8Slice(pattern), VARCHAR)));
    }

    private static ElasticsearchColumnHandle keywordColumn()
    {
        return new ElasticsearchColumnHandle(
                ImmutableList.of("value"),
                VARCHAR,
                new PrimitiveType("keyword"),
                new VarcharDecoder.Descriptor("value"),
                true);
    }

    private static ElasticsearchColumnHandle analyzedTextColumn()
    {
        return new ElasticsearchColumnHandle(
                ImmutableList.of("value"),
                VARCHAR,
                new PrimitiveType("text"),
                new VarcharDecoder.Descriptor("value"),
                false);
    }
}
