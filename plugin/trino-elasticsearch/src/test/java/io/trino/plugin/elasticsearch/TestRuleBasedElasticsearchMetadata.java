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

import io.trino.plugin.elasticsearch.client.ElasticsearchClient;
import io.trino.plugin.elasticsearch.client.IndexMetadata;
import io.trino.plugin.elasticsearch.decoders.IntegerDecoder;
import io.trino.plugin.elasticsearch.decoders.VarcharDecoder;
import io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.elasticsearch.ElasticsearchRemotePredicateTranslator.withRemotePredicate;
import static io.trino.plugin.elasticsearch.ElasticsearchTableHandle.Type.SCAN;
import static io.trino.plugin.elasticsearch.FullTextPushdownMode.SAFE;
import static io.trino.spi.expression.Constant.TRUE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runtime orchestration tests for the rule-based metadata facade.
 *
 * <p>Predicate recognition and boolean semantics are tested at the permanent planner/composer abstractions. This suite
 * intentionally does not test the retired synthetic-domain lowering design.</p>
 */
public class TestRuleBasedElasticsearchMetadata
{
    private static final ElasticsearchColumnHandle USER_ID = new ElasticsearchColumnHandle(
            List.of("UserID"),
            INTEGER,
            new IndexMetadata.PrimitiveType("integer"),
            new IntegerDecoder.Descriptor("UserID"),
            true);

    private static ElasticsearchClient client;
    private static RuleBasedElasticsearchMetadata metadata;
    private static ConnectorSession session;

    @BeforeAll
    public static void setUpMetadata()
    {
        ElasticsearchConfig config = new ElasticsearchConfig()
                .setHosts(List.of("localhost"))
                .setFullTextPushdownMode(SAFE);
        client = new ElasticsearchClient(config, Optional.empty(), Optional.empty());
        metadata = new RuleBasedElasticsearchMetadata(TESTING_TYPE_MANAGER, client, config);
        session = TestingConnectorSession.builder()
                .setPropertyMetadata(new ElasticsearchSessionProperties(config).getSessionProperties())
                .build();
    }

    @AfterAll
    public static void closeClient()
            throws IOException
    {
        client.close();
    }

    @Test
    public void testApplyingSameExactPredicateTwiceDoesNotCreateState()
    {
        Constraint constraint = exactConstraint(10L);

        ConstraintApplicationResult<ConnectorTableHandle> first = metadata.applyFilter(session, emptyTable(), constraint)
                .orElseThrow();
        ElasticsearchTableHandle pushed = (ElasticsearchTableHandle) first.getHandle();

        assertThat(pushed.remotePredicate()).contains(new ElasticsearchRemotePredicate.Term("UserID", 10L));
        assertThat(metadata.applyFilter(session, pushed, constraint)).isEmpty();
    }

    @Test
    public void testSameRemotePredicateIsNotAppended()
    {
        ElasticsearchRemotePredicate predicate = new ElasticsearchRemotePredicate.Term("UserID", 10L);
        ElasticsearchTableHandle input = withRemotePredicate(emptyTable(), Optional.of(predicate));

        assertThat(metadata.applyFilter(session, input, exactConstraint(10L))).isEmpty();
        assertThat(input.remotePredicate()).contains(predicate);
    }

    @Test
    public void testRepeatedApplyFilterReachesFixedPoint()
    {
        ElasticsearchTableHandle first = (ElasticsearchTableHandle) metadata.applyFilter(session, emptyTable(), exactConstraint(10L))
                .orElseThrow()
                .getHandle();
        ElasticsearchTableHandle second = (ElasticsearchTableHandle) metadata.applyFilter(session, first, exactConstraint(20L))
                .orElseThrow()
                .getHandle();

        assertThat(second.remotePredicate()).contains(new ElasticsearchRemotePredicate.And(List.of(
                new ElasticsearchRemotePredicate.Term("UserID", 10L),
                new ElasticsearchRemotePredicate.Term("UserID", 20L))));
        assertThat(metadata.applyFilter(session, second, exactConstraint(10L))).isEmpty();
        assertThat(metadata.applyFilter(session, second, exactConstraint(20L))).isEmpty();
    }

    @Test
    public void testRepeatedApplyFilterPreservesIndependentDocumentScopeRanges()
    {
        ElasticsearchTableHandle lower = (ElasticsearchTableHandle) metadata.applyFilter(
                        session,
                        emptyTable(),
                        rangeConstraint(Range.greaterThan(INTEGER, 10L)))
                .orElseThrow()
                .getHandle();
        ElasticsearchTableHandle bounded = (ElasticsearchTableHandle) metadata.applyFilter(
                        session,
                        lower,
                        rangeConstraint(Range.lessThan(INTEGER, 20L)))
                .orElseThrow()
                .getHandle();

        assertThat(bounded.remotePredicate()).contains(new ElasticsearchRemotePredicate.And(List.of(
                new ElasticsearchRemotePredicate.Range(
                        "UserID",
                        Optional.of(new ElasticsearchRemotePredicate.Bound(10L, false)),
                        Optional.empty()),
                new ElasticsearchRemotePredicate.Range(
                        "UserID",
                        Optional.empty(),
                        Optional.of(new ElasticsearchRemotePredicate.Bound(20L, false))))));
    }

    @Test
    public void testSafeAnalyzedDomainCannotBypassPlannerThroughLegacyMetadata()
    {
        ElasticsearchColumnHandle column = analyzedTextColumn();
        Domain domain = Domain.singleValue(VARCHAR, utf8Slice("fatal error"));
        Constraint constraint = new Constraint(
                TupleDomain.withColumnDomains(Map.<ColumnHandle, Domain>of(column, domain)),
                TRUE,
                Map.of());

        assertThat(metadata.applyFilter(session, emptyTable(), constraint)).isEmpty();
    }

    private static Constraint exactConstraint(long value)
    {
        return new Constraint(
                TupleDomain.withColumnDomains(Map.<ColumnHandle, Domain>of(USER_ID, Domain.singleValue(INTEGER, value))),
                TRUE,
                Map.of());
    }

    private static Constraint rangeConstraint(Range range)
    {
        return new Constraint(
                TupleDomain.withColumnDomains(Map.<ColumnHandle, Domain>of(
                        USER_ID, Domain.create(ValueSet.ofRanges(range), false))),
                TRUE,
                Map.of());
    }

    private static ElasticsearchTableHandle emptyTable()
    {
        return new ElasticsearchTableHandle(SCAN, "default", "events", Optional.empty());
    }

    private static ElasticsearchColumnHandle analyzedTextColumn()
    {
        return new ElasticsearchColumnHandle(
                List.of("value"),
                VARCHAR,
                new IndexMetadata.PrimitiveType("text"),
                new VarcharDecoder.Descriptor("value"),
                false);
    }
}
