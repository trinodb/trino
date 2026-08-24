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
import io.trino.plugin.elasticsearch.client.IndexMetadata;
import io.trino.plugin.elasticsearch.decoders.VarcharDecoder;
import io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.elasticsearch.FullTextPushdownMode.UNSAFE;
import static io.trino.plugin.elasticsearch.expression.ElasticsearchRemotePredicate.Enforcement.APPROXIMATE;
import static io.trino.spi.expression.Constant.TRUE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestElasticsearchRemoteColumnCase
{
    @Test
    public void testLogicalColumnNameKeepsOriginalRemotePath()
    {
        ElasticsearchColumnHandle remoteColumn = remoteTextColumn();

        Map<String, ColumnHandle> handles = CasePreservingElasticsearchMetadata.normalizeColumnHandles(Map.of("Ho_ten", remoteColumn));

        assertThat(handles).containsOnlyKeys("ho_ten");
        assertThat(handles.get("ho_ten")).isSameAs(remoteColumn);
        assertThat(((ElasticsearchColumnHandle) handles.get("ho_ten")).name()).isEqualTo("Ho_ten");
    }

    @Test
    public void testUnsafeFullTextPredicateIrUsesOriginalRemoteFieldCase()
    {
        ElasticsearchColumnHandle remoteColumn = remoteTextColumn();
        Domain domain = Domain.singleValue(VARCHAR, utf8Slice("sa"));
        Constraint constraint = new Constraint(
                TupleDomain.withColumnDomains(Map.<ColumnHandle, Domain>of(remoteColumn, domain)),
                TRUE,
                Map.of());

        ElasticsearchPredicatePushdownPlanner.Result result = ElasticsearchPredicatePushdownPlanner.plan(
                TestingConnectorSession.builder().build(),
                constraint,
                UNSAFE);

        assertThat(result.remainingConstraint().getSummary().isAll()).isTrue();
        assertThat(result.residualFilter().isAll()).isTrue();
        assertThat(result.remotePredicate()).contains(new ElasticsearchRemotePredicate.Enforced(
                new ElasticsearchRemotePredicate.MatchPhrase("Ho_ten", "sa"),
                APPROXIMATE));
    }

    private static ElasticsearchColumnHandle remoteTextColumn()
    {
        return new ElasticsearchColumnHandle(
                ImmutableList.of("Ho_ten"),
                VARCHAR,
                new IndexMetadata.PrimitiveType("text"),
                new VarcharDecoder.Descriptor("Ho_ten"),
                false);
    }
}
