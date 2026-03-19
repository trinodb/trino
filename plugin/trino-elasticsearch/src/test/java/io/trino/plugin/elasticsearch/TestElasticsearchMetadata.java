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

import io.airlift.slice.Slices;
import io.trino.plugin.elasticsearch.aggregation.MetricAggregation;
import io.trino.plugin.elasticsearch.aggregation.TermAggregation;
import io.trino.plugin.elasticsearch.client.ElasticsearchClient;
import io.trino.plugin.elasticsearch.client.IndexMetadata;
import io.trino.plugin.elasticsearch.decoders.BigintDecoder;
import io.trino.plugin.elasticsearch.decoders.VarcharDecoder;
import io.trino.plugin.elasticsearch.expression.TopN;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.elasticsearch.ElasticsearchTableHandle.Type.AGGREGATION;
import static io.trino.plugin.elasticsearch.ElasticsearchTableHandle.Type.SCAN;
import static io.trino.plugin.elasticsearch.expression.TopN.TopNSortItem.DEFAULT_SORT_BY_DOC;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestElasticsearchMetadata
{
    @Test
    public void testLikeToRegexp()
    {
        assertThat(likeToRegexp("a_b_c", Optional.empty())).isEqualTo("a.b.c");
        assertThat(likeToRegexp("a%b%c", Optional.empty())).isEqualTo("a.*b.*c");
        assertThat(likeToRegexp("a%b_c", Optional.empty())).isEqualTo("a.*b.c");
        assertThat(likeToRegexp("a[b", Optional.empty())).isEqualTo("a\\[b");
        assertThat(likeToRegexp("a_\\_b", Optional.of("\\"))).isEqualTo("a._b");
        assertThat(likeToRegexp("a$_b", Optional.of("$"))).isEqualTo("a_b");
        assertThat(likeToRegexp("s_.m%ex\\t", Optional.of("$"))).isEqualTo("s.\\.m.*ex\\\\t");
        assertThat(likeToRegexp("\000%", Optional.empty())).isEqualTo("\000.*");
        assertThat(likeToRegexp("\000%", Optional.of("\000"))).isEqualTo("%");
        assertThat(likeToRegexp("中文%", Optional.empty())).isEqualTo("中文.*");
        assertThat(likeToRegexp("こんにちは%", Optional.empty())).isEqualTo("こんにちは.*");
        assertThat(likeToRegexp("안녕하세요%", Optional.empty())).isEqualTo("안녕하세요.*");
        assertThat(likeToRegexp("Привет%", Optional.empty())).isEqualTo("Привет.*");
    }

    @Test
    public void testApplyLimitAddsDefaultDocSortForScan()
            throws IOException
    {
        ElasticsearchClient client = createClient();
        try {
            ElasticsearchMetadata metadata = new ElasticsearchMetadata(TESTING_TYPE_MANAGER, client, config());
            ElasticsearchTableHandle table = new ElasticsearchTableHandle(SCAN, "default", "nation", Optional.empty());

            LimitApplicationResult<ConnectorTableHandle> result = metadata.applyLimit(SESSION, table, 5).orElseThrow();
            ElasticsearchTableHandle newHandle = (ElasticsearchTableHandle) result.getHandle();

            assertThat(newHandle.topN()).hasValueSatisfying(topN -> {
                assertThat(topN.limit()).isEqualTo(5);
                assertThat(topN.topNSortItems()).containsExactly(DEFAULT_SORT_BY_DOC);
            });
        }
        finally {
            client.close();
        }
    }

    @Test
    public void testApplyLimitDoesNotAddDefaultDocSortForQueryBackedScan()
            throws IOException
    {
        ElasticsearchClient client = createClient();
        try {
            ElasticsearchMetadata metadata = new ElasticsearchMetadata(TESTING_TYPE_MANAGER, client, config());
            ElasticsearchTableHandle table = new ElasticsearchTableHandle(SCAN, "default", "nation", Optional.of("{\"query\":{\"match_all\":{}}}"));

            LimitApplicationResult<ConnectorTableHandle> result = metadata.applyLimit(SESSION, table, 5).orElseThrow();
            ElasticsearchTableHandle newHandle = (ElasticsearchTableHandle) result.getHandle();

            assertThat(newHandle.topN()).hasValueSatisfying(topN -> {
                assertThat(topN.limit()).isEqualTo(5);
                assertThat(topN.topNSortItems()).isEmpty();
            });
        }
        finally {
            client.close();
        }
    }

    @Test
    public void testApplyLimitRejectsAggregationHandle()
            throws IOException
    {
        ElasticsearchClient client = createClient();
        try {
            ElasticsearchMetadata metadata = new ElasticsearchMetadata(TESTING_TYPE_MANAGER, client, config());

            assertThat(metadata.applyLimit(SESSION, aggregationHandle(), 5)).isEmpty();
        }
        finally {
            client.close();
        }
    }

    @Test
    public void testApplyLimitDoesNotWidenExistingTopN()
            throws IOException
    {
        ElasticsearchClient client = createClient();
        try {
            ElasticsearchMetadata metadata = new ElasticsearchMetadata(TESTING_TYPE_MANAGER, client, config());

            assertThat(metadata.applyLimit(SESSION, tableHandleWithTopN(new TopN(5, List.of(DEFAULT_SORT_BY_DOC))), 10))
                    .isEmpty();
        }
        finally {
            client.close();
        }
    }

    @Test
    public void testApplyTopNCreatesSortItems()
            throws IOException
    {
        ElasticsearchClient client = createClient();
        try {
            ElasticsearchMetadata metadata = new ElasticsearchMetadata(TESTING_TYPE_MANAGER, client, config());
            ElasticsearchColumnHandle regionkey = bigintColumn("regionkey");

            TopNApplicationResult<ConnectorTableHandle> result = metadata.applyTopN(
                    SESSION,
                    scanHandle(),
                    5,
                    List.of(new SortItem("regionkey", SortOrder.DESC_NULLS_FIRST)),
                    Map.of("regionkey", regionkey))
                    .orElseThrow();
            ElasticsearchTableHandle newHandle = (ElasticsearchTableHandle) result.getHandle();

            assertThat(newHandle.topN()).hasValue(new TopN(
                    5,
                    List.of(TopN.TopNSortItem.sortBy("regionkey", SortOrder.DESC_NULLS_FIRST))));
        }
        finally {
            client.close();
        }
    }

    @Test
    public void testApplyTopNRejectsAggregationHandle()
            throws IOException
    {
        ElasticsearchClient client = createClient();
        try {
            ElasticsearchMetadata metadata = new ElasticsearchMetadata(TESTING_TYPE_MANAGER, client, config());

            assertThat(metadata.applyTopN(
                    SESSION,
                    aggregationHandle(),
                    5,
                    List.of(new SortItem("regionkey", SortOrder.ASC_NULLS_LAST)),
                    Map.of("regionkey", bigintColumn("regionkey"))))
                    .isEmpty();
        }
        finally {
            client.close();
        }
    }

    @Test
    public void testApplyTopNRejectsUnsupportedSortColumn()
            throws IOException
    {
        ElasticsearchClient client = createClient();
        try {
            ElasticsearchMetadata metadata = new ElasticsearchMetadata(TESTING_TYPE_MANAGER, client, config());

            assertThat(metadata.applyTopN(
                    SESSION,
                    scanHandle(),
                    5,
                    List.of(new SortItem("description", SortOrder.ASC_NULLS_LAST)),
                    Map.of("description", textColumn("description"))))
                    .isEmpty();
        }
        finally {
            client.close();
        }
    }

    @Test
    public void testApplyAggregationCreatesMetricAndTermAggregations()
            throws IOException
    {
        ElasticsearchClient client = createClient();
        try {
            ElasticsearchMetadata metadata = new ElasticsearchMetadata(TESTING_TYPE_MANAGER, client, config());
            ElasticsearchColumnHandle regionkey = bigintColumn("regionkey");
            ElasticsearchColumnHandle nationkey = bigintColumn("nationkey");

            AggregationApplicationResult<ConnectorTableHandle> result = metadata.applyAggregation(
                    SESSION,
                    scanHandle(),
                    List.of(sumAggregation("nationkey")),
                    Map.of("nationkey", nationkey),
                    List.of(List.of(regionkey)))
                    .orElseThrow();
            ElasticsearchTableHandle newHandle = (ElasticsearchTableHandle) result.getHandle();

            assertThat(newHandle.type()).isEqualTo(AGGREGATION);
            assertThat(newHandle.termAggregations())
                    .containsExactly(new TermAggregation("regionkey", BIGINT));
            assertThat(newHandle.metricAggregations())
                    .containsExactly(new MetricAggregation(
                            "sum",
                            BIGINT,
                            Optional.of(nationkey),
                            result.getAssignments().getFirst().getVariable()));
            assertThat(result.getAssignments()).singleElement().satisfies(assignment -> {
                ElasticsearchColumnHandle assignmentColumn = (ElasticsearchColumnHandle) assignment.getColumn();

                assertThat(assignment.getType()).isEqualTo(BIGINT);
                assertThat(assignmentColumn.name()).isEqualTo(assignment.getVariable());
            });
        }
        finally {
            client.close();
        }
    }

    @Test
    public void testApplyAggregationRejectsMultipleGroupingSets()
            throws IOException
    {
        ElasticsearchClient client = createClient();
        try {
            ElasticsearchMetadata metadata = new ElasticsearchMetadata(TESTING_TYPE_MANAGER, client, config());
            ElasticsearchColumnHandle regionkey = bigintColumn("regionkey");

            assertThat(metadata.applyAggregation(
                    SESSION,
                    scanHandle(),
                    List.of(countStarAggregation()),
                    Map.of(),
                    List.of(List.of(regionkey), List.of())))
                    .isEmpty();
        }
        finally {
            client.close();
        }
    }

    @Test
    public void testApplyAggregationRejectsNestedAggregation()
            throws IOException
    {
        ElasticsearchClient client = createClient();
        try {
            ElasticsearchMetadata metadata = new ElasticsearchMetadata(TESTING_TYPE_MANAGER, client, config());

            assertThat(metadata.applyAggregation(
                    SESSION,
                    aggregationHandle(List.of(new TermAggregation("regionkey", BIGINT))),
                    List.of(countStarAggregation()),
                    Map.of(),
                    List.of(List.of())))
                    .isEmpty();
        }
        finally {
            client.close();
        }
    }

    @Test
    public void testApplyAggregationRejectsUnsupportedGroupingColumn()
            throws IOException
    {
        ElasticsearchClient client = createClient();
        try {
            ElasticsearchMetadata metadata = new ElasticsearchMetadata(TESTING_TYPE_MANAGER, client, config());

            assertThat(metadata.applyAggregation(
                    SESSION,
                    scanHandle(),
                    List.of(countStarAggregation()),
                    Map.of(),
                    List.of(List.of(textColumn("description")))))
                    .isEmpty();
        }
        finally {
            client.close();
        }
    }

    @Test
    public void testApplyAggregationRejectsUnsupportedMetricColumn()
            throws IOException
    {
        ElasticsearchClient client = createClient();
        try {
            ElasticsearchMetadata metadata = new ElasticsearchMetadata(TESTING_TYPE_MANAGER, client, config());
            ElasticsearchColumnHandle name = keywordColumn("name");

            assertThat(metadata.applyAggregation(
                    SESSION,
                    scanHandle(),
                    List.of(sumAggregation("name", VARCHAR)),
                    Map.of("name", name),
                    List.of(List.of())))
                    .isEmpty();
        }
        finally {
            client.close();
        }
    }

    private static String likeToRegexp(String pattern, Optional<String> escapeChar)
    {
        return ElasticsearchMetadata.likeToRegexp(Slices.utf8Slice(pattern), escapeChar.map(Slices::utf8Slice));
    }

    private static ElasticsearchTableHandle scanHandle()
    {
        return new ElasticsearchTableHandle(SCAN, "default", "nation", Optional.empty());
    }

    private static ElasticsearchTableHandle aggregationHandle()
    {
        return aggregationHandle(List.of());
    }

    private static ElasticsearchTableHandle tableHandleWithTopN(TopN topN)
    {
        return new ElasticsearchTableHandle(
                SCAN,
                "default",
                "nation",
                TupleDomain.all(),
                Map.of(),
                Optional.empty(),
                Set.of(),
                List.of(),
                List.of(),
                Optional.of(topN));
    }

    private static ElasticsearchTableHandle aggregationHandle(List<TermAggregation> termAggregations)
    {
        return new ElasticsearchTableHandle(
                AGGREGATION,
                "default",
                "nation",
                TupleDomain.all(),
                Map.of(),
                Optional.empty(),
                Set.of(),
                termAggregations,
                List.of(),
                Optional.empty());
    }

    private static ElasticsearchColumnHandle bigintColumn(String name)
    {
        return new ElasticsearchColumnHandle(
                List.of(name),
                BIGINT,
                new IndexMetadata.PrimitiveType("long"),
                new BigintDecoder.Descriptor(name),
                true);
    }

    private static ElasticsearchColumnHandle keywordColumn(String name)
    {
        return new ElasticsearchColumnHandle(
                List.of(name),
                VARCHAR,
                new IndexMetadata.PrimitiveType("keyword"),
                new VarcharDecoder.Descriptor(name),
                true);
    }

    private static ElasticsearchColumnHandle textColumn(String name)
    {
        return new ElasticsearchColumnHandle(
                List.of(name),
                VARCHAR,
                new IndexMetadata.PrimitiveType("text"),
                new VarcharDecoder.Descriptor(name),
                false);
    }

    private static AggregateFunction countStarAggregation()
    {
        return new AggregateFunction("count", BIGINT, List.of(), List.of(), false, Optional.empty());
    }

    private static AggregateFunction sumAggregation(String variableName)
    {
        return sumAggregation(variableName, BIGINT);
    }

    private static AggregateFunction sumAggregation(String variableName, Type inputType)
    {
        return new AggregateFunction(
                "sum",
                BIGINT,
                List.of(new Variable(variableName, inputType)),
                List.of(),
                false,
                Optional.empty());
    }

    private static ElasticsearchClient createClient()
    {
        return new ElasticsearchClient(config(), Optional.empty(), Optional.empty());
    }

    private static ElasticsearchConfig config()
    {
        return new ElasticsearchConfig()
                .setHosts(List.of("localhost"))
                .setDefaultSchema("default");
    }
}
