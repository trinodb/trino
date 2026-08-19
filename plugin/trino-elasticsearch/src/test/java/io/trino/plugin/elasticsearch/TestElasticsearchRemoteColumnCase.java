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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonMapperProvider;
import io.trino.plugin.elasticsearch.client.IndexMetadata;
import io.trino.plugin.elasticsearch.decoders.VarcharDecoder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.elasticsearch.ElasticsearchQueryBuilder.buildSearchQuery;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestElasticsearchRemoteColumnCase
{
    private static final JsonMapper JSON_MAPPER = new JsonMapperProvider().get();

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
    public void testUnsafeFullTextQueryUsesOriginalRemoteFieldCase()
            throws IOException
    {
        ElasticsearchColumnHandle remoteColumn = remoteTextColumn();
        JsonNode actual = buildSearchQuery(
                TupleDomain.withColumnDomains(Map.of(remoteColumn, Domain.singleValue(VARCHAR, utf8Slice("sa")))),
                Optional.empty(),
                Map.of(),
                Map.of(),
                Map.of());

        assertThat(JSON_MAPPER.readTree(actual.toString()))
                .isEqualTo(JSON_MAPPER.readTree(
                        """
                        {"bool":{"filter":[{"match_phrase":{"Ho_ten":"sa"}}]}}"""));
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
