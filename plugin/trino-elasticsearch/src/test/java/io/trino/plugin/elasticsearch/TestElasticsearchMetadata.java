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
import io.trino.plugin.elasticsearch.client.ElasticsearchClient;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.LimitApplicationResult;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.elasticsearch.ElasticsearchTableHandle.Type.SCAN;
import static io.trino.plugin.elasticsearch.expression.TopN.TopNSortItem.DEFAULT_SORT_BY_DOC;
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

    private static String likeToRegexp(String pattern, Optional<String> escapeChar)
    {
        return ElasticsearchMetadata.likeToRegexp(Slices.utf8Slice(pattern), escapeChar.map(Slices::utf8Slice));
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
