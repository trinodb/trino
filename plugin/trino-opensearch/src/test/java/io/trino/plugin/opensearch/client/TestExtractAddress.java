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
package io.trino.plugin.opensearch.client;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.opensearch.client.OpenSearchClient.extractAddress;
import static org.assertj.core.api.Assertions.assertThat;

public class TestExtractAddress
{
    @Test
    public void test()
    {
        assertThat(extractAddress("node/1.2.3.4:9200")).isEqualTo(Optional.of("node:9200"));
        assertThat(extractAddress("1.2.3.4:9200")).isEqualTo(Optional.of("1.2.3.4:9200"));
        assertThat(extractAddress("node/1.2.3.4:9200")).isEqualTo(Optional.of("node:9200"));
        assertThat(extractAddress("node/[fe80::1]:9200")).isEqualTo(Optional.of("node:9200"));
        assertThat(extractAddress("[fe80::1]:9200")).isEqualTo(Optional.of("[fe80::1]:9200"));

        assertThat(extractAddress("")).isEqualTo(Optional.empty());
        assertThat(extractAddress("node/1.2.3.4")).isEqualTo(Optional.empty());
        assertThat(extractAddress("node/1.2.3.4:xxxx")).isEqualTo(Optional.empty());
        assertThat(extractAddress("1.2.3.4:xxxx")).isEqualTo(Optional.empty());
    }
}
