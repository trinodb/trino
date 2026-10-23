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
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestThriftHttpMetastoreConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ThriftHttpMetastoreConfig.class)
                .setBearerToken(null)
                .setAdditionalHeaders(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.metastore.http.client.bearer-token", "token")
                .put("hive.metastore.http.client.additional-headers", "key1:value1,key2:value2")
                .buildOrThrow();

        ThriftHttpMetastoreConfig expected = new ThriftHttpMetastoreConfig()
                .setBearerToken("token")
                .setAdditionalHeaders("key1:value1,key2:value2");

        assertFullMapping(properties, expected);
    }

    @Test
    public void testAdditionalHeaders()
    {
        assertThat(new ThriftHttpMetastoreConfig().setAdditionalHeaders("x-actor-username:hive").getAdditionalHeaders())
                .isEqualTo(ImmutableMap.of("x-actor-username", "hive"));
        assertThat(new ThriftHttpMetastoreConfig().setAdditionalHeaders(" key1 : value1 , key2:value2 ").getAdditionalHeaders())
                .isEqualTo(ImmutableMap.of("key1", "value1", "key2", "value2"));
        // value may contain an unescaped ':' after the first one
        assertThat(new ThriftHttpMetastoreConfig().setAdditionalHeaders("key:a:b").getAdditionalHeaders())
                .isEqualTo(ImmutableMap.of("key", "a:b"));
        assertThat(new ThriftHttpMetastoreConfig().setAdditionalHeaders("key\\:1:value\\,1,key2:value2").getAdditionalHeaders())
                .isEqualTo(ImmutableMap.of("key:1", "value,1", "key2", "value2"));
    }

    @Test
    public void testInvalidAdditionalHeaders()
    {
        assertThatThrownBy(() -> new ThriftHttpMetastoreConfig().setAdditionalHeaders("key1:value1,key2"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid format for 'hive.metastore.http.client.additional-headers', expected comma-separated name:value pairs");
        assertThatThrownBy(() -> new ThriftHttpMetastoreConfig().setAdditionalHeaders(":value"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Header name must not be empty in 'hive.metastore.http.client.additional-headers'");
        assertThatThrownBy(() -> new ThriftHttpMetastoreConfig().setAdditionalHeaders("key:value,key:other"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Multiple entries with same key");
    }
}
