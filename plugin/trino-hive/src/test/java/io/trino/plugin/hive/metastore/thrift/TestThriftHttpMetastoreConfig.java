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
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.trino.plugin.hive.metastore.thrift.ThriftHttpMetastoreConfig.AuthenticationMode.BEARER;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestThriftHttpMetastoreConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ThriftHttpMetastoreConfig.class)
                .setReadTimeout(new Duration(60, SECONDS))
                .setHttpBearerToken(null)
                .setAdditionalHeaders(null)
                .setAuthenticationMode(null)
                .setTruststorePath(null)
                .setTruststorePassword(null)
                .setVerifyHostname(true));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        File truststore = File.createTempFile("truststore", ".jks");
        truststore.deleteOnExit();

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.metastore.http.client.bearer-token", "test-token")
                .put("hive.metastore.http.client.additional-headers", "key\\:1:value\\,1, key\\,2:value\\:2")
                .put("hive.metastore.http.client.authentication.type", "BEARER")
                .put("hive.metastore.http.client.read-timeout", "1s")
                .put("hive.metastore.http.client.ssl.trust-certificate", truststore.getPath())
                .put("hive.metastore.http.client.ssl.trust-certificate-password", "changeit")
                .put("hive.metastore.http.client.ssl.verify-hostname", "false")
                .buildOrThrow();

        ThriftHttpMetastoreConfig expected = new ThriftHttpMetastoreConfig()
                .setHttpBearerToken("test-token")
                .setAdditionalHeaders("key\\:1:value\\,1, key\\,2:value\\:2")
                .setReadTimeout(new Duration(1, SECONDS))
                .setAuthenticationMode(BEARER)
                .setTruststorePath(truststore)
                .setTruststorePassword("changeit")
                .setVerifyHostname(false);

        assertFullMapping(properties, expected);
        assertThat(expected.getAdditionalHeaders())
                .isEqualTo(ImmutableMap.of("key:1", "value,1", "key,2", "value:2"));
    }
}
