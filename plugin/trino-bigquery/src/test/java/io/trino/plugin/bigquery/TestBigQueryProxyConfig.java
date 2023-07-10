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
package io.trino.plugin.bigquery;

import com.google.common.collect.ImmutableMap;
import com.google.inject.ConfigurationException;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestBigQueryProxyConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(BigQueryProxyConfig.class)
                .setUri(null)
                .setPassword(null)
                .setUsername(null)
                .setKeystorePath(null)
                .setKeystorePassword(null)
                .setTruststorePath(null)
                .setTruststorePassword(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path keystoreFile = Files.createTempFile(null, null);
        Path truststoreFile = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("bigquery.rpc-proxy.uri", "http://localhost:8000")
                .put("bigquery.rpc-proxy.username", "username")
                .put("bigquery.rpc-proxy.password", "password")
                .put("bigquery.rpc-proxy.truststore-path", truststoreFile.toString())
                .put("bigquery.rpc-proxy.truststore-password", "password-truststore")
                .put("bigquery.rpc-proxy.keystore-path", keystoreFile.toString())
                .put("bigquery.rpc-proxy.keystore-password", "password-keystore")
                .buildOrThrow();

        BigQueryProxyConfig expected = new BigQueryProxyConfig()
                .setUri(URI.create("http://localhost:8000"))
                .setUsername("username")
                .setPassword("password")
                .setKeystorePath(keystoreFile.toFile())
                .setKeystorePassword("password-keystore")
                .setTruststorePath(truststoreFile.toFile())
                .setTruststorePassword("password-truststore");

        assertFullMapping(properties, expected);
    }

    @Test
    public void testInvalidConfiguration()
    {
        BigQueryProxyConfig config = new BigQueryProxyConfig();
        config.setUri(URI.create("http://localhost:8000/path"));

        assertThatThrownBy(config::validate)
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("BigQuery RPC proxy URI cannot specify path");

        config.setUri(URI.create("http://localhost:8000"));

        config.setUsername("username");

        assertThatThrownBy(config::validate)
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("bigquery.rpc-proxy.username was set but bigquery.rpc-proxy.password is empty");

        config.setUsername(null);
        config.setPassword("password");

        assertThatThrownBy(config::validate)
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("bigquery.rpc-proxy.password was set but bigquery.rpc-proxy.username is empty");

        config.setUsername("username");
        config.validate();
    }
}
