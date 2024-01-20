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
package io.trino.plugin.cassandra;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.Resources.getResource;

public class TestCassandraPlugin
{
    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new CassandraPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create(
                "test",
                ImmutableMap.of(
                        "cassandra.contact-points", "host1",
                        "cassandra.load-policy.dc-aware.local-dc", "datacenter1"),
                new TestingConnectorContext()).shutdown();
    }

    @Test
    public void testTls()
            throws Exception
    {
        Path keystoreFile = new File(getResource("localhost.keystore").toURI()).toPath();
        Path truststoreFile = new File(getResource("localhost.truststore").toURI()).toPath();

        Plugin plugin = new CassandraPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create(
                "test",
                ImmutableMap.of(
                        "cassandra.contact-points", "host1",
                        "cassandra.load-policy.dc-aware.local-dc", "datacenter1",
                        "cassandra.tls.enabled", "true",
                        "cassandra.tls.keystore-path", keystoreFile.toString(),
                        "cassandra.tls.keystore-password", "changeit",
                        "cassandra.tls.truststore-path", truststoreFile.toString(),
                        "cassandra.tls.truststore-password", "changeit"),
                new TestingConnectorContext()).shutdown();
    }
}
