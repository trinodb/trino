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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.Plugin;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.type.Type;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.Resources.getResource;
import static io.trino.plugin.mongodb.ObjectIdType.OBJECT_ID;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMongoPlugin
{
    @Test
    public void testCreateConnector()
    {
        MongoPlugin plugin = new MongoPlugin();

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        Connector connector = factory.create(
                "test",
                ImmutableMap.of(
                        "mongodb.connection-url", "mongodb://localhost:27017",
                        "bootstrap.quiet", "true"),
                new TestingConnectorContext());

        Type type = getOnlyElement(plugin.getTypes());
        assertThat(type).isEqualTo(OBJECT_ID);

        connector.shutdown();
    }

    @Test
    public void testTls()
            throws Exception
    {
        Path keystoreFile = new File(getResource("localhost.keystore").toURI()).toPath();
        Path truststoreFile = new File(getResource("localhost.truststore").toURI()).toPath();

        Plugin plugin = new MongoPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create(
                "test",
                ImmutableMap.of(
                        "mongodb.connection-url", "mongodb://localhost:27017",
                        "bootstrap.quiet", "true",
                        "mongodb.tls.enabled", "true",
                        "mongodb.tls.keystore-path", keystoreFile.toString(),
                        "mongodb.tls.keystore-password", "changeit",
                        "mongodb.tls.truststore-path", truststoreFile.toString(),
                        "mongodb.tls.truststore-password", "changeit"),
                new TestingConnectorContext()).shutdown();
    }
}
