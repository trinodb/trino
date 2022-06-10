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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import static com.google.common.collect.MoreCollectors.toOptional;
import static com.google.common.collect.Streams.stream;

public class TestElasticsearchPlugin
{
    @Test
    public void testCreateConnector()
    {
        ConnectorFactory factory = getElasticsearchConnectorFactory();

        // simplest possible configuration
        factory.create(
                "test",
                ImmutableMap.of("elasticsearch.host", "dummy"),
                new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testPasswordSecurity()
    {
        ConnectorFactory factory = getElasticsearchConnectorFactory();

        // simplest possible configuration
        factory.create(
                "test",
                ImmutableMap.of(
                        "elasticsearch.host", "dummy",
                        "elasticsearch.security", "password",
                        "elasticsearch.auth.user", "user",
                        "elasticsearch.auth.password", "p4ssw0rd"),
                new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testAwsSecurity()
    {
        ConnectorFactory factory = getElasticsearchConnectorFactory();

        // simplest possible configuration
        factory.create(
                "test",
                ImmutableMap.of(
                        "elasticsearch.host", "dummy",
                        "elasticsearch.security", "AWS",
                        "elasticsearch.aws.region", "us-east-2"),
                new TestingConnectorContext())
                .shutdown();
    }

    private static ConnectorFactory getElasticsearchConnectorFactory()
    {
        Plugin plugin = new ElasticsearchPlugin();
        return stream(plugin.getConnectorFactories())
                .filter(factory -> factory.getName().equals("elasticsearch"))
                .collect(toOptional())
                .orElseThrow();
    }
}
