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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHiveHadoop2Plugin
{
    @Test
    public void testS3SecurityMappingAndHiveCachingMutuallyExclusive()
    {
        Plugin plugin = new HiveHadoop2Plugin();
        ConnectorFactory connectorFactory = Iterables.getOnlyElement(plugin.getConnectorFactories());

        assertThatThrownBy(() -> connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.s3.security-mapping.config-file", "/tmp/blah.txt")
                        .put("hive.cache.enabled", "true")
                        .build(),
                new TestingConnectorContext())
                .shutdown())
                .hasMessageContaining("S3 security mapping is not compatible with Hive caching");
    }

    @Test
    public void testGcsAccessTokenAndHiveCachingMutuallyExclusive()
    {
        Plugin plugin = new HiveHadoop2Plugin();
        ConnectorFactory connectorFactory = Iterables.getOnlyElement(plugin.getConnectorFactories());

        assertThatThrownBy(() -> connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.gcs.use-access-token", "true")
                        .put("hive.cache.enabled", "true")
                        .build(),
                new TestingConnectorContext())
                .shutdown())
                .hasMessageContaining("Use of GCS access token is not compatible with Hive caching");
    }

    @Test
    public void testRubixCache()
    {
        Plugin plugin = new HiveHadoop2Plugin();
        ConnectorFactory connectorFactory = Iterables.getOnlyElement(plugin.getConnectorFactories());

        connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.cache.enabled", "true")
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .put("hive.cache.location", "/tmp/cache")
                        .build(),
                new TestingConnectorContext())
                .shutdown();
    }
}
