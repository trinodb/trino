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
import com.qubole.rubix.core.CachingFileSystem;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.testing.TestingConnectorContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.prestosql.plugin.hive.HiveSessionProperties.InsertExistingPartitionsBehavior.APPEND;
import static io.prestosql.plugin.hive.HiveSessionProperties.InsertExistingPartitionsBehavior.ERROR;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHiveHadoop2Plugin
{
    private Path tempDirectory;

    @BeforeClass
    public void setup()
            throws IOException
    {
        tempDirectory = createTempDirectory(getClass().getSimpleName());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        deleteRecursively(tempDirectory, ALLOW_INSECURE);
    }

    @AfterMethod
    @BeforeMethod
    public void deinitializeRubix()
    {
        // revert static rubix initialization done by other tests
        CachingFileSystem.deinitialize();
    }

    @Test
    public void testS3SecurityMappingAndHiveCachingMutuallyExclusive()
            throws IOException
    {
        Path mappingConfig = Files.createTempFile(null, null);
        Plugin plugin = new HiveHadoop2Plugin();
        ConnectorFactory connectorFactory = Iterables.getOnlyElement(plugin.getConnectorFactories());

        assertThatThrownBy(() -> connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.s3.security-mapping.config-file", mappingConfig.toString())
                        .put("hive.cache.enabled", "true")
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .put("hive.cache.location", tempDirectory.toString())
                        .build(),
                new TestingConnectorContext())
                .shutdown()).hasMessageContaining("S3 security mapping is not compatible with Hive caching");
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
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .put("hive.cache.location", tempDirectory.toString())
                        .build(),
                new TestingConnectorContext())
                .shutdown())
                .hasMessageContaining("Use of GCS access token is not compatible with Hive caching");
    }

    @Test
    public void testImmutablePartitionsAndInsertOverwriteMutuallyExclusive()
    {
        Plugin plugin = new HiveHadoop2Plugin();
        ConnectorFactory connectorFactory = Iterables.getOnlyElement(plugin.getConnectorFactories());

        assertThatThrownBy(() -> connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.insert-existing-partitions-behavior", "APPEND")
                        .put("hive.immutable-partitions", "true")
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .build(),
                new TestingConnectorContext()))
                .hasMessageContaining("insert-existing-partitions-behavior cannot be APPEND when immutable-partitions is true");
    }

    @Test
    public void testInsertOverwriteIsSetToErrorWhenImmutablePartitionsIsTrue()
    {
        Plugin plugin = new HiveHadoop2Plugin();
        ConnectorFactory connectorFactory = Iterables.getOnlyElement(plugin.getConnectorFactories());

        Connector connector = connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.immutable-partitions", "true")
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .build(),
                new TestingConnectorContext());

        assertThat(getDefaultValueInsertExistingPartitionsBehavior(connector)).isEqualTo(ERROR);
    }

    @Test
    public void testInsertOverwriteIsSetToAppendWhenImmutablePartitionsIsFalseByDefault()
    {
        Plugin plugin = new HiveHadoop2Plugin();
        ConnectorFactory connectorFactory = Iterables.getOnlyElement(plugin.getConnectorFactories());

        Connector connector = connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .build(),
                new TestingConnectorContext());

        assertThat(getDefaultValueInsertExistingPartitionsBehavior(connector)).isEqualTo(APPEND);
    }

    private Object getDefaultValueInsertExistingPartitionsBehavior(Connector connector)
    {
        return connector.getSessionProperties().stream()
                .filter(propertyMetadata -> "insert_existing_partitions_behavior".equals(propertyMetadata.getName()))
                .findAny()
                .orElseThrow()
                .getDefaultValue();
    }

    @Test
    public void testHdfsImpersonationAndHiveCachingMutuallyExclusive()
    {
        Plugin plugin = new HiveHadoop2Plugin();
        ConnectorFactory connectorFactory = Iterables.getOnlyElement(plugin.getConnectorFactories());

        assertThatThrownBy(() -> connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.hdfs.impersonation.enabled", "true")
                        .put("hive.cache.enabled", "true")
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .put("hive.cache.location", tempDirectory.toString())
                        .build(),
                new TestingConnectorContext())
                .shutdown())
                .hasMessageContaining("HDFS impersonation is not compatible with Hive caching");
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
                        .put("hive.cache.location", tempDirectory.toString())
                        .build(),
                new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testRubixCacheWithNonExistingCacheDirectory()
    {
        Plugin plugin = new HiveHadoop2Plugin();
        ConnectorFactory connectorFactory = Iterables.getOnlyElement(plugin.getConnectorFactories());

        assertThatThrownBy(() -> connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.cache.enabled", "true")
                        .put("hive.cache.start-server-on-coordinator", "true")
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .put("hive.cache.location", "/tmp/non/existing/directory")
                        .build(),
                new TestingConnectorContext())
                .shutdown())
                .hasRootCauseMessage("None of the cache parent directories exists");

        assertThatThrownBy(() -> connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.cache.enabled", "true")
                        .put("hive.cache.start-server-on-coordinator", "true")
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .build(),
                new TestingConnectorContext())
                .shutdown())
                .hasRootCauseMessage("caching directories were not provided");

        // cache directories should not be required when cache is not explicitly started on coordinator
        connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.cache.enabled", "true")
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .build(),
                new TestingConnectorContext())
                .shutdown();
    }
}
