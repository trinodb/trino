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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableMap;
import com.qubole.rubix.core.CachingFileSystem;
import io.trino.spi.Plugin;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.collect.MoreCollectors.toOptional;
import static com.google.common.collect.Streams.stream;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.HiveSessionProperties.InsertExistingPartitionsBehavior.APPEND;
import static io.trino.plugin.hive.HiveSessionProperties.InsertExistingPartitionsBehavior.ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(singleThreaded = true) // see @BeforeMethod
public class TestHivePlugin
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

    @AfterMethod(alwaysRun = true)
    @BeforeMethod
    public void deinitializeRubix()
    {
        // revert static rubix initialization done by other tests
        CachingFileSystem.deinitialize();
    }

    @Test
    public void testCreateConnector()
    {
        ConnectorFactory factory = getHiveConnectorFactory();

        // simplest possible configuration
        factory.create("test", ImmutableMap.of("hive.metastore.uri", "thrift://foo:1234"), new TestingConnectorContext()).shutdown();
    }

    @Test
    public void testTestingFileMetastore()
    {
        ConnectorFactory factory = getHiveConnectorFactory();
        factory.create(
                        "test",
                        ImmutableMap.of(
                                "hive.metastore", "file",
                                "hive.metastore.catalog.dir", "/tmp"),
                        new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testThriftMetastore()
    {
        ConnectorFactory factory = getHiveConnectorFactory();

        factory.create(
                "test",
                ImmutableMap.of(
                        "hive.metastore", "thrift",
                        "hive.metastore.uri", "thrift://foo:1234"),
                new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testGlueMetastore()
    {
        ConnectorFactory factory = getHiveConnectorFactory();

        factory.create(
                "test",
                ImmutableMap.of(
                        "hive.metastore", "glue",
                        "hive.metastore.glue.region", "us-east-2"),
                new TestingConnectorContext())
                .shutdown();

        assertThatThrownBy(() -> factory.create(
                "test",
                ImmutableMap.of(
                        "hive.metastore", "glue",
                        "hive.metastore.uri", "thrift://foo:1234"),
                new TestingConnectorContext()))
                .hasMessageContaining("Error: Configuration property 'hive.metastore.uri' was not used");
    }

    @Test
    public void testRecordingMetastore()
    {
        ConnectorFactory factory = getHiveConnectorFactory();

        factory.create(
                "test",
                ImmutableMap.of(
                        "hive.metastore", "thrift",
                        "hive.metastore.uri", "thrift://foo:1234",
                        "hive.metastore-recording-path", "/tmp"),
                new TestingConnectorContext())
                .shutdown();

        factory.create(
                "test",
                ImmutableMap.of(
                        "hive.metastore", "glue",
                        "hive.metastore.glue.region", "us-east-2",
                        "hive.metastore-recording-path", "/tmp"),
                new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testS3SecurityMappingAndHiveCachingMutuallyExclusive()
            throws IOException
    {
        Path mappingConfig = Files.createTempFile(null, null);
        ConnectorFactory connectorFactory = getHiveConnectorFactory();

        assertThatThrownBy(() -> connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.s3.security-mapping.config-file", mappingConfig.toString())
                        .put("hive.cache.enabled", "true")
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .put("hive.cache.location", tempDirectory.toString())
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .hasMessageContaining("S3 security mapping is not compatible with Hive caching");
    }

    @Test
    public void testGcsAccessTokenAndHiveCachingMutuallyExclusive()
    {
        ConnectorFactory connectorFactory = getHiveConnectorFactory();

        assertThatThrownBy(() -> connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.gcs.use-access-token", "true")
                        .put("hive.cache.enabled", "true")
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .put("hive.cache.location", tempDirectory.toString())
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .hasMessageContaining("Use of GCS access token is not compatible with Hive caching");
    }

    @Test
    public void testImmutablePartitionsAndInsertOverwriteMutuallyExclusive()
    {
        ConnectorFactory connectorFactory = getHiveConnectorFactory();

        assertThatThrownBy(() -> connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.insert-existing-partitions-behavior", "APPEND")
                        .put("hive.immutable-partitions", "true")
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .hasMessageContaining("insert-existing-partitions-behavior cannot be APPEND when immutable-partitions is true");
    }

    @Test
    public void testInsertOverwriteIsSetToErrorWhenImmutablePartitionsIsTrue()
    {
        ConnectorFactory connectorFactory = getHiveConnectorFactory();

        Connector connector = connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.immutable-partitions", "true")
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .buildOrThrow(),
                new TestingConnectorContext());
        assertThat(getDefaultValueInsertExistingPartitionsBehavior(connector)).isEqualTo(ERROR);
        connector.shutdown();
    }

    @Test
    public void testInsertOverwriteIsSetToAppendWhenImmutablePartitionsIsFalseByDefault()
    {
        ConnectorFactory connectorFactory = getHiveConnectorFactory();

        Connector connector = connectorFactory.create(
                "test",
                ImmutableMap.of("hive.metastore.uri", "thrift://foo:1234"),
                new TestingConnectorContext());
        assertThat(getDefaultValueInsertExistingPartitionsBehavior(connector)).isEqualTo(APPEND);
        connector.shutdown();
    }

    private Object getDefaultValueInsertExistingPartitionsBehavior(Connector connector)
    {
        return connector.getSessionProperties().stream()
                .filter(propertyMetadata -> "insert_existing_partitions_behavior".equals(propertyMetadata.getName()))
                .collect(onlyElement())
                .getDefaultValue();
    }

    @Test
    public void testHdfsImpersonationAndHiveCachingMutuallyExclusive()
    {
        ConnectorFactory connectorFactory = getHiveConnectorFactory();

        assertThatThrownBy(() -> connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.hdfs.impersonation.enabled", "true")
                        .put("hive.cache.enabled", "true")
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .put("hive.cache.location", tempDirectory.toString())
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .hasMessageContaining("HDFS impersonation is not compatible with Hive caching");
    }

    @Test
    public void testRubixCache()
    {
        ConnectorFactory connectorFactory = getHiveConnectorFactory();

        connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.cache.enabled", "true")
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .put("hive.cache.location", tempDirectory.toString())
                        .buildOrThrow(),
                new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testRubixCacheWithNonExistingCacheDirectory()
    {
        ConnectorFactory connectorFactory = getHiveConnectorFactory();

        assertThatThrownBy(() -> connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.cache.enabled", "true")
                        .put("hive.cache.start-server-on-coordinator", "true")
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .put("hive.cache.location", "/tmp/non/existing/directory")
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .hasRootCauseMessage("None of the cache parent directories exists");

        assertThatThrownBy(() -> connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.cache.enabled", "true")
                        .put("hive.cache.start-server-on-coordinator", "true")
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .buildOrThrow(),
                new TestingConnectorContext()))
                .hasRootCauseMessage("caching directories were not provided");

        // cache directories should not be required when cache is not explicitly started on coordinator
        connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.cache.enabled", "true")
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .buildOrThrow(),
                new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testAllowAllAccessControl()
    {
        ConnectorFactory connectorFactory = getHiveConnectorFactory();

        connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .put("hive.security", "allow-all")
                        .buildOrThrow(),
                new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testReadOnlyAllAccessControl()
    {
        ConnectorFactory connectorFactory = getHiveConnectorFactory();

        connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .put("hive.security", "read-only")
                        .buildOrThrow(),
                new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testFileBasedAccessControl()
            throws Exception
    {
        ConnectorFactory connectorFactory = getHiveConnectorFactory();
        File tempFile = File.createTempFile("test-hive-plugin-access-control", ".json");
        tempFile.deleteOnExit();
        Files.write(tempFile.toPath(), "{}".getBytes(UTF_8));

        connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .put("hive.security", "file")
                        .put("security.config-file", tempFile.getAbsolutePath())
                        .buildOrThrow(),
                new TestingConnectorContext())
                .shutdown();
    }

    @Test
    public void testSystemAccessControl()
    {
        ConnectorFactory connectorFactory = getHiveConnectorFactory();

        Connector connector = connectorFactory.create(
                "test",
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore.uri", "thrift://foo:1234")
                        .put("hive.security", "system")
                        .buildOrThrow(),
                new TestingConnectorContext());
        assertThatThrownBy(connector::getAccessControl).isInstanceOf(UnsupportedOperationException.class);
        connector.shutdown();
    }

    private static ConnectorFactory getHiveConnectorFactory()
    {
        Plugin plugin = new HivePlugin();
        return stream(plugin.getConnectorFactories())
                .filter(factory -> factory.getName().equals("hive"))
                .collect(toOptional())
                .orElseThrow();
    }
}
