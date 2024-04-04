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
package io.trino.hdfs;

import com.google.common.collect.ImmutableMap;
import com.qubole.rubix.core.CachingFileSystem;
import io.airlift.testing.TempFile;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.hdfs.HdfsFileSystemManager;
import io.trino.testing.TestingNodeManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
class TestCachingSetup
{
    @BeforeEach
    @AfterEach
    public void deinitializeRubix()
    {
        // revert static Rubix initialization done by other tests
        CachingFileSystem.deinitialize();
    }

    @Test
    public void testS3SecurityMappingAndHiveCachingMutuallyExclusive(@TempDir Path tempDirectory)
            throws IOException
    {
        try (TempFile mappingConfig = new TempFile()) {
            assertThatThrownBy(() -> createFileSystemManager(
                    ImmutableMap.<String, String>builder()
                            .put("hive.s3.security-mapping.config-file", mappingConfig.path().toString())
                            .put("hive.cache.enabled", "true")
                            .put("hive.cache.location", tempDirectory.toString())
                            .buildOrThrow()))
                    .hasMessageContaining("S3 security mapping is not compatible with Hive caching");
        }
    }

    @Test
    public void testGcsAccessTokenAndHiveCachingMutuallyExclusive(@TempDir Path tempDirectory)
    {
        assertThatThrownBy(() -> createFileSystemManager(
                ImmutableMap.<String, String>builder()
                        .put("hive.gcs.use-access-token", "true")
                        .put("hive.cache.enabled", "true")
                        .put("hive.cache.location", tempDirectory.toString())
                        .buildOrThrow()))
                .hasMessageContaining("Use of GCS access token is not compatible with Hive caching");
    }

    @Test
    public void testHdfsImpersonationAndHiveCachingMutuallyExclusive(@TempDir Path tempDirectory)
    {
        assertThatThrownBy(() -> createFileSystemManager(
                ImmutableMap.<String, String>builder()
                        .put("hive.hdfs.impersonation.enabled", "true")
                        .put("hive.cache.enabled", "true")
                        .put("hive.cache.location", tempDirectory.toString())
                        .buildOrThrow()))
                .hasMessageContaining("HDFS impersonation is not compatible with Hive caching");
    }

    @Test
    public void testRubixCache(@TempDir Path tempDirectory)
    {
        createFileSystemManager(
                ImmutableMap.<String, String>builder()
                        .put("hive.cache.enabled", "true")
                        .put("hive.cache.location", tempDirectory.toString())
                        .buildOrThrow());
    }

    @Test
    public void testRubixCacheWithNonExistingCacheDirectory()
    {
        assertThatThrownBy(() -> createFileSystemManager(
                ImmutableMap.<String, String>builder()
                        .put("hive.cache.enabled", "true")
                        .put("hive.cache.start-server-on-coordinator", "true")
                        .put("hive.cache.location", "/tmp/non/existing/directory")
                        .buildOrThrow()))
                .hasMessageContaining("None of the cache parent directories exists");

        assertThatThrownBy(() -> createFileSystemManager(
                ImmutableMap.<String, String>builder()
                        .put("hive.cache.enabled", "true")
                        .put("hive.cache.start-server-on-coordinator", "true")
                        .buildOrThrow()))
                .hasMessageContaining("caching directories were not provided");

        // cache directories should not be required when cache is not explicitly started on coordinator
        createFileSystemManager(
                ImmutableMap.<String, String>builder()
                        .put("hive.cache.enabled", "true")
                        .buildOrThrow());
    }

    private static void createFileSystemManager(Map<String, String> config)
    {
        HdfsFileSystemManager manager = new HdfsFileSystemManager(
                ImmutableMap.<String, String>builder()
                        .putAll(config)
                        .put("boostrap.quiet", "true")
                        .buildOrThrow(),
                true,
                true,
                true,
                "test",
                new TestingNodeManager(),
                OpenTelemetry.noop());
        manager.configure();
        manager.create();
        manager.stop();
    }
}
