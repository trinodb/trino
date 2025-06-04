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

import com.google.cloud.hadoop.repackaged.gcs.com.google.api.services.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.hadoop.HadoopNative;
import io.trino.hdfs.authentication.HdfsAuthentication;
import io.trino.hdfs.authentication.HdfsAuthentication.ExceptionAction;
import io.trino.hdfs.gcs.GcsStorageFactory;
import io.trino.spi.Plugin;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.security.ConnectorIdentity;
import jakarta.annotation.PreDestroy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemManager;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Optional;

import static io.trino.hdfs.FileSystemUtils.getRawFileSystem;
import static java.util.Objects.requireNonNull;

public class HdfsEnvironment
{
    static {
        HadoopNative.requireHadoopNative();
        FileSystemManager.registerCache(TrinoFileSystemCache.INSTANCE);
    }

    private static final Logger log = Logger.get(HdfsEnvironment.class);

    private final OpenTelemetry openTelemetry;
    private final HdfsConfiguration hdfsConfiguration;
    private final HdfsAuthentication hdfsAuthentication;
    private final Optional<FsPermission> newDirectoryPermissions;
    private final boolean newFileInheritOwnership;
    private final boolean verifyChecksum;
    private final Optional<GcsStorageFactory> gcsStorageFactory;

    @VisibleForTesting
    public HdfsEnvironment(HdfsConfiguration hdfsConfiguration, HdfsConfig config, HdfsAuthentication hdfsAuthentication)
    {
        this(OpenTelemetry.noop(), hdfsConfiguration, config, hdfsAuthentication, Optional.empty());
    }

    @Inject
    public HdfsEnvironment(
            OpenTelemetry openTelemetry,
            HdfsConfiguration hdfsConfiguration,
            HdfsConfig config,
            HdfsAuthentication hdfsAuthentication,
            Optional<GcsStorageFactory> gcsStorageFactory)
    {
        this.openTelemetry = requireNonNull(openTelemetry, "openTelemetry is null");
        this.hdfsConfiguration = requireNonNull(hdfsConfiguration, "hdfsConfiguration is null");
        this.newFileInheritOwnership = config.isNewFileInheritOwnership();
        this.verifyChecksum = config.isVerifyChecksum();
        this.hdfsAuthentication = requireNonNull(hdfsAuthentication, "hdfsAuthentication is null");
        this.newDirectoryPermissions = config.getNewDirectoryFsPermissions();
        this.gcsStorageFactory = requireNonNull(gcsStorageFactory, "gcsStorageFactory is null");
    }

    @PreDestroy
    public void shutdown()
            throws IOException
    {
        // shut down if running in an isolated classloader
        if (!getClass().getClassLoader().equals(Plugin.class.getClassLoader())) {
            FileSystemFinalizerService.shutdown();
            stopFileSystemStatsThread();
            TrinoFileSystemCache.INSTANCE.closeAll();
        }
    }

    public Configuration getConfiguration(HdfsContext context, Path path)
    {
        return hdfsConfiguration.getConfiguration(context, path.toUri());
    }

    public FileSystem getFileSystem(HdfsContext context, Path path)
            throws IOException
    {
        return getFileSystem(context.getIdentity(), path, getConfiguration(context, path));
    }

    public FileSystem getFileSystem(ConnectorIdentity identity, Path path, Configuration configuration)
            throws IOException
    {
        try (var _ = new ThreadContextClassLoader(getClass().getClassLoader())) {
            return hdfsAuthentication.doAs(identity, () -> {
                FileSystem fileSystem = path.getFileSystem(configuration);
                fileSystem.setVerifyChecksum(verifyChecksum);
                if (getRawFileSystem(fileSystem) instanceof OpenTelemetryAwareFileSystem fs) {
                    fs.setOpenTelemetry(openTelemetry);
                }
                return fileSystem;
            });
        }
    }

    public Optional<FsPermission> getNewDirectoryPermissions()
    {
        return newDirectoryPermissions;
    }

    public boolean isNewFileInheritOwnership()
    {
        return newFileInheritOwnership;
    }

    public <T> T doAs(ConnectorIdentity identity, ExceptionAction<T> action)
            throws IOException
    {
        try (var _ = new ThreadContextClassLoader(getClass().getClassLoader())) {
            return hdfsAuthentication.doAs(identity, action);
        }
    }

    public Storage createGcsStorage(HdfsContext context, Path path)
    {
        return gcsStorageFactory
                .orElseThrow(() -> new IllegalStateException("GcsStorageFactory not set"))
                .create(this, context, path);
    }

    private static void stopFileSystemStatsThread()
    {
        try {
            Field field = FileSystem.Statistics.class.getDeclaredField("STATS_DATA_CLEANER");
            field.setAccessible(true);
            ((Thread) field.get(null)).interrupt();
        }
        catch (ReflectiveOperationException | RuntimeException e) {
            log.error(e, "Error stopping file system stats thread");
        }
    }
}
