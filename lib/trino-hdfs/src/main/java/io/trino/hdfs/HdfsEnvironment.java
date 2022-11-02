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

import io.trino.hadoop.HadoopNative;
import io.trino.hdfs.authentication.GenericExceptionAction;
import io.trino.hdfs.authentication.HdfsAuthentication;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemManager;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class HdfsEnvironment
{
    static {
        HadoopNative.requireHadoopNative();
        FileSystemManager.registerCache(TrinoFileSystemCache.INSTANCE);
    }

    private final HdfsConfiguration hdfsConfiguration;
    private final HdfsAuthentication hdfsAuthentication;
    private final Optional<FsPermission> newDirectoryPermissions;
    private final boolean newFileInheritOwnership;
    private final boolean verifyChecksum;

    @Inject
    public HdfsEnvironment(
            HdfsConfiguration hdfsConfiguration,
            HdfsConfig config,
            HdfsAuthentication hdfsAuthentication)
    {
        this.hdfsConfiguration = requireNonNull(hdfsConfiguration, "hdfsConfiguration is null");
        this.newFileInheritOwnership = config.isNewFileInheritOwnership();
        this.verifyChecksum = config.isVerifyChecksum();
        this.hdfsAuthentication = requireNonNull(hdfsAuthentication, "hdfsAuthentication is null");
        this.newDirectoryPermissions = config.getNewDirectoryFsPermissions();
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
        return hdfsAuthentication.doAs(identity, () -> {
            FileSystem fileSystem = path.getFileSystem(configuration);
            fileSystem.setVerifyChecksum(verifyChecksum);
            return fileSystem;
        });
    }

    public Optional<FsPermission> getNewDirectoryPermissions()
    {
        return newDirectoryPermissions;
    }

    public boolean isNewFileInheritOwnership()
    {
        return newFileInheritOwnership;
    }

    public <R, E extends Exception> R doAs(ConnectorIdentity identity, GenericExceptionAction<R, E> action)
            throws E
    {
        return hdfsAuthentication.doAs(identity, action);
    }

    public void doAs(ConnectorIdentity identity, Runnable action)
    {
        hdfsAuthentication.doAs(identity, action);
    }
}
