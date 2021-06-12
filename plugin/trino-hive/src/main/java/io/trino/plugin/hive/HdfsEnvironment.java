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

import com.google.common.primitives.Shorts;
import io.trino.hadoop.HadoopNative;
import io.trino.plugin.hive.authentication.GenericExceptionAction;
import io.trino.plugin.hive.authentication.HdfsAuthentication;
import io.trino.plugin.hive.fs.TrinoFileSystemCache;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemManager;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import javax.inject.Inject;

import java.io.IOException;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.Integer.parseUnsignedInt;
import static java.util.Objects.requireNonNull;

public class HdfsEnvironment
{
    static {
        HadoopNative.requireHadoopNative();
        FileSystemManager.registerCache(TrinoFileSystemCache.INSTANCE);
    }

    private final HdfsConfiguration hdfsConfiguration;
    private final HdfsAuthentication hdfsAuthentication;
    private final FsPermission newDirectoryPermissions;
    private final boolean newFileInheritOwnership;
    private final boolean verifyChecksum;

    @Inject
    public HdfsEnvironment(
            HdfsConfiguration hdfsConfiguration,
            HdfsConfig config,
            HdfsAuthentication hdfsAuthentication)
    {
        this.hdfsConfiguration = requireNonNull(hdfsConfiguration, "hdfsConfiguration is null");
        requireNonNull(config, "config is null");
        this.newDirectoryPermissions = FsPermission.createImmutable(Shorts.checkedCast(parseUnsignedInt(config.getNewDirectoryPermissions(), 8)));
        this.newFileInheritOwnership = config.isNewFileInheritOwnership();
        this.verifyChecksum = config.isVerifyChecksum();
        this.hdfsAuthentication = requireNonNull(hdfsAuthentication, "hdfsAuthentication is null");
    }

    public Configuration getConfiguration(HdfsContext context, Path path)
    {
        return hdfsConfiguration.getConfiguration(context, path.toUri());
    }

    public FileSystem getFileSystem(HdfsContext context, Path path)
            throws IOException
    {
        return getFileSystem(context.getIdentity().getUser(), path, getConfiguration(context, path));
    }

    public FileSystem getFileSystem(String user, Path path, Configuration configuration)
            throws IOException
    {
        return hdfsAuthentication.doAs(user, () -> {
            FileSystem fileSystem = path.getFileSystem(configuration);
            fileSystem.setVerifyChecksum(verifyChecksum);
            return fileSystem;
        });
    }

    public FsPermission getNewDirectoryPermissions()
    {
        return newDirectoryPermissions;
    }

    public boolean isNewFileInheritOwnership()
    {
        return newFileInheritOwnership;
    }

    public <R, E extends Exception> R doAs(String user, GenericExceptionAction<R, E> action)
            throws E
    {
        return hdfsAuthentication.doAs(user, action);
    }

    public void doAs(String user, Runnable action)
    {
        hdfsAuthentication.doAs(user, action);
    }

    public static class HdfsContext
    {
        private final ConnectorIdentity identity;

        public HdfsContext(ConnectorIdentity identity)
        {
            this.identity = requireNonNull(identity, "identity is null");
        }

        public HdfsContext(ConnectorSession session)
        {
            requireNonNull(session, "session is null");
            this.identity = requireNonNull(session.getIdentity(), "session.getIdentity() is null");
        }

        public ConnectorIdentity getIdentity()
        {
            return identity;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .omitNullValues()
                    .add("user", identity)
                    .toString();
        }
    }
}
