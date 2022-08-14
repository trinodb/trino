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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.common.primitives.Shorts;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import org.apache.hadoop.fs.permission.FsPermission;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Integer.parseUnsignedInt;
import static java.util.Objects.requireNonNull;

public class HdfsConfig
{
    public static final String SKIP_DIR_PERMISSIONS = "skip";
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private List<File> resourceConfigFiles = ImmutableList.of();
    private String newDirectoryPermissions = "0777";
    private boolean newFileInheritOwnership;
    private boolean verifyChecksum = true;
    private Duration ipcPingInterval = new Duration(10, TimeUnit.SECONDS);
    private Duration dfsTimeout = new Duration(60, TimeUnit.SECONDS);
    private Duration dfsConnectTimeout = new Duration(500, TimeUnit.MILLISECONDS);
    private int dfsConnectMaxRetries = 5;
    private Duration dfsKeyProviderCacheTtl = new Duration(30, TimeUnit.MINUTES);
    private String domainSocketPath;
    private HostAndPort socksProxy;
    private boolean wireEncryptionEnabled;
    private int fileSystemMaxCacheSize = 1000;
    private Integer dfsReplication;

    @NotNull
    public List<@FileExists File> getResourceConfigFiles()
    {
        return resourceConfigFiles;
    }

    @Config("hive.config.resources")
    public HdfsConfig setResourceConfigFiles(String files)
    {
        this.resourceConfigFiles = SPLITTER.splitToList(files).stream()
                .map(File::new)
                .collect(toImmutableList());
        return this;
    }

    public HdfsConfig setResourceConfigFiles(List<File> files)
    {
        this.resourceConfigFiles = ImmutableList.copyOf(files);
        return this;
    }

    public Optional<FsPermission> getNewDirectoryFsPermissions()
    {
        if (newDirectoryPermissions.equalsIgnoreCase(HdfsConfig.SKIP_DIR_PERMISSIONS)) {
            return Optional.empty();
        }
        return Optional.of(FsPermission.createImmutable(Shorts.checkedCast(parseUnsignedInt(newDirectoryPermissions, 8))));
    }

    @Pattern(regexp = "(skip)|0[0-7]{3}", message = "must be either 'skip' or an octal number, with leading 0")
    public String getNewDirectoryPermissions()
    {
        return this.newDirectoryPermissions;
    }

    @Config("hive.fs.new-directory-permissions")
    @ConfigDescription("File system permissions for new directories")
    public HdfsConfig setNewDirectoryPermissions(String newDirectoryPermissions)
    {
        this.newDirectoryPermissions = requireNonNull(newDirectoryPermissions, "newDirectoryPermissions is null");
        return this;
    }

    public boolean isNewFileInheritOwnership()
    {
        return newFileInheritOwnership;
    }

    @Config("hive.fs.new-file-inherit-ownership")
    @ConfigDescription("File system permissions for new directories")
    public HdfsConfig setNewFileInheritOwnership(boolean newFileInheritOwnership)
    {
        this.newFileInheritOwnership = newFileInheritOwnership;
        return this;
    }

    public boolean isVerifyChecksum()
    {
        return verifyChecksum;
    }

    @Config("hive.dfs.verify-checksum")
    public HdfsConfig setVerifyChecksum(boolean verifyChecksum)
    {
        this.verifyChecksum = verifyChecksum;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getIpcPingInterval()
    {
        return ipcPingInterval;
    }

    @Config("hive.dfs.ipc-ping-interval")
    public HdfsConfig setIpcPingInterval(Duration pingInterval)
    {
        this.ipcPingInterval = pingInterval;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getDfsTimeout()
    {
        return dfsTimeout;
    }

    @Config("hive.dfs-timeout")
    public HdfsConfig setDfsTimeout(Duration dfsTimeout)
    {
        this.dfsTimeout = dfsTimeout;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getDfsConnectTimeout()
    {
        return dfsConnectTimeout;
    }

    @Config("hive.dfs.connect.timeout")
    public HdfsConfig setDfsConnectTimeout(Duration dfsConnectTimeout)
    {
        this.dfsConnectTimeout = dfsConnectTimeout;
        return this;
    }

    @Min(0)
    public int getDfsConnectMaxRetries()
    {
        return dfsConnectMaxRetries;
    }

    @Config("hive.dfs.connect.max-retries")
    public HdfsConfig setDfsConnectMaxRetries(int dfsConnectMaxRetries)
    {
        this.dfsConnectMaxRetries = dfsConnectMaxRetries;
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getDfsKeyProviderCacheTtl()
    {
        return dfsKeyProviderCacheTtl;
    }

    @Config("hive.dfs.key-provider.cache-ttl")
    public HdfsConfig setDfsKeyProviderCacheTtl(Duration dfsClientKeyProviderCacheTtl)
    {
        this.dfsKeyProviderCacheTtl = dfsClientKeyProviderCacheTtl;
        return this;
    }

    public String getDomainSocketPath()
    {
        return domainSocketPath;
    }

    @Config("hive.dfs.domain-socket-path")
    public HdfsConfig setDomainSocketPath(String domainSocketPath)
    {
        this.domainSocketPath = domainSocketPath;
        return this;
    }

    public HostAndPort getSocksProxy()
    {
        return socksProxy;
    }

    @Config("hive.hdfs.socks-proxy")
    public HdfsConfig setSocksProxy(HostAndPort socksProxy)
    {
        this.socksProxy = socksProxy;
        return this;
    }

    public boolean isWireEncryptionEnabled()
    {
        return wireEncryptionEnabled;
    }

    @Config("hive.hdfs.wire-encryption.enabled")
    @ConfigDescription("Should be turned on when HDFS wire encryption is enabled")
    public HdfsConfig setWireEncryptionEnabled(boolean wireEncryptionEnabled)
    {
        this.wireEncryptionEnabled = wireEncryptionEnabled;
        return this;
    }

    public int getFileSystemMaxCacheSize()
    {
        return fileSystemMaxCacheSize;
    }

    @Config("hive.fs.cache.max-size")
    @ConfigDescription("Hadoop FileSystem cache size")
    public HdfsConfig setFileSystemMaxCacheSize(int fileSystemMaxCacheSize)
    {
        this.fileSystemMaxCacheSize = fileSystemMaxCacheSize;
        return this;
    }

    @Min(1)
    public Integer getDfsReplication()
    {
        return dfsReplication;
    }

    @Config("hive.dfs.replication")
    @ConfigDescription("Hadoop FileSystem replication factor")
    public HdfsConfig setDfsReplication(Integer dfsReplication)
    {
        this.dfsReplication = dfsReplication;
        return this;
    }
}
