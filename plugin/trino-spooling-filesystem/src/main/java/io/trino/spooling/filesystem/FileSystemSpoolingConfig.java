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
package io.trino.spooling.filesystem;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import jakarta.validation.constraints.AssertTrue;

import static io.trino.spooling.filesystem.FileSystemSpoolingConfig.Layout.SIMPLE;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class FileSystemSpoolingConfig
{
    private boolean azureEnabled;
    private boolean s3Enabled;
    private boolean gcsEnabled;
    private String location;
    private Layout layout = SIMPLE;
    private Duration ttl = new Duration(12, HOURS);
    private Duration directAccessTtl = new Duration(1, HOURS);
    private boolean encryptionEnabled = true;
    private boolean explicitAckEnabled = true;
    private boolean pruningEnabled = true;
    private Duration pruningInterval = new Duration(5, MINUTES);
    private long pruningBatchSize = 250;

    public boolean isAzureEnabled()
    {
        return azureEnabled;
    }

    @Config("fs.azure.enabled")
    public FileSystemSpoolingConfig setAzureEnabled(boolean azureEnabled)
    {
        this.azureEnabled = azureEnabled;
        return this;
    }

    public boolean isS3Enabled()
    {
        return s3Enabled;
    }

    @Config("fs.s3.enabled")
    public FileSystemSpoolingConfig setS3Enabled(boolean nativeS3Enabled)
    {
        this.s3Enabled = nativeS3Enabled;
        return this;
    }

    public boolean isGcsEnabled()
    {
        return gcsEnabled;
    }

    @Config("fs.gcs.enabled")
    public FileSystemSpoolingConfig setGcsEnabled(boolean gcsEnabled)
    {
        this.gcsEnabled = gcsEnabled;
        return this;
    }

    public String getLocation()
    {
        return location;
    }

    @Config("fs.location")
    public FileSystemSpoolingConfig setLocation(String location)
    {
        this.location = location;
        return this;
    }

    public Layout getLayout()
    {
        return layout;
    }

    @Config("fs.layout")
    @ConfigDescription("File system layout for spooled segments storage")
    public FileSystemSpoolingConfig setLayout(Layout layout)
    {
        this.layout = layout;
        return this;
    }

    public Duration getTtl()
    {
        return ttl;
    }

    @Config("fs.segment.ttl")
    @ConfigDescription("Maximum duration for the client to retrieve spooled segment before it expires")
    public FileSystemSpoolingConfig setTtl(Duration ttl)
    {
        this.ttl = ttl;
        return this;
    }

    public Duration getDirectAccessTtl()
    {
        return directAccessTtl;
    }

    @ConfigDescription("Maximum duration for the client to retrieve spooled segment from the direct URI")
    @Config("fs.segment.direct.ttl")
    public FileSystemSpoolingConfig setDirectAccessTtl(Duration directAccessTtl)
    {
        this.directAccessTtl = directAccessTtl;
        return this;
    }

    public boolean isEncryptionEnabled()
    {
        return encryptionEnabled;
    }

    @Config("fs.segment.encryption")
    @ConfigDescription("Encrypt segments with ephemeral keys")
    public FileSystemSpoolingConfig setEncryptionEnabled(boolean encryptionEnabled)
    {
        this.encryptionEnabled = encryptionEnabled;
        return this;
    }

    public boolean isExplicitAckEnabled()
    {
        return explicitAckEnabled;
    }

    @ConfigDescription("Enables deletion of segments on client acknowledgment")
    @Config("fs.segment.explicit-ack")
    public FileSystemSpoolingConfig setExplicitAckEnabled(boolean explicitAckEnabled)
    {
        this.explicitAckEnabled = explicitAckEnabled;
        return this;
    }

    public boolean isPruningEnabled()
    {
        return pruningEnabled;
    }

    @Config("fs.segment.pruning.enabled")
    @ConfigDescription("Prune expired segments periodically")
    public FileSystemSpoolingConfig setPruningEnabled(boolean pruningEnabled)
    {
        this.pruningEnabled = pruningEnabled;
        return this;
    }

    public Duration getPruningInterval()
    {
        return pruningInterval;
    }

    @Config("fs.segment.pruning.interval")
    @ConfigDescription("Interval to prune expired segments")
    public FileSystemSpoolingConfig setPruningInterval(Duration pruningInterval)
    {
        this.pruningInterval = pruningInterval;
        return this;
    }

    public long getPruningBatchSize()
    {
        return pruningBatchSize;
    }

    @Config("fs.segment.pruning.batch-size")
    @ConfigDescription("Prune expired segments in batches of provided size")
    public FileSystemSpoolingConfig setPruningBatchSize(long pruningBatchSize)
    {
        this.pruningBatchSize = pruningBatchSize;
        return this;
    }

    @AssertTrue(message = "At least one storage file system must be enabled")
    public boolean isEitherNativeFileSystemEnabled()
    {
        return azureEnabled || s3Enabled || gcsEnabled;
    }

    @AssertTrue(message = "Location must end with a slash")
    public boolean locationEndsWithSlash()
    {
        return location.endsWith("/");
    }

    public enum Layout
    {
        SIMPLE,
        PARTITIONED
    }
}
