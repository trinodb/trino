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
package io.trino.filesystem.manager;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.LegacyConfig;

import static java.lang.System.getenv;

public class FileSystemConfig
{
    private boolean hadoopEnabled;
    private boolean alluxioEnabled;
    private boolean azureEnabled;
    private boolean s3Enabled;
    private boolean gcsEnabled;
    private boolean localEnabled;
    private boolean cacheEnabled;

    // Enable leak detection if configured or if running in a CI environment
    private boolean trackingEnabled = getenv("CONTINUOUS_INTEGRATION") != null;

    public boolean isHadoopEnabled()
    {
        return hadoopEnabled;
    }

    @Config("fs.hadoop.enabled")
    public FileSystemConfig setHadoopEnabled(boolean hadoopEnabled)
    {
        this.hadoopEnabled = hadoopEnabled;
        return this;
    }

    public boolean isAlluxioEnabled()
    {
        return alluxioEnabled;
    }

    @Config("fs.alluxio.enabled")
    public FileSystemConfig setAlluxioEnabled(boolean alluxioEnabled)
    {
        this.alluxioEnabled = alluxioEnabled;
        return this;
    }

    public boolean isAzureEnabled()
    {
        return azureEnabled;
    }

    @LegacyConfig("fs.native-azure.enabled")
    @Config("fs.azure.enabled")
    public FileSystemConfig setAzureEnabled(boolean azureEnabled)
    {
        this.azureEnabled = azureEnabled;
        return this;
    }

    public boolean isS3Enabled()
    {
        return s3Enabled;
    }

    @LegacyConfig("fs.native-s3.enabled")
    @Config("fs.s3.enabled")
    public FileSystemConfig setS3Enabled(boolean s3Enabled)
    {
        this.s3Enabled = s3Enabled;
        return this;
    }

    public boolean isGcsEnabled()
    {
        return gcsEnabled;
    }

    @LegacyConfig("fs.native-gcs.enabled")
    @Config("fs.gcs.enabled")
    public FileSystemConfig setGcsEnabled(boolean gcsEnabled)
    {
        this.gcsEnabled = gcsEnabled;
        return this;
    }

    public boolean isLocalEnabled()
    {
        return localEnabled;
    }

    @LegacyConfig("fs.native-local.enabled")
    @Config("fs.local.enabled")
    public FileSystemConfig setLocalEnabled(boolean localEnabled)
    {
        this.localEnabled = localEnabled;
        return this;
    }

    public boolean isCacheEnabled()
    {
        return cacheEnabled;
    }

    @Config("fs.cache.enabled")
    public FileSystemConfig setCacheEnabled(boolean enabled)
    {
        this.cacheEnabled = enabled;
        return this;
    }

    public boolean isTrackingEnabled()
    {
        return trackingEnabled;
    }

    @ConfigDescription("Enable input/output stream tracking to detect resource leaks")
    @Config("fs.tracking.enabled")
    public FileSystemConfig setTrackingEnabled(boolean trackingEnabled)
    {
        this.trackingEnabled = trackingEnabled;
        return this;
    }
}
