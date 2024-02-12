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

import static java.util.concurrent.TimeUnit.HOURS;

public class FileSystemSpoolingConfig
{
    private boolean nativeAzureEnabled;
    private boolean nativeS3Enabled;
    private boolean nativeGcsEnabled;
    private String location;

    private Duration ttl = new Duration(2, HOURS);

    private boolean encryptionEnabled = true;

    public boolean isNativeAzureEnabled()
    {
        return nativeAzureEnabled;
    }

    @Config("fs.native-azure.enabled")
    public FileSystemSpoolingConfig setNativeAzureEnabled(boolean nativeAzureEnabled)
    {
        this.nativeAzureEnabled = nativeAzureEnabled;
        return this;
    }

    public boolean isNativeS3Enabled()
    {
        return nativeS3Enabled;
    }

    @Config("fs.native-s3.enabled")
    public FileSystemSpoolingConfig setNativeS3Enabled(boolean nativeS3Enabled)
    {
        this.nativeS3Enabled = nativeS3Enabled;
        return this;
    }

    public boolean isNativeGcsEnabled()
    {
        return nativeGcsEnabled;
    }

    @Config("fs.native-gcs.enabled")
    public FileSystemSpoolingConfig setNativeGcsEnabled(boolean nativeGcsEnabled)
    {
        this.nativeGcsEnabled = nativeGcsEnabled;
        return this;
    }

    public String getLocation()
    {
        return location;
    }

    @Config("location")
    public FileSystemSpoolingConfig setLocation(String location)
    {
        this.location = location;
        return this;
    }

    public Duration getTtl()
    {
        return ttl;
    }

    @ConfigDescription("Maximum duration for the client to retrieve spooled segment")
    @Config("ttl")
    public void setTtl(Duration ttl)
    {
        this.ttl = ttl;
    }

    public boolean isEncryptionEnabled()
    {
        return encryptionEnabled;
    }

    @ConfigDescription("Encrypt segments with ephemeral encryption keys")
    @Config("encryption")
    public void setEncryptionEnabled(boolean encryptionEnabled)
    {
        this.encryptionEnabled = encryptionEnabled;
    }

    @AssertTrue(message = "At least one native file system must be enabled")
    public boolean isEitherNativeFileSystemEnabled()
    {
        return nativeAzureEnabled || nativeS3Enabled || nativeGcsEnabled;
    }
}
