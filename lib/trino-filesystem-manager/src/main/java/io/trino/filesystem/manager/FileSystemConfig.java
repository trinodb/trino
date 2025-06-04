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

public class FileSystemConfig
{
    private boolean hadoopEnabled;
    private boolean alluxioEnabled;
    private boolean nativeAzureEnabled;
    private boolean nativeS3Enabled;
    private boolean nativeGcsEnabled;
    private boolean nativeLocalEnabled;
    private boolean cacheEnabled;

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
    public FileSystemConfig setAlluxioEnabled(boolean nativeAlluxioEnabled)
    {
        this.alluxioEnabled = nativeAlluxioEnabled;
        return this;
    }

    public boolean isNativeAzureEnabled()
    {
        return nativeAzureEnabled;
    }

    @Config("fs.native-azure.enabled")
    public FileSystemConfig setNativeAzureEnabled(boolean nativeAzureEnabled)
    {
        this.nativeAzureEnabled = nativeAzureEnabled;
        return this;
    }

    public boolean isNativeS3Enabled()
    {
        return nativeS3Enabled;
    }

    @Config("fs.native-s3.enabled")
    public FileSystemConfig setNativeS3Enabled(boolean nativeS3Enabled)
    {
        this.nativeS3Enabled = nativeS3Enabled;
        return this;
    }

    public boolean isNativeGcsEnabled()
    {
        return nativeGcsEnabled;
    }

    @Config("fs.native-local.enabled")
    public FileSystemConfig setNativeLocalEnabled(boolean nativeLocalEnabled)
    {
        this.nativeLocalEnabled = nativeLocalEnabled;
        return this;
    }

    public boolean isNativeLocalEnabled()
    {
        return nativeLocalEnabled;
    }

    @Config("fs.native-gcs.enabled")
    public FileSystemConfig setNativeGcsEnabled(boolean nativeGcsEnabled)
    {
        this.nativeGcsEnabled = nativeGcsEnabled;
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
}
