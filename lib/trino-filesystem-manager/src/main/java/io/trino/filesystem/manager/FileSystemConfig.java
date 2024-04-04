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
import io.airlift.configuration.DefunctConfig;

import static io.trino.filesystem.manager.FileSystemConfig.FileSystemCacheMode.DISABLED;

@DefunctConfig("fs.cache.enabled")
public class FileSystemConfig
{
    private boolean hadoopEnabled = true;
    private boolean nativeAzureEnabled;
    private boolean nativeS3Enabled;
    private boolean nativeGcsEnabled;
    private FileSystemCacheMode cacheMode = DISABLED;

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

    @Config("fs.native-gcs.enabled")
    public FileSystemConfig setNativeGcsEnabled(boolean nativeGcsEnabled)
    {
        this.nativeGcsEnabled = nativeGcsEnabled;
        return this;
    }

    public FileSystemCacheMode getCacheMode()
    {
        return cacheMode;
    }

    @Config("fs.cache.mode")
    public FileSystemConfig setCacheMode(FileSystemCacheMode enabled)
    {
        this.cacheMode = enabled;
        return this;
    }

    public enum FileSystemCacheMode
    {
        ENABLED, /** all nodes **/
        COORDINATOR_ONLY,
        WORKERS_ONLY,
        DISABLED;
    }
}
