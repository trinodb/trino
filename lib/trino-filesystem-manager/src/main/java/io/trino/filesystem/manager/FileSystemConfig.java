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

import static io.trino.filesystem.manager.FileSystemConfig.CacheType.NONE;

public class FileSystemConfig
{
    private boolean hadoopEnabled = true;
    private boolean nativeAzureEnabled;
    private boolean nativeS3Enabled;
    private boolean nativeGcsEnabled;
    private CacheType cacheType = NONE;

    // This enables us to communicate with S3Express buckets.
    // Enabling `nativeS3Enabled` by default may lead to differences in behavior.
    // This flag indicates the `FileSystemModule` to load `S3FileSystemModule` as well for
    // accessing S3Express while `FileSystemModule` still passes `s3Enabled` to `HdfsFileSystemLoader` on the basis of `nativeS3Enabled`.
    // This gives the benefit of the both -- we can have the calls routed to old FileSystem until `nativeS3Enabled` is true in OSS.
    private boolean nativeS3ExpressEnabled = true;

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

    public boolean isNativeS3ExpressEnabled()
    {
        return nativeS3ExpressEnabled;
    }

    @Config("fs.s3-express.enabled")
    public FileSystemConfig setNativeS3ExpressEnabled(boolean nativeS3ExpressEnabled)
    {
        this.nativeS3ExpressEnabled = nativeS3ExpressEnabled;
        return this;
    }

    public CacheType getCacheType()
    {
        return cacheType;
    }

    @Config("fs.cache")
    public FileSystemConfig setCacheType(CacheType cacheType)
    {
        this.cacheType = cacheType;
        return this;
    }

    public enum CacheType
    {
        NONE,
        ALLUXIO,
    }
}
