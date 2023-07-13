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
    private boolean hadoopEnabled = true;
    private boolean nativeS3Enabled;

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
}
