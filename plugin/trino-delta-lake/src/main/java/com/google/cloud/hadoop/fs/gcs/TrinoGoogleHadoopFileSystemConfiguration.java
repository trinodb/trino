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
package com.google.cloud.hadoop.fs.gcs;

import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import org.apache.hadoop.conf.Configuration;

/**
 * Wrapper class used to access package-private methods which
 * convert {@link Configuration} to gcs hadoop-connectors specific
 * configuration instances.
 */
public class TrinoGoogleHadoopFileSystemConfiguration
{
    private TrinoGoogleHadoopFileSystemConfiguration() {}

    public static GoogleCloudStorageOptions.Builder getGcsOptionsBuilder(Configuration configuration)
    {
        return GoogleHadoopFileSystemConfiguration.getGcsOptionsBuilder(configuration);
    }
}
