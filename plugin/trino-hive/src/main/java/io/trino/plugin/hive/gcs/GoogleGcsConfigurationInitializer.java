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
package io.trino.plugin.hive.gcs;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import io.trino.hdfs.ConfigurationInitializer;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_CONFIG_PREFIX;
import static com.google.cloud.hadoop.fs.gcs.HadoopCredentialConfiguration.ACCESS_TOKEN_PROVIDER_IMPL_SUFFIX;
import static com.google.cloud.hadoop.fs.gcs.HadoopCredentialConfiguration.ENABLE_SERVICE_ACCOUNTS_SUFFIX;
import static com.google.cloud.hadoop.fs.gcs.HadoopCredentialConfiguration.SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX;

public class GoogleGcsConfigurationInitializer
        implements ConfigurationInitializer
{
    private final boolean useGcsAccessToken;
    private final String jsonKeyFilePath;

    @Inject
    public GoogleGcsConfigurationInitializer(HiveGcsConfig config)
    {
        this.useGcsAccessToken = config.isUseGcsAccessToken();
        this.jsonKeyFilePath = config.getJsonKeyFilePath();
    }

    @Override
    public void initializeConfiguration(Configuration config)
    {
        config.set("fs.gs.impl", GoogleHadoopFileSystem.class.getName());

        if (useGcsAccessToken) {
            // use oauth token to authenticate with Google Cloud Storage
            config.setBoolean(GCS_CONFIG_PREFIX + ENABLE_SERVICE_ACCOUNTS_SUFFIX.getKey(), false);
            config.setClass(GCS_CONFIG_PREFIX + ACCESS_TOKEN_PROVIDER_IMPL_SUFFIX.getKey(), GcsAccessTokenProvider.class, AccessTokenProvider.class);
        }
        else if (jsonKeyFilePath != null) {
            // use service account key file
            config.setBoolean(GCS_CONFIG_PREFIX + ENABLE_SERVICE_ACCOUNTS_SUFFIX.getKey(), true);
            config.set(GCS_CONFIG_PREFIX + SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX.getKey(), jsonKeyFilePath);
        }
    }
}
