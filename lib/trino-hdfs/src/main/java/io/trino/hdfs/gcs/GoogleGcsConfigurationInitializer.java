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
package io.trino.hdfs.gcs;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.inject.Inject;
import io.trino.hdfs.ConfigurationInitializer;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumSet;
import java.util.Optional;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_CONFIG_PREFIX;
import static com.google.cloud.hadoop.fs.gcs.HadoopCredentialConfiguration.ACCESS_TOKEN_PROVIDER_IMPL_SUFFIX;
import static com.google.cloud.hadoop.fs.gcs.HadoopCredentialConfiguration.ENABLE_SERVICE_ACCOUNTS_SUFFIX;
import static com.google.cloud.hadoop.fs.gcs.HadoopCredentialConfiguration.SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;

public class GoogleGcsConfigurationInitializer
        implements ConfigurationInitializer
{
    private final boolean useGcsAccessToken;
    private final String jsonKeyFilePath;

    @Inject
    public GoogleGcsConfigurationInitializer(HiveGcsConfig config)
    {
        config.validate();
        this.useGcsAccessToken = config.isUseGcsAccessToken();
        this.jsonKeyFilePath = Optional.ofNullable(config.getJsonKey())
                .map(GoogleGcsConfigurationInitializer::getJsonKeyFilePath)
                .orElse(config.getJsonKeyFilePath());
    }

    private static String getJsonKeyFilePath(String jsonKey)
    {
        try {
            // Just create a temporary json key file.
            Path tempFile = Files.createTempFile("gcs-key-", ".json", PosixFilePermissions.asFileAttribute(EnumSet.of(OWNER_READ, OWNER_WRITE)));
            tempFile.toFile().deleteOnExit();
            Files.writeString(tempFile, jsonKey, StandardCharsets.UTF_8);
            return tempFile.toString();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to create a temp file for the GCS JSON key", e);
        }
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
