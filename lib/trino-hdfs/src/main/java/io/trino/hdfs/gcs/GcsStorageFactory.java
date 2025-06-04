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

import com.google.cloud.hadoop.repackaged.gcs.com.google.api.client.http.HttpTransport;
import com.google.cloud.hadoop.repackaged.gcs.com.google.api.client.json.jackson2.JacksonFactory;
import com.google.cloud.hadoop.repackaged.gcs.com.google.api.services.storage.Storage;
import com.google.cloud.hadoop.repackaged.gcs.com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.hadoop.util.HttpTransportFactory;
import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.hadoop.util.RetryHttpInitializer;
import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.hadoop.util.RetryHttpInitializerOptions;
import com.google.inject.Inject;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import org.apache.hadoop.fs.Path;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Optional;

import static com.google.cloud.hadoop.fs.gcs.TrinoGoogleHadoopFileSystemConfiguration.getGcsOptionsBuilder;
import static com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.CLOUD_PLATFORM_SCOPE;
import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.hdfs.gcs.GcsConfigurationProvider.GCS_OAUTH_KEY;
import static java.nio.charset.StandardCharsets.UTF_8;

public class GcsStorageFactory
{
    private static final String APPLICATION_NAME = "Trino";

    private final boolean useGcsAccessToken;
    private final Optional<GoogleCredentials> jsonGoogleCredential;

    @Inject
    public GcsStorageFactory(HiveGcsConfig hiveGcsConfig)
            throws IOException
    {
        hiveGcsConfig.validate();
        this.useGcsAccessToken = hiveGcsConfig.isUseGcsAccessToken();
        String jsonKey = hiveGcsConfig.getJsonKey();
        String jsonKeyFilePath = hiveGcsConfig.getJsonKeyFilePath();
        if (jsonKey != null) {
            try (InputStream inputStream = new ByteArrayInputStream(jsonKey.getBytes(UTF_8))) {
                jsonGoogleCredential = Optional.of(GoogleCredentials.fromStream(inputStream).createScoped(CLOUD_PLATFORM_SCOPE));
            }
        }
        else if (jsonKeyFilePath != null) {
            try (FileInputStream inputStream = new FileInputStream(jsonKeyFilePath)) {
                jsonGoogleCredential = Optional.of(GoogleCredentials.fromStream(inputStream).createScoped(CLOUD_PLATFORM_SCOPE));
            }
        }
        else {
            jsonGoogleCredential = Optional.empty();
        }
    }

    public Storage create(HdfsEnvironment environment, HdfsContext context, Path path)
    {
        try {
            GoogleCloudStorageOptions gcsOptions = getGcsOptionsBuilder(environment.getConfiguration(context, path)).build();
            HttpTransport httpTransport = HttpTransportFactory.createHttpTransport(
                    gcsOptions.getProxyAddress(),
                    gcsOptions.getProxyUsername(),
                    gcsOptions.getProxyPassword());
            GoogleCredentials credential;
            if (useGcsAccessToken) {
                String accessToken = nullToEmpty(context.getIdentity().getExtraCredentials().get(GCS_OAUTH_KEY));
                try (ByteArrayInputStream inputStream = new ByteArrayInputStream(accessToken.getBytes(UTF_8))) {
                    credential = GoogleCredentials.fromStream(inputStream).createScoped(CLOUD_PLATFORM_SCOPE);
                }
            }
            else {
                credential = jsonGoogleCredential.orElseThrow(() -> new IllegalStateException("GCS credentials not configured"));
            }
            return new Storage.Builder(httpTransport, JacksonFactory.getDefaultInstance(), new RetryHttpInitializer(credential, RetryHttpInitializerOptions.builder()
                        .setReadTimeout(gcsOptions.getHttpRequestReadTimeout())
                        .setMaxRequestRetries(gcsOptions.getMaxHttpRequestRetries())
                    .build()))
                    .setApplicationName(APPLICATION_NAME)
                    .build();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
