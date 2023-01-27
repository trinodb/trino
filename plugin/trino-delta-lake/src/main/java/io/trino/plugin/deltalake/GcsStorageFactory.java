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
package io.trino.plugin.deltalake;

import com.google.cloud.hadoop.fs.gcs.TrinoGoogleHadoopFileSystemConfiguration;
import com.google.cloud.hadoop.repackaged.gcs.com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.cloud.hadoop.repackaged.gcs.com.google.api.client.http.HttpTransport;
import com.google.cloud.hadoop.repackaged.gcs.com.google.api.client.json.jackson2.JacksonFactory;
import com.google.cloud.hadoop.repackaged.gcs.com.google.api.services.storage.Storage;
import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.hadoop.util.CredentialFactory;
import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.hadoop.util.HttpTransportFactory;
import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.hadoop.util.RetryHttpInitializer;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.plugin.hive.gcs.HiveGcsConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.plugin.hive.gcs.GcsConfigurationProvider.GCS_OAUTH_KEY;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class GcsStorageFactory
{
    private static final String APPLICATION_NAME = "Trino-Delta-Lake";

    private final HdfsEnvironment hdfsEnvironment;
    private final boolean useGcsAccessToken;
    @Nullable
    private final Optional<GoogleCredential> jsonGoogleCredential;

    @Inject
    public GcsStorageFactory(HdfsEnvironment hdfsEnvironment, HiveGcsConfig hiveGcsConfig)
            throws IOException
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.useGcsAccessToken = hiveGcsConfig.isUseGcsAccessToken();
        String jsonKeyFilePath = hiveGcsConfig.getJsonKeyFilePath();
        if (jsonKeyFilePath != null) {
            try (FileInputStream inputStream = new FileInputStream(jsonKeyFilePath)) {
                jsonGoogleCredential = Optional.of(GoogleCredential.fromStream(inputStream).createScoped(CredentialFactory.DEFAULT_SCOPES));
            }
        }
        else {
            jsonGoogleCredential = Optional.empty();
        }
    }

    public Storage create(ConnectorSession session, Path path)
    {
        try {
            GoogleCloudStorageOptions gcsOptions = TrinoGoogleHadoopFileSystemConfiguration.getGcsOptionsBuilder(hdfsEnvironment.getConfiguration(new HdfsContext(session), path)).build();
            HttpTransport httpTransport = HttpTransportFactory.createHttpTransport(
                    gcsOptions.getTransportType(),
                    gcsOptions.getProxyAddress(),
                    gcsOptions.getProxyUsername(),
                    gcsOptions.getProxyPassword(),
                    Duration.ofMillis(gcsOptions.getHttpRequestReadTimeout()));
            GoogleCredential credential;
            if (useGcsAccessToken) {
                String accessToken = nullToEmpty(session.getIdentity().getExtraCredentials().get(GCS_OAUTH_KEY));
                try (ByteArrayInputStream inputStream = new ByteArrayInputStream(accessToken.getBytes(UTF_8))) {
                    credential = GoogleCredential.fromStream(inputStream).createScoped(CredentialFactory.DEFAULT_SCOPES);
                }
            }
            else {
                credential = jsonGoogleCredential.get();
            }
            return new Storage.Builder(httpTransport, JacksonFactory.getDefaultInstance(), new RetryHttpInitializer(credential, APPLICATION_NAME))
                    .setApplicationName(APPLICATION_NAME)
                    .build();
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }
}
