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
package io.trino.filesystem.gcs;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.security.ConnectorIdentity;
import org.threeten.bp.Duration;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;

import static com.google.cloud.storage.StorageRetryStrategy.getUniformStorageRetryStrategy;
import static com.google.common.base.Strings.nullToEmpty;
import static java.nio.charset.StandardCharsets.UTF_8;

public class GcsStorageFactory
{
    public static final String GCS_OAUTH_KEY = "gcs.oauth";
    public static final List<String> DEFAULT_SCOPES = ImmutableList.of("https://www.googleapis.com/auth/cloud-platform");
    private final String projectId;
    private final boolean useGcsAccessToken;
    private final Optional<GoogleCredentials> jsonGoogleCredential;
    private final int maxRetries;
    private final double backoffScaleFactor;
    private final Duration maxRetryTime;
    private final Duration minBackoffDelay;
    private final Duration maxBackoffDelay;

    @Inject
    public GcsStorageFactory(GcsFileSystemConfig config)
            throws IOException
    {
        config.validate();
        projectId = config.getProjectId();
        useGcsAccessToken = config.isUseGcsAccessToken();
        String jsonKey = config.getJsonKey();
        String jsonKeyFilePath = config.getJsonKeyFilePath();
        if (jsonKey != null) {
            try (InputStream inputStream = new ByteArrayInputStream(jsonKey.getBytes(UTF_8))) {
                jsonGoogleCredential = Optional.of(GoogleCredentials.fromStream(inputStream).createScoped(DEFAULT_SCOPES));
            }
        }
        else if (jsonKeyFilePath != null) {
            try (FileInputStream inputStream = new FileInputStream(jsonKeyFilePath)) {
                jsonGoogleCredential = Optional.of(GoogleCredentials.fromStream(inputStream).createScoped(DEFAULT_SCOPES));
            }
        }
        else {
            jsonGoogleCredential = Optional.empty();
        }
        this.maxRetries = config.getMaxRetries();
        this.backoffScaleFactor = config.getBackoffScaleFactor();
        // To avoid name collision by importing io.airlift.Duration
        this.maxRetryTime = Duration.ofMillis(config.getMaxRetryTime().toMillis());
        this.minBackoffDelay = Duration.ofMillis(config.getMinBackoffDelay().toMillis());
        this.maxBackoffDelay = Duration.ofMillis(config.getMaxBackoffDelay().toMillis());
    }

    public Storage create(ConnectorIdentity identity)
    {
        try {
            GoogleCredentials credentials;
            if (useGcsAccessToken) {
                String accessToken = nullToEmpty(identity.getExtraCredentials().get(GCS_OAUTH_KEY));
                try (ByteArrayInputStream inputStream = new ByteArrayInputStream(accessToken.getBytes(UTF_8))) {
                    credentials = GoogleCredentials.fromStream(inputStream).createScoped(DEFAULT_SCOPES);
                }
            }
            else {
                credentials = jsonGoogleCredential.orElseGet(() -> {
                    try {
                        return GoogleCredentials.getApplicationDefault();
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }
            StorageOptions.Builder storageOptionsBuilder = StorageOptions.newBuilder();
            if (projectId != null) {
                storageOptionsBuilder.setProjectId(projectId);
            }
            // Note: without uniform strategy we cannot retry idempotent operations.
            // The trino-filesystem api does not violate the conditions for idempotency, see https://cloud.google.com/storage/docs/retry-strategy#java for details.
            return storageOptionsBuilder
                    .setCredentials(credentials)
                    .setStorageRetryStrategy(getUniformStorageRetryStrategy())
                    .setRetrySettings(RetrySettings.newBuilder()
                            .setMaxAttempts(maxRetries + 1)
                            .setRetryDelayMultiplier(backoffScaleFactor)
                            .setTotalTimeout(maxRetryTime)
                            .setInitialRetryDelay(minBackoffDelay)
                            .setMaxRetryDelay(maxBackoffDelay)
                            .build())
                    .build()
                    .getService();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
