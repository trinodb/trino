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
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.inject.Inject;
import io.trino.spi.security.ConnectorIdentity;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

import static com.google.cloud.storage.StorageRetryStrategy.getUniformStorageRetryStrategy;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_NO_AUTH_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_EXPIRES_AT_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_PROJECT_ID_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_SERVICE_HOST_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_USER_PROJECT_PROPERTY;
import static java.util.Objects.requireNonNull;

public class GcsStorageFactory
{
    public static final String GCS_OAUTH_KEY = "gcs.oauth";
    private final String projectId;
    private final Optional<String> endpoint;
    private final int maxRetries;
    private final double backoffScaleFactor;
    private final Duration maxRetryTime;
    private final Duration minBackoffDelay;
    private final Duration maxBackoffDelay;
    private final String applicationId;
    private final GcsAuth gcsAuth;

    @Inject
    public GcsStorageFactory(GcsFileSystemConfig config, GcsAuth gcsAuth)
    {
        this.gcsAuth = requireNonNull(gcsAuth, "gcsAuth is null");
        projectId = config.getProjectId();
        endpoint = config.getEndpoint();
        this.maxRetries = config.getMaxRetries();
        this.backoffScaleFactor = config.getBackoffScaleFactor();
        this.maxRetryTime = config.getMaxRetryTime().toJavaTime();
        this.minBackoffDelay = config.getMinBackoffDelay().toJavaTime();
        this.maxBackoffDelay = config.getMaxBackoffDelay().toJavaTime();
        this.applicationId = config.getApplicationId();
    }

    public Storage create(ConnectorIdentity identity)
    {
        try {
            Map<String, String> extraCredentials = identity.getExtraCredentials();
            boolean noAuth = Boolean.parseBoolean(extraCredentials.getOrDefault(EXTRA_CREDENTIALS_GCS_NO_AUTH_PROPERTY, "false"));
            String vendedOAuthToken = extraCredentials.get(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY);

            StorageOptions.Builder storageOptionsBuilder = StorageOptions.newBuilder();

            String effectiveProjectId = extraCredentials.getOrDefault(EXTRA_CREDENTIALS_GCS_PROJECT_ID_PROPERTY, projectId);
            if (effectiveProjectId != null) {
                storageOptionsBuilder.setProjectId(effectiveProjectId);
            }

            if (noAuth) {
                storageOptionsBuilder.setCredentials(NoCredentials.getInstance());
            }
            else if (vendedOAuthToken != null) {
                Date expirationTime = null;
                String expiresAt = extraCredentials.get(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_EXPIRES_AT_PROPERTY);
                if (expiresAt != null) {
                    expirationTime = new Date(Long.parseLong(expiresAt));
                }
                AccessToken accessToken = new AccessToken(vendedOAuthToken, expirationTime);
                storageOptionsBuilder.setCredentials(GoogleCredentials.create(accessToken));
            }
            else {
                gcsAuth.setAuth(storageOptionsBuilder, identity);
            }

            String vendedServiceHost = extraCredentials.get(EXTRA_CREDENTIALS_GCS_SERVICE_HOST_PROPERTY);
            if (vendedServiceHost != null) {
                storageOptionsBuilder.setHost(vendedServiceHost);
            }
            else {
                endpoint.ifPresent(storageOptionsBuilder::setHost);
            }

            String vendedUserProject = extraCredentials.get(EXTRA_CREDENTIALS_GCS_USER_PROJECT_PROPERTY);
            if (vendedUserProject != null) {
                storageOptionsBuilder.setQuotaProjectId(vendedUserProject);
            }

            // Note: without uniform strategy we cannot retry idempotent operations.
            // The trino-filesystem api does not violate the conditions for idempotency, see https://cloud.google.com/storage/docs/retry-strategy#java for details.
            return storageOptionsBuilder
                    .setStorageRetryStrategy(getUniformStorageRetryStrategy())
                    .setRetrySettings(RetrySettings.newBuilder()
                            .setMaxAttempts(maxRetries + 1)
                            .setRetryDelayMultiplier(backoffScaleFactor)
                            .setTotalTimeoutDuration(maxRetryTime)
                            .setInitialRetryDelayDuration(minBackoffDelay)
                            .setMaxRetryDelayDuration(maxBackoffDelay)
                            .build())
                    .setHeaderProvider(() -> Map.of(USER_AGENT, StorageOptions.getLibraryName() + "/" + StorageOptions.version() + " " + applicationId))
                    .build()
                    .getService();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
