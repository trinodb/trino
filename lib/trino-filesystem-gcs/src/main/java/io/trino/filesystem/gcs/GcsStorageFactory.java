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
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.auth.oauth2.OAuth2CredentialsWithRefresh;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.inject.Inject;
import io.trino.spi.security.ConnectorIdentity;
import jakarta.annotation.PreDestroy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.cloud.storage.StorageRetryStrategy.getUniformStorageRetryStrategy;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static io.trino.filesystem.gcs.GcsFileSystemConfig.AuthType.ACCESS_TOKEN;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_EXPIRES_AT_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_PROJECT_ID_PROPERTY;
import static java.util.Objects.requireNonNull;

public class GcsStorageFactory
{
    public static final String GCS_OAUTH_KEY = "gcs.oauth";
    private final GcsFileSystemConfig.AuthType authType;
    private final String projectId;
    private final Optional<String> endpoint;
    private final int maxRetries;
    private final double backoffScaleFactor;
    private final Duration maxRetryTime;
    private final Duration minBackoffDelay;
    private final Duration maxBackoffDelay;
    private final String applicationId;
    private final GcsAuth gcsAuth;
    private volatile Storage cachedStorage;

    @Inject
    public GcsStorageFactory(GcsFileSystemConfig config, GcsAuth gcsAuth)
    {
        this.gcsAuth = requireNonNull(gcsAuth, "gcsAuth is null");
        authType = config.getAuthType();
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
        return create(identity, Optional.empty());
    }

    public Storage create(ConnectorIdentity identity, Supplier<Map<String, String>> credentialsRefresher)
    {
        return create(identity, Optional.of(credentialsRefresher));
    }

    private Storage create(ConnectorIdentity identity, Optional<Supplier<Map<String, String>>> credentialsRefresher)
    {
        if (isCacheable(identity, credentialsRefresher)) {
            Storage storage = cachedStorage;
            if (storage == null) {
                synchronized (this) {
                    storage = cachedStorage;
                    if (storage == null) {
                        storage = createStorage(identity, credentialsRefresher);
                        cachedStorage = storage;
                    }
                }
            }
            return storage;
        }
        return createStorage(identity, credentialsRefresher);
    }

    @PreDestroy
    public void stop()
            throws Exception
    {
        Storage storage = cachedStorage;
        cachedStorage = null;
        if (storage != null) {
            storage.close();
        }
    }

    private boolean isCacheable(ConnectorIdentity identity, Optional<Supplier<Map<String, String>>> credentialsRefresher)
    {
        return authType != ACCESS_TOKEN && credentialsRefresher.isEmpty() && !identity.getExtraCredentials().containsKey(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY);
    }

    private Storage createStorage(ConnectorIdentity identity, Optional<Supplier<Map<String, String>>> credentialsRefresher)
    {
        try {
            StorageOptions.Builder storageOptionsBuilder = StorageOptions.newBuilder();

            if (!setOAuthCredentials(storageOptionsBuilder, identity, credentialsRefresher)) {
                if (projectId != null) {
                    storageOptionsBuilder.setProjectId(projectId);
                }
                gcsAuth.setAuth(storageOptionsBuilder, identity);
            }

            endpoint.ifPresent(storageOptionsBuilder::setHost);

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

    private boolean setOAuthCredentials(StorageOptions.Builder builder, ConnectorIdentity identity, Optional<Supplier<Map<String, String>>> credentialsRefresher)
    {
        if (identity.getExtraCredentials().containsKey(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY)) {
            builder.setCredentials(oauthCredentials(identity.getExtraCredentials(), credentialsRefresher));

            String effectiveProjectId = identity.getExtraCredentials().getOrDefault(EXTRA_CREDENTIALS_GCS_PROJECT_ID_PROPERTY, projectId);
            if (effectiveProjectId != null) {
                builder.setProjectId(effectiveProjectId);
            }
            return true;
        }
        checkArgument(credentialsRefresher.isEmpty(), "Credentials refresher requires vended GCS OAuth token in extra credentials");
        return false;
    }

    private static OAuth2Credentials oauthCredentials(Map<String, String> extraCredentials, Optional<Supplier<Map<String, String>>> credentialsRefresher)
    {
        if (credentialsRefresher.isEmpty() || !extraCredentials.containsKey(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_EXPIRES_AT_PROPERTY)) {
            return GoogleCredentials.create(toAccessToken(extraCredentials));
        }
        Supplier<Map<String, String>> refresher = credentialsRefresher.get();
        return OAuth2CredentialsWithRefresh.newBuilder()
                .setAccessToken(toAccessToken(extraCredentials))
                .setRefreshHandler(() -> toAccessToken(refresher.get()))
                .build();
    }

    private static AccessToken toAccessToken(Map<String, String> extraCredentials)
    {
        String accessToken = extraCredentials.get(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY);
        Optional<Date> expireAt = Optional.ofNullable(extraCredentials.get(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_EXPIRES_AT_PROPERTY))
                .map(Long::parseLong)
                .map(Instant::ofEpochMilli)
                .map(Date::from);
        return new AccessToken(accessToken, expireAt.orElse(null));
    }
}
