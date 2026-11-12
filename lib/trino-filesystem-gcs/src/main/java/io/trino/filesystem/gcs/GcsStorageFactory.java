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
import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
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
        if (isCacheable(identity)) {
            Storage storage = cachedStorage;
            if (storage == null) {
                synchronized (this) {
                    storage = cachedStorage;
                    if (storage == null) {
                        storage = createStorage(identity);
                        cachedStorage = storage;
                    }
                }
            }
            return storage;
        }
        return createStorage(identity);
    }

    public Storage create(Supplier<ConnectorIdentity> identitySupplier)
    {
        ConnectorIdentity identity = identitySupplier.get();
        if (!identity.getExtraCredentials().containsKey(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY)) {
            return create(identity);
        }
        StorageOptions.Builder builder = StorageOptions.newBuilder();
        setOAuthCredentials(builder, identity, new IdentityOAuthCredentials(identitySupplier));
        return createStorage(builder);
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

    private boolean isCacheable(ConnectorIdentity identity)
    {
        return authType != ACCESS_TOKEN && !identity.getExtraCredentials().containsKey(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY);
    }

    private Storage createStorage(ConnectorIdentity identity)
    {
        StorageOptions.Builder builder = StorageOptions.newBuilder();
        if (identity.getExtraCredentials().containsKey(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY)) {
            setOAuthCredentials(builder, identity, GoogleCredentials.create(toAccessToken(identity.getExtraCredentials())));
        }
        else {
            if (projectId != null) {
                builder.setProjectId(projectId);
            }
            try {
                gcsAuth.setAuth(builder, identity);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return createStorage(builder);
    }

    private Storage createStorage(StorageOptions.Builder builder)
    {
        endpoint.ifPresent(builder::setHost);

        // Note: without uniform strategy we cannot retry idempotent operations.
        // The trino-filesystem api does not violate the conditions for idempotency, see https://cloud.google.com/storage/docs/retry-strategy#java for details.
        return builder
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

    private void setOAuthCredentials(StorageOptions.Builder builder, ConnectorIdentity identity, Credentials credentials)
    {
        builder.setCredentials(credentials);
        String effectiveProjectId = identity.getExtraCredentials().getOrDefault(EXTRA_CREDENTIALS_GCS_PROJECT_ID_PROPERTY, projectId);
        if (effectiveProjectId != null) {
            builder.setProjectId(effectiveProjectId);
        }
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
