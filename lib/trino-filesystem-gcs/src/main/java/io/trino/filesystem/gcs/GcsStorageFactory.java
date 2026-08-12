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
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableMap;
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
    private static final String AUDIT_QUERY_ID_HEADER = "x-goog-custom-audit-trino-query-id";
    private static final String AUDIT_USER_HEADER = "x-goog-custom-audit-trino-user";

    private final GcsFileSystemConfig.AuthType authType;
    private final String projectId;
    private final Optional<String> endpoint;
    private final int maxRetries;
    private final double backoffScaleFactor;
    private final Duration maxRetryTime;
    private final Duration minBackoffDelay;
    private final Duration maxBackoffDelay;
    private final String applicationId;
    private final boolean customAuditHeadersEnabled;
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
        this.customAuditHeadersEnabled = config.isCustomAuditHeadersEnabled();
    }

    public Storage create(ConnectorIdentity identity)
    {
        return create(identity, Optional.empty());
    }

    public Storage create(ConnectorIdentity identity, Optional<String> queryId)
    {
        // A Storage instance carrying a per-query audit header must not be cached and reused across queries.
        boolean carriesPerQueryAuditHeaders = customAuditHeadersEnabled && queryId.isPresent();
        if (!carriesPerQueryAuditHeaders && isCacheable(identity)) {
            Storage storage = cachedStorage;
            if (storage == null) {
                synchronized (this) {
                    storage = cachedStorage;
                    if (storage == null) {
                        storage = createStorage(identity, queryId);
                        cachedStorage = storage;
                    }
                }
            }
            return storage;
        }
        return createStorage(identity, queryId);
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

    private Storage createStorage(ConnectorIdentity identity, Optional<String> queryId)
    {
        try {
            StorageOptions.Builder storageOptionsBuilder = StorageOptions.newBuilder();

            if (!setOAuthCredentials(storageOptionsBuilder, identity)) {
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
                    .setHeaderProvider(() -> buildHeaders(identity, queryId))
                    .build()
                    .getService();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Map<String, String> buildHeaders(ConnectorIdentity identity, Optional<String> queryId)
    {
        String userAgent = StorageOptions.getLibraryName() + "/" + StorageOptions.version() + " " + applicationId;
        if (!customAuditHeadersEnabled || queryId.isEmpty()) {
            return ImmutableMap.of(USER_AGENT, userAgent);
        }
        // Custom audit headers (x-goog-custom-audit-*) are surfaced in GCS Cloud Audit Logs.
        // See: https://cloud.google.com/storage/docs/audit-logging#custom-audit-info
        return ImmutableMap.of(
                USER_AGENT, userAgent,
                AUDIT_QUERY_ID_HEADER, queryId.get(),
                AUDIT_USER_HEADER, identity.getUser());
    }

    private boolean setOAuthCredentials(StorageOptions.Builder builder, ConnectorIdentity identity)
    {
        if (identity.getExtraCredentials().containsKey(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY)) {
            String accessToken = identity.getExtraCredentials().get(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY);
            Optional<Date> expireAt = Optional.ofNullable(identity.getExtraCredentials().get(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_EXPIRES_AT_PROPERTY))
                    .map(Long::parseLong)
                    .map(Instant::ofEpochMilli)
                    .map(Date::from);
            builder.setCredentials(GoogleCredentials.create(new AccessToken(accessToken, expireAt.orElse(null))));

            String effectiveProjectId = identity.getExtraCredentials().getOrDefault(EXTRA_CREDENTIALS_GCS_PROJECT_ID_PROPERTY, projectId);
            if (effectiveProjectId != null) {
                builder.setProjectId(effectiveProjectId);
            }
            return true;
        }
        return false;
    }
}
