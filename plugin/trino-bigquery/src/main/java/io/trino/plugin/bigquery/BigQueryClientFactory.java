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
package io.trino.plugin.bigquery;

import com.google.api.gax.rpc.HeaderProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.trino.spi.connector.ConnectorSession;

import javax.inject.Inject;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class BigQueryClientFactory
{
    private final IdentityCacheMapping identityCacheMapping;
    private final BigQueryCredentialsSupplier credentialsSupplier;
    private final BigQueryConfig bigQueryConfig;
    private final ViewMaterializationCache materializationCache;
    private final HeaderProvider headerProvider;
    private final Cache<IdentityCacheMapping.IdentityCacheKey, BigQueryClient> clientCache;

    @Inject
    public BigQueryClientFactory(
            IdentityCacheMapping identityCacheMapping,
            BigQueryCredentialsSupplier credentialsSupplier,
            BigQueryConfig bigQueryConfig,
            ViewMaterializationCache materializationCache,
            HeaderProvider headerProvider)
    {
        this.identityCacheMapping = requireNonNull(identityCacheMapping, "identityCacheMapping is null");
        this.credentialsSupplier = requireNonNull(credentialsSupplier, "credentialsSupplier is null");
        this.bigQueryConfig = requireNonNull(bigQueryConfig, "bigQueryConfig is null");
        this.materializationCache = requireNonNull(materializationCache, "materializationCache is null");
        this.headerProvider = requireNonNull(headerProvider, "headerProvider is null");

        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder()
                .expireAfterWrite(bigQueryConfig.getServiceCacheTtl().toMillis(), MILLISECONDS);

        clientCache = cacheBuilder.build();
    }

    public BigQueryClient create(ConnectorSession session)
    {
        IdentityCacheMapping.IdentityCacheKey cacheKey = identityCacheMapping.getRemoteUserCacheKey(session);

        try {
            return clientCache.get(cacheKey, () -> createBigQueryClient(session));
        }
        catch (ExecutionException e) {
            return createBigQueryClient(session);
        }
    }

    protected BigQueryClient createBigQueryClient(ConnectorSession session)
    {
        return new BigQueryClient(createBigQuery(session), bigQueryConfig, materializationCache);
    }

    protected BigQuery createBigQuery(ConnectorSession session)
    {
        Optional<Credentials> credentials = credentialsSupplier.getCredentials(session);
        String billingProjectId = calculateBillingProjectId(bigQueryConfig.getParentProjectId(), credentials);
        BigQueryOptions.Builder options = BigQueryOptions.newBuilder()
                .setHeaderProvider(headerProvider)
                .setProjectId(billingProjectId);
        credentials.ifPresent(options::setCredentials);
        return options.build().getService();
    }

    // Note that at this point the config has been validated, which means that option 2 or option 3 will always be valid
    static String calculateBillingProjectId(Optional<String> configParentProjectId, Optional<Credentials> credentials)
    {
        // 1. Get from configuration
        return configParentProjectId
                // 2. Get from the provided credentials, but only ServiceAccountCredentials contains the project id.
                // All other credentials types (User, AppEngine, GCE, CloudShell, etc.) take it from the environment
                .orElseGet(() -> credentials
                        .filter(ServiceAccountCredentials.class::isInstance)
                        .map(ServiceAccountCredentials.class::cast)
                        .map(ServiceAccountCredentials::getProjectId)
                        // 3. No configuration was provided, so get the default from the environment
                        .orElseGet(BigQueryOptions::getDefaultProjectId));
    }
}
