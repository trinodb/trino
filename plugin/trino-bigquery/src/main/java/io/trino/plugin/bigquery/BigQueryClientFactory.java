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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.common.cache.CacheBuilder;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.cache.NonEvictableCache;
import io.trino.spi.connector.ConnectorSession;

import java.util.Optional;
import java.util.Set;

import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class BigQueryClientFactory
{
    private final IdentityCacheMapping identityCacheMapping;
    private final Optional<String> projectId;
    private final boolean caseInsensitiveNameMatching;
    private final ViewMaterializationCache materializationCache;
    private final BigQueryLabelFactory labelFactory;

    private final NonEvictableCache<IdentityCacheMapping.IdentityCacheKey, BigQueryClient> clientCache;
    private final Duration metadataCacheTtl;
    private final Set<BigQueryOptionsConfigurer> optionsConfigurers;

    @Inject
    public BigQueryClientFactory(
            IdentityCacheMapping identityCacheMapping,
            BigQueryConfig bigQueryConfig,
            ViewMaterializationCache materializationCache,
            BigQueryLabelFactory labelFactory,
            Set<BigQueryOptionsConfigurer> optionsConfigurers)
    {
        this.identityCacheMapping = requireNonNull(identityCacheMapping, "identityCacheMapping is null");
        requireNonNull(bigQueryConfig, "bigQueryConfig is null");
        this.projectId = bigQueryConfig.getProjectId();
        this.caseInsensitiveNameMatching = bigQueryConfig.isCaseInsensitiveNameMatching();
        this.materializationCache = requireNonNull(materializationCache, "materializationCache is null");
        this.labelFactory = requireNonNull(labelFactory, "labelFactory is null");
        this.metadataCacheTtl = bigQueryConfig.getMetadataCacheTtl();
        this.optionsConfigurers = requireNonNull(optionsConfigurers, "optionsConfigurers is null");

        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder()
                .expireAfterWrite(bigQueryConfig.getServiceCacheTtl().toMillis(), MILLISECONDS);

        clientCache = buildNonEvictableCache(cacheBuilder);
    }

    public BigQueryClient create(ConnectorSession session)
    {
        IdentityCacheMapping.IdentityCacheKey cacheKey = identityCacheMapping.getRemoteUserCacheKey(session);
        return uncheckedCacheGet(clientCache, cacheKey, () -> createBigQueryClient(session));
    }

    protected BigQueryClient createBigQueryClient(ConnectorSession session)
    {
        return new BigQueryClient(createBigQuery(session), labelFactory, caseInsensitiveNameMatching, materializationCache, metadataCacheTtl, projectId);
    }

    protected BigQuery createBigQuery(ConnectorSession session)
    {
        BigQueryOptions.Builder options = BigQueryOptions.newBuilder();
        for (BigQueryOptionsConfigurer configurer : optionsConfigurers) {
            options = configurer.configure(options, session);
        }
        return options.build().getService();
    }
}
