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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigHidden;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import io.trino.plugin.base.logging.SessionInterpolatedValues;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

import static io.trino.plugin.base.logging.FormatInterpolator.hasValidPlaceholders;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

@DefunctConfig("bigquery.parallelism")
public class BigQueryConfig
{
    public static final int DEFAULT_MAX_READ_ROWS_RETRIES = 3;
    public static final String VIEWS_ENABLED = "bigquery.views-enabled";
    public static final String ARROW_SERIALIZATION_ENABLED = "bigquery.arrow-serialization.enabled";

    private Optional<String> projectId = Optional.empty();
    private Optional<String> parentProjectId = Optional.empty();
    private boolean viewsEnabled;
    private boolean arrowSerializationEnabled = true;
    private Duration viewExpireDuration = new Duration(24, HOURS);
    private boolean skipViewMaterialization;
    private boolean viewMaterializationWithFilter;
    private Optional<String> viewMaterializationProject = Optional.empty();
    private Optional<String> viewMaterializationDataset = Optional.empty();
    private int maxReadRowsRetries = DEFAULT_MAX_READ_ROWS_RETRIES;
    private int metadataPageSize = 1000;
    private boolean caseInsensitiveNameMatching;
    private Duration caseInsensitiveNameMatchingCacheTtl = new Duration(0, MILLISECONDS);
    private Duration viewsCacheTtl = new Duration(15, MINUTES);
    private Duration serviceCacheTtl = new Duration(3, MINUTES);
    private Duration metadataCacheTtl = new Duration(0, MILLISECONDS);
    @Deprecated
    private boolean isLegacyMetadataListing;
    private boolean queryResultsCacheEnabled;
    private String queryLabelName;
    private String queryLabelFormat;
    private boolean proxyEnabled;
    private boolean projectionPushDownEnabled = true;
    private int metadataParallelism = Runtime.getRuntime().availableProcessors();

    public Optional<String> getProjectId()
    {
        return projectId;
    }

    @Config("bigquery.project-id")
    @ConfigDescription("The Google Cloud Project ID where the data reside")
    public BigQueryConfig setProjectId(String projectId)
    {
        this.projectId = Optional.ofNullable(projectId);
        return this;
    }

    public Optional<String> getParentProjectId()
    {
        return parentProjectId;
    }

    @Config("bigquery.parent-project-id")
    @ConfigDescription("The Google Cloud Project ID to bill for the export")
    public BigQueryConfig setParentProjectId(String parentProjectId)
    {
        this.parentProjectId = Optional.ofNullable(parentProjectId);
        return this;
    }

    public boolean isViewsEnabled()
    {
        return viewsEnabled;
    }

    @Config(VIEWS_ENABLED)
    @ConfigDescription("Enables the connector to read from views and not only tables")
    public BigQueryConfig setViewsEnabled(boolean viewsEnabled)
    {
        this.viewsEnabled = viewsEnabled;
        return this;
    }

    public boolean isArrowSerializationEnabled()
    {
        return arrowSerializationEnabled;
    }

    @Config(ARROW_SERIALIZATION_ENABLED)
    @LegacyConfig("bigquery.experimental.arrow-serialization.enabled")
    @ConfigDescription("Enables Arrow serialization while reading data")
    public BigQueryConfig setArrowSerializationEnabled(boolean arrowSerializationEnabled)
    {
        this.arrowSerializationEnabled = arrowSerializationEnabled;
        return this;
    }

    @NotNull
    public Duration getViewExpireDuration()
    {
        return viewExpireDuration;
    }

    @Config("bigquery.view-expire-duration")
    public BigQueryConfig setViewExpireDuration(Duration viewExpireDuration)
    {
        this.viewExpireDuration = viewExpireDuration;
        return this;
    }

    public boolean isSkipViewMaterialization()
    {
        return skipViewMaterialization;
    }

    @Config("bigquery.skip-view-materialization")
    @ConfigDescription("Skip materializing views")
    public BigQueryConfig setSkipViewMaterialization(boolean skipViewMaterialization)
    {
        this.skipViewMaterialization = skipViewMaterialization;
        return this;
    }

    public boolean isViewMaterializationWithFilter()
    {
        return viewMaterializationWithFilter;
    }

    @Config("bigquery.view-materialization-with-filter")
    @ConfigDescription("Use filter when materializing views")
    public BigQueryConfig setViewMaterializationWithFilter(boolean viewMaterializationWithFilter)
    {
        this.viewMaterializationWithFilter = viewMaterializationWithFilter;
        return this;
    }

    public Optional<String> getViewMaterializationProject()
    {
        return viewMaterializationProject;
    }

    @Config("bigquery.view-materialization-project")
    @ConfigDescription("The project where the materialized view is going to be created")
    public BigQueryConfig setViewMaterializationProject(String viewMaterializationProject)
    {
        this.viewMaterializationProject = Optional.ofNullable(viewMaterializationProject);
        return this;
    }

    public Optional<String> getViewMaterializationDataset()
    {
        return viewMaterializationDataset;
    }

    @Config("bigquery.view-materialization-dataset")
    @ConfigDescription("The dataset where the materialized view is going to be created")
    public BigQueryConfig setViewMaterializationDataset(String viewMaterializationDataset)
    {
        this.viewMaterializationDataset = Optional.ofNullable(viewMaterializationDataset);
        return this;
    }

    @Min(0)
    public int getMaxReadRowsRetries()
    {
        return maxReadRowsRetries;
    }

    @Config("bigquery.max-read-rows-retries")
    @ConfigDescription("The number of retries in case of retryable server issues")
    public BigQueryConfig setMaxReadRowsRetries(int maxReadRowsRetries)
    {
        this.maxReadRowsRetries = maxReadRowsRetries;
        return this;
    }

    @Min(1)
    public int getMetadataPageSize()
    {
        return metadataPageSize;
    }

    @Config("bigquery.metadata-page-size")
    @ConfigDescription("The number of metadata entries retrieved per API request")
    public BigQueryConfig setMetadataPageSize(int metadataPageSize)
    {
        this.metadataPageSize = metadataPageSize;
        return this;
    }

    public boolean isCaseInsensitiveNameMatching()
    {
        return caseInsensitiveNameMatching;
    }

    @Config("bigquery.case-insensitive-name-matching")
    @ConfigDescription("Match dataset and table names case-insensitively")
    public BigQueryConfig setCaseInsensitiveNameMatching(boolean caseInsensitiveNameMatching)
    {
        this.caseInsensitiveNameMatching = caseInsensitiveNameMatching;
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getCaseInsensitiveNameMatchingCacheTtl()
    {
        return caseInsensitiveNameMatchingCacheTtl;
    }

    @Config("bigquery.case-insensitive-name-matching.cache-ttl")
    @ConfigDescription("Duration for which case insensitive schema and table names are cached. Set to 0ms to disable the cache.")
    public BigQueryConfig setCaseInsensitiveNameMatchingCacheTtl(Duration caseInsensitiveNameMatchingCacheTtl)
    {
        this.caseInsensitiveNameMatchingCacheTtl = caseInsensitiveNameMatchingCacheTtl;
        return this;
    }

    @NotNull
    @MinDuration("0m")
    public Duration getViewsCacheTtl()
    {
        return viewsCacheTtl;
    }

    @Config("bigquery.views-cache-ttl")
    @ConfigDescription("Duration for which the materialization of a view will be cached and reused")
    public BigQueryConfig setViewsCacheTtl(Duration viewsCacheTtl)
    {
        this.viewsCacheTtl = viewsCacheTtl;
        return this;
    }

    @NotNull
    @MinDuration("0m")
    public Duration getServiceCacheTtl()
    {
        return serviceCacheTtl;
    }

    @ConfigHidden
    @Config("bigquery.service-cache-ttl")
    @ConfigDescription("Duration for which BigQuery client service instances are cached")
    public BigQueryConfig setServiceCacheTtl(Duration serviceCacheTtl)
    {
        this.serviceCacheTtl = serviceCacheTtl;
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getMetadataCacheTtl()
    {
        return metadataCacheTtl;
    }

    @Config("bigquery.metadata.cache-ttl")
    @ConfigDescription("Duration for which BigQuery client metadata is cached after listing")
    public BigQueryConfig setMetadataCacheTtl(Duration metadataCacheTtl)
    {
        this.metadataCacheTtl = metadataCacheTtl;
        return this;
    }

    public boolean isLegacyMetadataListing()
    {
        return isLegacyMetadataListing;
    }

    @Config("bigquery.legacy-metadata-listing")
    @ConfigHidden
    @ConfigDescription("Call BigQuery REST API per table when listing metadata")
    public BigQueryConfig setLegacyMetadataListing(boolean legacyMetadataListing)
    {
        isLegacyMetadataListing = legacyMetadataListing;
        return this;
    }

    public boolean isQueryResultsCacheEnabled()
    {
        return queryResultsCacheEnabled;
    }

    @Config("bigquery.query-results-cache.enabled")
    public BigQueryConfig setQueryResultsCacheEnabled(boolean queryResultsCacheEnabled)
    {
        this.queryResultsCacheEnabled = queryResultsCacheEnabled;
        return this;
    }

    public String getQueryLabelFormat()
    {
        return queryLabelFormat;
    }

    @Config("bigquery.job.label-format")
    @ConfigDescription("Adds `bigquery.job.label-name` label to the BigQuery job with provided value format")
    public BigQueryConfig setQueryLabelFormat(String queryLabelFormat)
    {
        this.queryLabelFormat = queryLabelFormat;
        return this;
    }

    @AssertTrue(message = "Incorrect bigquery.job.label-format may consist of only letters, digits, underscores, commas, spaces, equal signs and predefined values")
    boolean isQueryLabelFormatValid()
    {
        return queryLabelFormat == null || hasValidPlaceholders(queryLabelFormat, SessionInterpolatedValues.values());
    }

    public String getQueryLabelName()
    {
        return queryLabelName;
    }

    @Config("bigquery.job.label-name")
    @ConfigDescription("Adds label with the given name to the BigQuery job")
    public BigQueryConfig setQueryLabelName(String queryLabelName)
    {
        this.queryLabelName = queryLabelName;
        return this;
    }

    public boolean isProxyEnabled()
    {
        return proxyEnabled;
    }

    @Config("bigquery.rpc-proxy.enabled")
    @ConfigDescription("Enables proxying of RPC and gRPC requests to BigQuery APIs")
    public BigQueryConfig setProxyEnabled(boolean proxyEnabled)
    {
        this.proxyEnabled = proxyEnabled;
        return this;
    }

    public boolean isProjectionPushdownEnabled()
    {
        return projectionPushDownEnabled;
    }

    @Config("bigquery.projection-pushdown-enabled")
    @ConfigDescription("Dereference push down for ROW type")
    public BigQueryConfig setProjectionPushdownEnabled(boolean projectionPushDownEnabled)
    {
        this.projectionPushDownEnabled = projectionPushDownEnabled;
        return this;
    }

    @Min(1)
    @Max(32)
    public int getMetadataParallelism()
    {
        return metadataParallelism;
    }

    @ConfigDescription("Limits metadata enumeration calls parallelism")
    @Config("bigquery.metadata.parallelism")
    public BigQueryConfig setMetadataParallelism(int metadataParallelism)
    {
        this.metadataParallelism = metadataParallelism;
        return this;
    }

    @AssertTrue(message = "View expiration duration must be longer than view cache TTL")
    public boolean isValidViewExpireDuration()
    {
        return viewExpireDuration.toMillis() > viewsCacheTtl.toMillis();
    }

    @AssertTrue(message = VIEWS_ENABLED + " config property must be enabled when bigquery.skip-view-materialization is enabled")
    public boolean isValidViewsWehnEnabledSkipViewMaterialization()
    {
        return !skipViewMaterialization || viewsEnabled;
    }

    @AssertTrue(message = VIEWS_ENABLED + " config property must be enabled when bigquery.view-materialization-with-filter is enabled")
    public boolean isValidViewsEnableWhenViewMaterializationWithFilter()
    {
        return !viewMaterializationWithFilter || viewsEnabled;
    }

    @AssertTrue(message = "bigquery.case-insensitive-name-matching config must be enabled when bigquery.case-insensitive-name-matching.cache-ttl is set")
    public boolean isValidCaseInsensitiveNameMatchingCacheTtl()
    {
        return caseInsensitiveNameMatchingCacheTtl.isZero() || caseInsensitiveNameMatching;
    }
}
