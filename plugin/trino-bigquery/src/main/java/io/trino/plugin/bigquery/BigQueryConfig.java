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
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import io.trino.plugin.base.logging.SessionInterpolatedValues;
import jakarta.annotation.PostConstruct;

import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.base.logging.FormatInterpolator.hasValidPlaceholders;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

@DefunctConfig("bigquery.case-insensitive-name-matching.cache-ttl")
public class BigQueryConfig
{
    public static final int DEFAULT_MAX_READ_ROWS_RETRIES = 3;
    public static final String VIEWS_ENABLED = "bigquery.views-enabled";
    public static final String EXPERIMENTAL_ARROW_SERIALIZATION_ENABLED = "bigquery.experimental.arrow-serialization.enabled";

    private Optional<String> projectId = Optional.empty();
    private Optional<String> parentProjectId = Optional.empty();
    private Optional<Integer> parallelism = Optional.empty();
    private boolean viewsEnabled;
    private boolean arrowSerializationEnabled;
    private Duration viewExpireDuration = new Duration(24, HOURS);
    private boolean skipViewMaterialization;
    private Optional<String> viewMaterializationProject = Optional.empty();
    private Optional<String> viewMaterializationDataset = Optional.empty();
    private int maxReadRowsRetries = DEFAULT_MAX_READ_ROWS_RETRIES;
    private boolean caseInsensitiveNameMatching;
    private Duration viewsCacheTtl = new Duration(15, MINUTES);
    private Duration serviceCacheTtl = new Duration(3, MINUTES);
    private Duration metadataCacheTtl = new Duration(0, MILLISECONDS);
    private boolean queryResultsCacheEnabled;
    private String queryLabelName;
    private String queryLabelFormat;
    private boolean proxyEnabled;
    private int metadataParallelism = 2;

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

    public Optional<Integer> getParallelism()
    {
        return parallelism;
    }

    @Config("bigquery.parallelism")
    @ConfigDescription("The number of partitions to split the data into.")
    public BigQueryConfig setParallelism(Integer parallelism)
    {
        this.parallelism = Optional.ofNullable(parallelism);

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

    @Config(EXPERIMENTAL_ARROW_SERIALIZATION_ENABLED)
    @ConfigDescription("Enables experimental Arrow serialization while reading data")
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

    @PostConstruct
    public void validate()
    {
        checkState(viewExpireDuration.toMillis() > viewsCacheTtl.toMillis(), "View expiration duration must be longer than view cache TTL");

        if (skipViewMaterialization) {
            checkState(viewsEnabled, "%s config property must be enabled when skipping view materialization", VIEWS_ENABLED);
        }
    }
}
