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
package io.trino.plugin.kafka.schema.rest;

import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.Set;

import static java.util.concurrent.TimeUnit.SECONDS;

public class RestTableDescriptionSupplierConfig
{
    private URI tableDescriptionApiUrl;
    private Duration refreshInterval = new Duration(30, SECONDS);
    private Set<String> confluentSchemaRegistryUrls = ImmutableSet.of();

    @NotNull(message = "kafka.table-description-api-url is required when using rest table description supplier")
    public URI getTableDescriptionApiUrl()
    {
        return tableDescriptionApiUrl;
    }

    @Config("kafka.table-description-api-url")
    @ConfigDescription("Base URL of the REST API that returns Kafka table and schema descriptions (JSON array of table description objects)")
    public RestTableDescriptionSupplierConfig setTableDescriptionApiUrl(String tableDescriptionApiUrl)
    {
        this.tableDescriptionApiUrl = tableDescriptionApiUrl == null || tableDescriptionApiUrl.isBlank()
                ? null
                : URI.create(tableDescriptionApiUrl.trim());
        return this;
    }

    @MinDuration("1s")
    @MaxDuration("1h")
    public Duration getRefreshInterval()
    {
        return refreshInterval;
    }

    @Config("kafka.table-description-api-refresh-interval")
    @ConfigDescription("How often to refresh table list from the REST API")
    public RestTableDescriptionSupplierConfig setRefreshInterval(Duration refreshInterval)
    {
        this.refreshInterval = refreshInterval;
        return this;
    }

    public Set<String> getConfluentSchemaRegistryUrls()
    {
        return confluentSchemaRegistryUrls;
    }

    @Config("kafka.confluent-schema-registry-url")
    @ConfigDescription("Optional Confluent Schema Registry URL(s) for resolving Avro schemas by subject. When set, table descriptions may use \"subject\" instead of \"dataSchema\" for key/message")
    public RestTableDescriptionSupplierConfig setConfluentSchemaRegistryUrls(Set<String> confluentSchemaRegistryUrls)
    {
        this.confluentSchemaRegistryUrls = confluentSchemaRegistryUrls == null ? ImmutableSet.of() : ImmutableSet.copyOf(confluentSchemaRegistryUrls);
        return this;
    }
}
