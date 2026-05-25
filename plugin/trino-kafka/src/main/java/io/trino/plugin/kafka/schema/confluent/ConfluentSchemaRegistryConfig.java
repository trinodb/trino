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
package io.trino.plugin.kafka.schema.confluent;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDuration;
import io.trino.spi.HostAddress;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Size;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.kafka.schema.confluent.EmptyFieldStrategy.IGNORE;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ConfluentSchemaRegistryConfig
{
    private Set<HostAddress> confluentSchemaRegistryUrls = ImmutableSet.of();
    private List<String> confluentSchemaRegistryUrlStrings = ImmutableList.of();
    private int confluentSchemaRegistryClientCacheSize = 1000;
    private EmptyFieldStrategy emptyFieldStrategy = IGNORE;
    private Duration confluentSubjectsCacheRefreshInterval = new Duration(1, SECONDS);

    @Size(min = 1)
    public Set<HostAddress> getConfluentSchemaRegistryUrls()
    {
        return confluentSchemaRegistryUrls;
    }

    @Config("kafka.confluent-schema-registry-url")
    @ConfigDescription("The url of the Confluent Schema Registry")
    public ConfluentSchemaRegistryConfig setConfluentSchemaRegistryUrls(Set<String> confluentSchemaRegistryUrls)
    {
        if (confluentSchemaRegistryUrls == null) {
            this.confluentSchemaRegistryUrls = ImmutableSet.of();
            this.confluentSchemaRegistryUrlStrings = ImmutableList.of();
            return this;
        }
        List<String> trimmed = confluentSchemaRegistryUrls.stream()
                .flatMap(url -> url == null || url.isBlank() ? Stream.empty() : Stream.of(url.trim()))
                .collect(toImmutableList());
        this.confluentSchemaRegistryUrls = trimmed.stream()
                .map(ConfluentSchemaRegistryConfig::toHostAddress)
                .collect(toImmutableSet());
        this.confluentSchemaRegistryUrlStrings = trimmed;
        return this;
    }

    public List<String> getConfluentSchemaRegistryUrlStrings()
    {
        return confluentSchemaRegistryUrlStrings;
    }

    @Min(1)
    @Max(2000)
    public int getConfluentSchemaRegistryClientCacheSize()
    {
        return confluentSchemaRegistryClientCacheSize;
    }

    @Config("kafka.confluent-schema-registry-client-cache-size")
    @ConfigDescription("The maximum number of subjects that can be stored in the Confluent Schema Registry client cache")
    public ConfluentSchemaRegistryConfig setConfluentSchemaRegistryClientCacheSize(int confluentSchemaRegistryClientCacheSize)
    {
        this.confluentSchemaRegistryClientCacheSize = confluentSchemaRegistryClientCacheSize;
        return this;
    }

    public EmptyFieldStrategy getEmptyFieldStrategy()
    {
        return emptyFieldStrategy;
    }

    @Config("kafka.empty-field-strategy")
    @ConfigDescription("How to handle struct types with no fields: ignore, add a marker field named '$empty_field_marker' or fail the query")
    public ConfluentSchemaRegistryConfig setEmptyFieldStrategy(EmptyFieldStrategy emptyFieldStrategy)
    {
        this.emptyFieldStrategy = emptyFieldStrategy;
        return this;
    }

    @MinDuration("1ms")
    @MaxDuration("30s")
    public Duration getConfluentSubjectsCacheRefreshInterval()
    {
        return confluentSubjectsCacheRefreshInterval;
    }

    @Config("kafka.confluent-subjects-cache-refresh-interval")
    @ConfigDescription("The interval that the topic to subjects cache will be refreshed")
    public ConfluentSchemaRegistryConfig setConfluentSubjectsCacheRefreshInterval(Duration confluentSubjectsCacheRefreshInterval)
    {
        this.confluentSubjectsCacheRefreshInterval = confluentSubjectsCacheRefreshInterval;
        return this;
    }

    private static HostAddress toHostAddress(String value)
    {
        return HostAddress.fromString(value);
    }
}
