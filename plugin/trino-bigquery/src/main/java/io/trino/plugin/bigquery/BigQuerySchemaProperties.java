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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static java.util.Objects.requireNonNull;

public final class BigQuerySchemaProperties
{
    public static final String LOCATION_PROPERTY = "location";

    private final List<PropertyMetadata<?>> schemaProperties;

    @Inject
    public BigQuerySchemaProperties()
    {
        schemaProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(stringProperty(
                        LOCATION_PROPERTY,
                        "BigQuery dataset location, e.g. US, EU, asia-northeast1. Defaults to the BigQuery API's default (US) when unset.",
                        null,
                        false))
                .build();
    }

    public List<PropertyMetadata<?>> getSchemaProperties()
    {
        return schemaProperties;
    }

    public static Optional<String> location(Map<String, Object> schemaProperties)
    {
        requireNonNull(schemaProperties, "schemaProperties is null");
        return Optional.ofNullable((String) schemaProperties.get(LOCATION_PROPERTY));
    }
}
