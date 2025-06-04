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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metastore.Database;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.session.PropertyMetadata.stringProperty;

public final class HiveSchemaProperties
{
    public static final String LOCATION_PROPERTY = "location";

    public static final List<PropertyMetadata<?>> SCHEMA_PROPERTIES = ImmutableList.of(
            stringProperty(
                    LOCATION_PROPERTY,
                    "Base file system location URI",
                    null,
                    false));

    private HiveSchemaProperties() {}

    public static Map<String, Object> fromDatabase(Database db)
    {
        ImmutableMap.Builder<String, Object> result = ImmutableMap.builder();
        db.getLocation().ifPresent(location -> result.put(HiveSchemaProperties.LOCATION_PROPERTY, location));

        return result.buildOrThrow();
    }

    public static Optional<String> getLocation(Map<String, Object> schemaProperties)
    {
        return Optional.ofNullable((String) schemaProperties.get(LOCATION_PROPERTY));
    }
}
