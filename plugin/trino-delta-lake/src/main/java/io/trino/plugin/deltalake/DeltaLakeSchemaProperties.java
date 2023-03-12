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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.HiveTypeName;
import io.trino.plugin.hive.metastore.Database;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMNS;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMN_TYPES;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static java.util.stream.Collectors.joining;

public class DeltaLakeSchemaProperties
{
    public static final String LOCATION_PROPERTY = "location";

    public static final List<PropertyMetadata<?>> SCHEMA_PROPERTIES = ImmutableList.of(
            stringProperty(
                    LOCATION_PROPERTY,
                    "URI for the default location to store tables created in this schema",
                    null,
                    false));

    private DeltaLakeSchemaProperties() {}

    public static Map<String, Object> fromDatabase(Database db)
    {
        ImmutableMap.Builder<String, Object> result = ImmutableMap.builder();
        db.getLocation().ifPresent(location -> result.put(LOCATION_PROPERTY, location));

        return result.buildOrThrow();
    }

    public static Optional<String> getLocation(Map<String, Object> schemaProperties)
    {
        return Optional.ofNullable((String) schemaProperties.get(LOCATION_PROPERTY));
    }

    public static Properties buildHiveSchema(List<String> columnNames, List<Type> columnTypes)
    {
        Properties schema = new Properties();
        schema.setProperty(LIST_COLUMNS, String.join(",", columnNames));
        schema.setProperty(LIST_COLUMN_TYPES, columnTypes.stream()
                .map(DeltaHiveTypeTranslator::toHiveType)
                .map(HiveType::getHiveTypeName)
                .map(HiveTypeName::toString)
                .collect(joining(":")));
        return schema;
    }
}
