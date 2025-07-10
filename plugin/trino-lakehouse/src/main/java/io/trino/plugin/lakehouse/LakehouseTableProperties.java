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
package io.trino.plugin.lakehouse;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.google.inject.Inject;
import io.trino.plugin.deltalake.DeltaLakeTableProperties;
import io.trino.plugin.hive.HiveTableProperties;
import io.trino.plugin.hudi.HudiTableProperties;
import io.trino.plugin.iceberg.IcebergTableProperties;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class LakehouseTableProperties
{
    public static final String TABLE_TYPE_PROPERTY = "type";

    private final List<PropertyMetadata<?>> tableProperties;
    private final PropertyMetadata<?> hiveFormatProperty;
    private final PropertyMetadata<?> icebergFormatProperty;

    @Inject
    public LakehouseTableProperties(
            HiveTableProperties hiveTableProperties,
            IcebergTableProperties icebergTableProperties,
            DeltaLakeTableProperties deltaTableProperties,
            HudiTableProperties hudiTableProperties,
            LakehouseConfig config)
    {
        Map<String, PropertyMetadata<?>> tableProperties = new HashMap<>();

        tableProperties.put("type", enumProperty(
                "type",
                "Table type",
                TableType.class,
                config.getTableType(),
                false));

        tableProperties.put("format", stringProperty(
                "format",
                "File format for the table",
                "",
                false));

        tableProperties.put("sorted_by", new PropertyMetadata<>(
                "sorted_by",
                "Sorted columns",
                new ArrayType(VARCHAR),
                List.class,
                ImmutableList.of(),
                false,
                value -> (List<?>) value,
                value -> value));

        List<PropertyMetadata<?>> allProperties = Streams.concat(
                        hiveTableProperties.getTableProperties().stream(),
                        icebergTableProperties.getTableProperties().stream(),
                        deltaTableProperties.getTableProperties().stream(),
                        hudiTableProperties.getTableProperties().stream())
                .filter(property -> !property.getName().equals("format"))
                .filter(property -> !property.getName().equals("sorted_by"))
                .toList();

        for (PropertyMetadata<?> property : allProperties) {
            PropertyMetadata<?> existing = tableProperties.putIfAbsent(property.getName(), property);
            if (existing == null) {
                continue;
            }
            if (!existing.getDescription().equals(property.getDescription())) {
                throw new VerifyException("Conflicting table property '%s' with different descriptions: %s <> %s"
                        .formatted(property.getName(), existing.getDescription(), property.getDescription()));
            }
            if (!existing.getJavaType().equals(property.getJavaType())) {
                throw new VerifyException("Conflicting table property '%s' with different Java types: %s <> %s"
                        .formatted(property.getName(), existing.getJavaType(), property.getJavaType()));
            }
            if (!existing.getSqlType().equals(property.getSqlType())) {
                throw new VerifyException("Conflicting table property '%s' with different SQL types: %s <> %s"
                        .formatted(property.getName(), existing.getSqlType(), property.getSqlType()));
            }
            if (!Objects.equals(existing.getDefaultValue(), property.getDefaultValue())) {
                throw new VerifyException("Conflicting table property '%s' with different default values: %s <> %s"
                        .formatted(property.getName(), existing.getDefaultValue(), property.getDefaultValue()));
            }
            if (existing.isHidden() != property.isHidden()) {
                throw new VerifyException("Conflicting table property '%s' with different hidden flags: %s <> %s"
                        .formatted(property.getName(), existing.isHidden(), property.isHidden()));
            }
        }

        this.tableProperties = ImmutableList.copyOf(tableProperties.values());

        this.hiveFormatProperty = hiveTableProperties.getTableProperties().stream()
                .filter(property -> property.getName().equals("format"))
                .collect(onlyElement());

        this.icebergFormatProperty = icebergTableProperties.getTableProperties().stream()
                .filter(property -> property.getName().equals("format"))
                .collect(onlyElement());
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public Map<String, Object> unwrapProperties(Map<String, Object> wrappedProperties)
    {
        Map<String, Object> properties = new HashMap<>(wrappedProperties);
        properties.computeIfPresent("format", (_, value) -> switch (getTableType(properties)) {
            case HIVE -> decodeProperty(hiveFormatProperty, value);
            case ICEBERG -> decodeProperty(icebergFormatProperty, value);
            case DELTA, HUDI -> value;
        });
        return properties;
    }

    public Map<String, Object> wrapProperties(TableType tableType, Map<String, Object> unwrappedProperties)
    {
        Map<String, Object> properties = new HashMap<>(unwrappedProperties);
        properties.computeIfPresent("format", (_, value) -> switch (tableType) {
            case HIVE -> encodeProperty(hiveFormatProperty, value);
            case ICEBERG -> encodeProperty(icebergFormatProperty, value);
            case DELTA, HUDI -> value;
        });
        properties.put("type", tableType);
        return properties;
    }

    public static TableType getTableType(Map<String, Object> tableProperties)
    {
        return (TableType) tableProperties.get(TABLE_TYPE_PROPERTY);
    }

    private static <T> Object decodeProperty(PropertyMetadata<T> property, Object value)
    {
        return Optional.ofNullable(emptyToNull((String) value))
                .map(property::decode)
                .orElseGet(property::getDefaultValue);
    }

    private static <T> Object encodeProperty(PropertyMetadata<T> property, Object value)
    {
        return property.encode(property.getJavaType().cast(value));
    }
}
