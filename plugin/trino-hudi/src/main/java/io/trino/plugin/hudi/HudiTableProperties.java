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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_UNKNOWN_TABLE_TYPE;
import static io.trino.plugin.hudi.HudiTableType.COPY_ON_WRITE;
import static io.trino.plugin.hudi.HudiTableType.MERGE_ON_READ;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class HudiTableProperties
{
    public static final String LOCATION_PROPERTY = "location";
    public static final String HUDI_TABLE_TYPE = "type";
    public static final String RECORD_KEY_FIELDS = "record_key_fields";
    public static final String PRE_COMBINE_FIELD = "pre_combine_field";
    public static final String PARTITIONED_BY_PROPERTY = "partitioned_by";
    public static final String STORAGE_FORMAT_PROPERTY = "format";
    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public HudiTableProperties(HudiConfig config)
    {
        tableProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(stringProperty(
                        LOCATION_PROPERTY,
                        "File system location URI for the table",
                        null,
                        false))
                .add(stringProperty(
                        HUDI_TABLE_TYPE,
                        "Hoodie table type",
                        "cow",
                        false))
                .add(stringProperty(
                        PRE_COMBINE_FIELD,
                        "Hoodie pre combine field",
                        null,
                        false))
                .add(enumProperty(
                        STORAGE_FORMAT_PROPERTY,
                        "Hoodie storage format for the table",
                        HudiStorageFormat.class,
                        config.getHudiStorageFormat(),
                        false))
                .add(new PropertyMetadata<>(
                        RECORD_KEY_FIELDS,
                        "Hoodie record key field",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ((Collection<String>) value).stream()
                                .map(name -> name.toLowerCase(ENGLISH))
                                .collect(toImmutableList()),
                        value -> value))
                .add(new PropertyMetadata<>(
                        PARTITIONED_BY_PROPERTY,
                        "Hoodie partition columns",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ((Collection<String>) value).stream()
                                .map(name -> ((String) name).toLowerCase(ENGLISH))
                                .collect(toImmutableList()),
                        value -> value))
                .build();
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static Optional<String> getTableLocation(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((String) tableProperties.get(LOCATION_PROPERTY));
    }

    public static Optional<List<String>> getRecordKeyFields(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((List<String>) tableProperties.get(RECORD_KEY_FIELDS));
    }

    public static Optional<String> getPreCombineField(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((String) tableProperties.get(PRE_COMBINE_FIELD));
    }

    public static Optional<List<String>> getPartitionedBy(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((List<String>) tableProperties.get(PARTITIONED_BY_PROPERTY));
    }

    public static HudiTableType getHudiTableType(Map<String, Object> tableProperties)
    {
        String tableType = (String) tableProperties.get(HUDI_TABLE_TYPE);
        switch (tableType) {
            case "mor":
                return MERGE_ON_READ;
            case "cow":
                return COPY_ON_WRITE;
            default:
                throw new TrinoException(HUDI_UNKNOWN_TABLE_TYPE, format("Not a Hudi table type: %s", tableType));
        }
    }

    public static HudiStorageFormat getHudiStorageFormat(Map<String, Object> tableProperties)
    {
        return (HudiStorageFormat) tableProperties.get(STORAGE_FORMAT_PROPERTY);
    }
}
