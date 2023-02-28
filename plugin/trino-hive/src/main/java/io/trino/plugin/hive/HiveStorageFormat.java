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

import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.type.Category;
import io.trino.plugin.hive.type.MapTypeInfo;
import io.trino.plugin.hive.type.PrimitiveCategory;
import io.trino.plugin.hive.type.PrimitiveTypeInfo;
import io.trino.plugin.hive.type.TypeInfo;
import io.trino.spi.TrinoException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Functions.identity;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.hive.util.HiveClassNames.AVRO_CONTAINER_INPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.AVRO_CONTAINER_OUTPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.AVRO_SERDE_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.COLUMNAR_SERDE_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.HIVE_IGNORE_KEY_OUTPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.HIVE_SEQUENCEFILE_OUTPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.JSON_SERDE_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.LAZY_BINARY_COLUMNAR_SERDE_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.LAZY_SIMPLE_SERDE_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.MAPRED_PARQUET_INPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.MAPRED_PARQUET_OUTPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.OPENCSV_SERDE_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.ORC_INPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.ORC_OUTPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.ORC_SERDE_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.PARQUET_HIVE_SERDE_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.RCFILE_INPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.RCFILE_OUTPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.REGEX_HIVE_SERDE_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.SEQUENCEFILE_INPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveClassNames.TEXT_INPUT_FORMAT_CLASS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public enum HiveStorageFormat
{
    ORC(
            ORC_SERDE_CLASS,
            ORC_INPUT_FORMAT_CLASS,
            ORC_OUTPUT_FORMAT_CLASS,
            DataSize.of(64, Unit.MEGABYTE)),
    PARQUET(
            PARQUET_HIVE_SERDE_CLASS,
            MAPRED_PARQUET_INPUT_FORMAT_CLASS,
            MAPRED_PARQUET_OUTPUT_FORMAT_CLASS,
            DataSize.of(64, Unit.MEGABYTE)),
    AVRO(
            AVRO_SERDE_CLASS,
            AVRO_CONTAINER_INPUT_FORMAT_CLASS,
            AVRO_CONTAINER_OUTPUT_FORMAT_CLASS,
            DataSize.of(64, Unit.MEGABYTE)),
    RCBINARY(
            LAZY_BINARY_COLUMNAR_SERDE_CLASS,
            RCFILE_INPUT_FORMAT_CLASS,
            RCFILE_OUTPUT_FORMAT_CLASS,
            DataSize.of(8, Unit.MEGABYTE)),
    RCTEXT(
            COLUMNAR_SERDE_CLASS,
            RCFILE_INPUT_FORMAT_CLASS,
            RCFILE_OUTPUT_FORMAT_CLASS,
            DataSize.of(8, Unit.MEGABYTE)),
    SEQUENCEFILE(
            LAZY_SIMPLE_SERDE_CLASS,
            SEQUENCEFILE_INPUT_FORMAT_CLASS,
            HIVE_SEQUENCEFILE_OUTPUT_FORMAT_CLASS,
            DataSize.of(8, Unit.MEGABYTE)),
    JSON(
            JSON_SERDE_CLASS,
            TEXT_INPUT_FORMAT_CLASS,
            HIVE_IGNORE_KEY_OUTPUT_FORMAT_CLASS,
            DataSize.of(8, Unit.MEGABYTE)),
    TEXTFILE(
            LAZY_SIMPLE_SERDE_CLASS,
            TEXT_INPUT_FORMAT_CLASS,
            HIVE_IGNORE_KEY_OUTPUT_FORMAT_CLASS,
            DataSize.of(8, Unit.MEGABYTE)),
    CSV(
            OPENCSV_SERDE_CLASS,
            TEXT_INPUT_FORMAT_CLASS,
            HIVE_IGNORE_KEY_OUTPUT_FORMAT_CLASS,
            DataSize.of(8, Unit.MEGABYTE)),
    REGEX(
            REGEX_HIVE_SERDE_CLASS,
            TEXT_INPUT_FORMAT_CLASS,
            HIVE_IGNORE_KEY_OUTPUT_FORMAT_CLASS,
            DataSize.of(8, Unit.MEGABYTE));

    private final String serde;
    private final String inputFormat;
    private final String outputFormat;
    private final DataSize estimatedWriterMemoryUsage;

    HiveStorageFormat(String serde, String inputFormat, String outputFormat, DataSize estimatedWriterMemoryUsage)
    {
        this.serde = requireNonNull(serde, "serde is null");
        this.inputFormat = requireNonNull(inputFormat, "inputFormat is null");
        this.outputFormat = requireNonNull(outputFormat, "outputFormat is null");
        this.estimatedWriterMemoryUsage = requireNonNull(estimatedWriterMemoryUsage, "estimatedWriterMemoryUsage is null");
    }

    public String getSerde()
    {
        return serde;
    }

    public String getInputFormat()
    {
        return inputFormat;
    }

    public String getOutputFormat()
    {
        return outputFormat;
    }

    public DataSize getEstimatedWriterMemoryUsage()
    {
        return estimatedWriterMemoryUsage;
    }

    public void validateColumns(List<HiveColumnHandle> handles)
    {
        if (this == AVRO) {
            for (HiveColumnHandle handle : handles) {
                if (!handle.isPartitionKey()) {
                    validateAvroType(handle.getHiveType().getTypeInfo(), handle.getName());
                }
            }
        }
    }

    private static void validateAvroType(TypeInfo type, String columnName)
    {
        if (type.getCategory() == Category.MAP) {
            TypeInfo keyType = mapTypeInfo(type).getMapKeyTypeInfo();
            if ((keyType.getCategory() != Category.PRIMITIVE) ||
                    (primitiveTypeInfo(keyType).getPrimitiveCategory() != PrimitiveCategory.STRING)) {
                throw new TrinoException(NOT_SUPPORTED, format("Column '%s' has a non-varchar map key, which is not supported by Avro", columnName));
            }
        }
        else if (type.getCategory() == Category.PRIMITIVE) {
            PrimitiveCategory primitive = primitiveTypeInfo(type).getPrimitiveCategory();
            if (primitive == PrimitiveCategory.BYTE) {
                throw new TrinoException(NOT_SUPPORTED, format("Column '%s' is tinyint, which is not supported by Avro. Use integer instead.", columnName));
            }
            if (primitive == PrimitiveCategory.SHORT) {
                throw new TrinoException(NOT_SUPPORTED, format("Column '%s' is smallint, which is not supported by Avro. Use integer instead.", columnName));
            }
        }
    }

    private static final Map<SerdeAndInputFormat, HiveStorageFormat> HIVE_STORAGE_FORMAT_FROM_STORAGE_FORMAT = Arrays.stream(HiveStorageFormat.values())
            .collect(toImmutableMap(format -> new SerdeAndInputFormat(format.getSerde(), format.getInputFormat()), identity()));

    private static final class SerdeAndInputFormat
    {
        private final String serde;
        private final String inputFormat;

        public SerdeAndInputFormat(String serde, String inputFormat)
        {
            this.serde = serde;
            this.inputFormat = inputFormat;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SerdeAndInputFormat that = (SerdeAndInputFormat) o;
            return serde.equals(that.serde) && inputFormat.equals(that.inputFormat);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(serde, inputFormat);
        }
    }

    public static Optional<HiveStorageFormat> getHiveStorageFormat(StorageFormat storageFormat)
    {
        return Optional.ofNullable(HIVE_STORAGE_FORMAT_FROM_STORAGE_FORMAT.get(new SerdeAndInputFormat(storageFormat.getSerde(), storageFormat.getInputFormat())));
    }

    private static PrimitiveTypeInfo primitiveTypeInfo(TypeInfo typeInfo)
    {
        return (PrimitiveTypeInfo) typeInfo;
    }

    private static MapTypeInfo mapTypeInfo(TypeInfo typeInfo)
    {
        return (MapTypeInfo) typeInfo;
    }
}
