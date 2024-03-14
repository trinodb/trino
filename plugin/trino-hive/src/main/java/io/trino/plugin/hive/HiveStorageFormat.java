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

import com.google.common.collect.ImmutableMap;
import io.trino.hive.formats.compression.CompressionKind;
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
import java.util.Optional;

import static com.google.common.base.Functions.identity;
import static io.trino.hive.formats.HiveClassNames.AVRO_CONTAINER_INPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.AVRO_CONTAINER_OUTPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.AVRO_SERDE_CLASS;
import static io.trino.hive.formats.HiveClassNames.COLUMNAR_SERDE_CLASS;
import static io.trino.hive.formats.HiveClassNames.HIVE_IGNORE_KEY_OUTPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.HIVE_SEQUENCEFILE_OUTPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.JSON_SERDE_CLASS;
import static io.trino.hive.formats.HiveClassNames.LAZY_BINARY_COLUMNAR_SERDE_CLASS;
import static io.trino.hive.formats.HiveClassNames.LAZY_SIMPLE_SERDE_CLASS;
import static io.trino.hive.formats.HiveClassNames.MAPRED_PARQUET_INPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.MAPRED_PARQUET_OUTPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.OPENCSV_SERDE_CLASS;
import static io.trino.hive.formats.HiveClassNames.OPENX_JSON_SERDE_CLASS;
import static io.trino.hive.formats.HiveClassNames.ORC_INPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.ORC_OUTPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.ORC_SERDE_CLASS;
import static io.trino.hive.formats.HiveClassNames.PARQUET_HIVE_SERDE_CLASS;
import static io.trino.hive.formats.HiveClassNames.RCFILE_INPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.RCFILE_OUTPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.REGEX_SERDE_CLASS;
import static io.trino.hive.formats.HiveClassNames.SEQUENCEFILE_INPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.TEXT_INPUT_FORMAT_CLASS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public enum HiveStorageFormat
{
    ORC(
            ORC_SERDE_CLASS,
            ORC_INPUT_FORMAT_CLASS,
            ORC_OUTPUT_FORMAT_CLASS),
    PARQUET(
            PARQUET_HIVE_SERDE_CLASS,
            MAPRED_PARQUET_INPUT_FORMAT_CLASS,
            MAPRED_PARQUET_OUTPUT_FORMAT_CLASS),
    AVRO(
            AVRO_SERDE_CLASS,
            AVRO_CONTAINER_INPUT_FORMAT_CLASS,
            AVRO_CONTAINER_OUTPUT_FORMAT_CLASS),
    RCBINARY(
            LAZY_BINARY_COLUMNAR_SERDE_CLASS,
            RCFILE_INPUT_FORMAT_CLASS,
            RCFILE_OUTPUT_FORMAT_CLASS),
    RCTEXT(
            COLUMNAR_SERDE_CLASS,
            RCFILE_INPUT_FORMAT_CLASS,
            RCFILE_OUTPUT_FORMAT_CLASS),
    SEQUENCEFILE(
            LAZY_SIMPLE_SERDE_CLASS,
            SEQUENCEFILE_INPUT_FORMAT_CLASS,
            HIVE_SEQUENCEFILE_OUTPUT_FORMAT_CLASS),
    JSON(
            JSON_SERDE_CLASS,
            TEXT_INPUT_FORMAT_CLASS,
            HIVE_IGNORE_KEY_OUTPUT_FORMAT_CLASS),
    OPENX_JSON(
            OPENX_JSON_SERDE_CLASS,
            TEXT_INPUT_FORMAT_CLASS,
            HIVE_IGNORE_KEY_OUTPUT_FORMAT_CLASS),
    TEXTFILE(
            LAZY_SIMPLE_SERDE_CLASS,
            TEXT_INPUT_FORMAT_CLASS,
            HIVE_IGNORE_KEY_OUTPUT_FORMAT_CLASS),
    CSV(
            OPENCSV_SERDE_CLASS,
            TEXT_INPUT_FORMAT_CLASS,
            HIVE_IGNORE_KEY_OUTPUT_FORMAT_CLASS),
    REGEX(
            REGEX_SERDE_CLASS,
            TEXT_INPUT_FORMAT_CLASS,
            HIVE_IGNORE_KEY_OUTPUT_FORMAT_CLASS);

    private final String serde;
    private final String inputFormat;
    private final String outputFormat;

    HiveStorageFormat(String serde, String inputFormat, String outputFormat)
    {
        this.serde = requireNonNull(serde, "serde is null");
        this.inputFormat = requireNonNull(inputFormat, "inputFormat is null");
        this.outputFormat = requireNonNull(outputFormat, "outputFormat is null");
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

    public boolean isSplittable(String path)
    {
        // Only uncompressed text input format is splittable
        return switch (this) {
            case ORC, PARQUET, AVRO, RCBINARY, RCTEXT, SEQUENCEFILE -> true;
            case JSON, OPENX_JSON, TEXTFILE, CSV, REGEX -> CompressionKind.forFile(path).isEmpty();
        };
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

    private static PrimitiveTypeInfo primitiveTypeInfo(TypeInfo typeInfo)
    {
        return (PrimitiveTypeInfo) typeInfo;
    }

    private static MapTypeInfo mapTypeInfo(TypeInfo typeInfo)
    {
        return (MapTypeInfo) typeInfo;
    }

    @SuppressWarnings("unused")
    private record SerdeAndInputFormat(String serde, String inputFormat) {}

    private static final Map<SerdeAndInputFormat, HiveStorageFormat> HIVE_STORAGE_FORMATS = ImmutableMap.<SerdeAndInputFormat, HiveStorageFormat>builder()
            .putAll(Arrays.stream(values()).collect(
                    toMap(format -> new SerdeAndInputFormat(format.getSerde(), format.getInputFormat()), identity())))
            .put(new SerdeAndInputFormat(PARQUET_HIVE_SERDE_CLASS, "parquet.hive.DeprecatedParquetInputFormat"), PARQUET)
            .put(new SerdeAndInputFormat(PARQUET_HIVE_SERDE_CLASS, "org.apache.hadoop.mapred.TextInputFormat"), PARQUET)
            .put(new SerdeAndInputFormat(PARQUET_HIVE_SERDE_CLASS, "parquet.hive.MapredParquetInputFormat"), PARQUET)
            .buildOrThrow();

    public static Optional<HiveStorageFormat> getHiveStorageFormat(StorageFormat storageFormat)
    {
        return Optional.ofNullable(HIVE_STORAGE_FORMATS.get(
                new SerdeAndInputFormat(storageFormat.getSerde(), storageFormat.getInputFormat())));
    }

    public String humanName()
    {
        return switch (this) {
            case AVRO -> "Avro";
            case PARQUET -> "Parquet";
            default -> toString();
        };
    }
}
