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
package io.trino.plugin.paimon.format;

import io.trino.plugin.paimon.PaimonErrorCode;
import io.trino.plugin.paimon.PaimonTypeUtils;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.format.orc.OrcFileFormatFactory;
import org.apache.paimon.format.parquet.ParquetFileFormatFactory;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.paimon.format.TrinoPaimonFormatWriterOptions.empty;
import static java.util.Objects.requireNonNull;

public class TrinoPaimonFileFormat
        extends FileFormat
{
    static final String ORC = "orc";
    static final String PARQUET = "parquet";
    private static final String ORC_STRIPE_SIZE = "orc.stripe.size";
    private static final String PARQUET_BLOCK_SIZE = "parquet.block.size";
    private static final String PARQUET_PAGE_SIZE = "parquet.page.size";
    private static final String PARQUET_PAGE_ROW_COUNT_LIMIT = "parquet.page.row.count.limit";

    private final FormatContext context;

    TrinoPaimonFileFormat(String formatIdentifier, FormatContext context)
    {
        super(formatIdentifier);
        this.context = requireNonNull(context, "context is null");
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> filters)
    {
        if (isRowTrackingRead(dataSchemaRowType, projectedRowType)) {
            return nativeFileFormat().createReaderFactory(dataSchemaRowType, projectedRowType, filters);
        }
        validateSupportedReadType(projectedRowType);
        return new TrinoPaimonFormatReaderFactory(formatIdentifier, projectedRowType);
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType type)
    {
        validateWriteType(formatIdentifier, type);
        return new TrinoPaimonFormatWriterFactory(
                formatIdentifier,
                type,
                context.writeBatchSize(),
                writerOptions());
    }

    @Override
    public void validateDataFields(RowType rowType)
    {
        if (PARQUET.equals(formatIdentifier) || ORC.equals(formatIdentifier)) {
            return;
        }
        throw new TrinoException(PaimonErrorCode.PAIMON_BAD_DATA,
                "Unsupported Trino Paimon file format: " + formatIdentifier
                        + ". Only Parquet and ORC are supported with the Trino no-Hadoop format provider.");
    }

    @Override
    public Optional<SimpleStatsExtractor> createStatsExtractor(
            RowType type,
            SimpleColStatsCollector.Factory[] statsCollectors)
    {
        return Optional.of(new TrinoPaimonSimpleStatsExtractor(type, statsCollectors));
    }

    static List<Type> trinoTypes(RowType rowType)
    {
        return rowType.getFields().stream()
                .map(field -> PaimonTypeUtils.fromPaimonType(field.type()))
                .toList();
    }

    private TrinoPaimonFormatWriterOptions writerOptions()
    {
        if (PARQUET.equals(formatIdentifier)) {
            return new TrinoPaimonFormatWriterOptions(
                    blockSizeBytes(PARQUET_BLOCK_SIZE),
                    positiveIntegerOption(PARQUET_PAGE_SIZE),
                    positiveIntegerOption(PARQUET_PAGE_ROW_COUNT_LIMIT));
        }
        if (ORC.equals(formatIdentifier)) {
            return new TrinoPaimonFormatWriterOptions(
                    blockSizeBytes(ORC_STRIPE_SIZE),
                    Optional.empty(),
                    Optional.empty());
        }
        return empty();
    }

    private Optional<Long> blockSizeBytes(String formatSpecificKey)
    {
        Optional<Long> blockSizeBytes = Optional.ofNullable(context.blockSize())
                .map(blockSize -> blockSize.getBytes());
        if (blockSizeBytes.isEmpty()) {
            blockSizeBytes = positiveLongOption(formatSpecificKey);
        }
        blockSizeBytes.ifPresent(size -> checkArgument(size > 0, "file.block-size must be greater than 0 bytes"));
        return blockSizeBytes;
    }

    private Optional<Long> positiveLongOption(String key)
    {
        if (!context.options().containsKey(key)) {
            return Optional.empty();
        }
        long value = context.options().getLong(key, -1);
        checkArgument(value > 0, "%s must be greater than 0", key);
        return Optional.of(value);
    }

    private Optional<Integer> positiveIntegerOption(String key)
    {
        if (!context.options().containsKey(key)) {
            return Optional.empty();
        }
        int value = context.options().getInteger(key, -1);
        checkArgument(value > 0, "%s must be greater than 0", key);
        return Optional.of(value);
    }

    private FileFormat nativeFileFormat()
    {
        return switch (formatIdentifier) {
            case PARQUET -> new ParquetFileFormatFactory().create(context);
            case ORC -> new OrcFileFormatFactory().create(context);
            default -> throw new UnsupportedOperationException("Unsupported Trino Paimon file format: " + formatIdentifier);
        };
    }

    private static boolean isRowTrackingRead(RowType dataSchemaRowType, RowType projectedRowType)
    {
        requireNonNull(dataSchemaRowType, "dataSchemaRowType is null");
        requireNonNull(projectedRowType, "projectedRowType is null");
        return containsField(dataSchemaRowType, SpecialFields.ROW_ID.name())
                && projectedRowType.getFieldNames().stream()
                .anyMatch(field -> SpecialFields.ROW_ID.name().equalsIgnoreCase(field)
                        || SpecialFields.SEQUENCE_NUMBER.name().equalsIgnoreCase(field));
    }

    private static boolean containsField(RowType rowType, String fieldName)
    {
        requireNonNull(rowType, "rowType is null");
        requireNonNull(fieldName, "fieldName is null");
        return rowType.getFieldNames().stream()
                .anyMatch(field -> fieldName.equalsIgnoreCase(field));
    }

    public static void validateWriteType(String formatIdentifier, RowType type)
    {
        requireNonNull(formatIdentifier, "formatIdentifier is null");
        requireNonNull(type, "type is null");
        validateSupportedWriteType(type);
        if (ORC.equals(formatIdentifier) && containsTimeType(type)) {
            throw new UnsupportedOperationException(
                    "Trino Paimon ORC writer does not support Paimon TIME columns; use Parquet or Paimon's native writer for ORC TIME data");
        }
    }

    private static void validateSupportedWriteType(RowType rowType)
    {
        if (rowType.getFields().stream()
                .map(field -> field.type())
                .anyMatch(PaimonTypeUtils::containsUnsupportedTrinoFormatProviderWriteType)) {
            throw new UnsupportedOperationException(
                    "Trino Paimon file format does not support Paimon BLOB, VARIANT, VECTOR, or MULTISET writes");
        }
    }

    private static boolean containsTimeType(RowType rowType)
    {
        return rowType.getFields().stream()
                .map(field -> field.type())
                .anyMatch(TrinoPaimonFileFormat::containsTimeType);
    }

    private static boolean containsTimeType(DataType type)
    {
        requireNonNull(type, "type is null");
        if (type.getTypeRoot() == DataTypeRoot.TIME_WITHOUT_TIME_ZONE) {
            return true;
        }
        return switch (type.getTypeRoot()) {
            case ARRAY, MAP, MULTISET, ROW, VECTOR -> DataTypeChecks.getNestedTypes(type).stream()
                    .anyMatch(TrinoPaimonFileFormat::containsTimeType);
            default -> false;
        };
    }

    private static void validateSupportedReadType(RowType rowType)
    {
        if (rowType.getFields().stream()
                .map(field -> field.type())
                .anyMatch(PaimonTypeUtils::containsUnsupportedTrinoFormatProviderReadType)) {
            throw new UnsupportedOperationException(
                    "Trino Paimon file format does not support Paimon BLOB, VARIANT, VECTOR, or MULTISET reads");
        }
    }
}
