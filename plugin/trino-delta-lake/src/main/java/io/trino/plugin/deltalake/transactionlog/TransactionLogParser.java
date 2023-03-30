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
package io.trino.plugin.deltalake.transactionlog;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.plugin.base.util.JsonUtils;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.transactionlog.checkpoint.LastCheckpoint;
import io.trino.spi.TrinoException;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.filesystem.Locations.appendPath;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogJsonEntryPath;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Double.parseDouble;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;

public final class TransactionLogParser
{
    private static final Logger log = Logger.get(TransactionLogParser.class);

    // Before 1900, Java Time and Joda Time are not consistent with java.sql.Date and java.util.Calendar
    // Since January 1, 1900 UTC is still December 31, 1899 in other zones, we are adding a 1 day margin.
    public static final LocalDate START_OF_MODERN_ERA = LocalDate.of(1900, 1, 2);

    public static final String LAST_CHECKPOINT_FILENAME = "_last_checkpoint";

    private TransactionLogParser() {}

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    // partition timestamp values are represented as yyyy-MM-dd HH:mm:ss.SSSSSSSSS, where the fractional seconds part can have 0-9 digits
    public static final DateTimeFormatter PARTITION_TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendValue(YEAR, 4, 10, SignStyle.NORMAL)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 2)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 2)
            .appendLiteral(' ')
            .appendValue(ChronoField.HOUR_OF_DAY, 2).appendLiteral(':')
            .appendValue(ChronoField.MINUTE_OF_HOUR, 2).appendLiteral(':')
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .toFormatter(Locale.ENGLISH)
            .withChronology(IsoChronology.INSTANCE)
            .withResolverStyle(ResolverStyle.STRICT);
    public static final DateTimeFormatter JSON_STATISTICS_TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendValue(YEAR, 4, 10, SignStyle.NORMAL)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 2)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 2)
            .appendLiteral('T')
            .append(ISO_LOCAL_TIME)
            .appendOffsetId()
            .optionalStart()
            .appendLiteral('[')
            .parseCaseSensitive()
            .appendZoneRegionId()
            .appendLiteral(']')
            .toFormatter(Locale.ENGLISH)
            .withChronology(IsoChronology.INSTANCE)
            .withResolverStyle(ResolverStyle.STRICT);

    public static DeltaLakeTransactionLogEntry parseJson(String json)
            throws JsonProcessingException
    {
        // lines are json strings followed by 'x' in some Databricks versions of Delta
        if (json.endsWith("x")) {
            json = json.substring(0, json.length() - 1);
        }
        return JsonUtils.parseJson(OBJECT_MAPPER, json, DeltaLakeTransactionLogEntry.class);
    }

    private static Object parseDecimal(DecimalType type, String valueString)
    {
        BigDecimal bigDecimal = new BigDecimal(valueString).setScale(type.getScale());
        if (type.isShort()) {
            return bigDecimal.unscaledValue().longValueExact();
        }
        return Decimals.valueOf(bigDecimal.unscaledValue());
    }

    @Nullable
    public static Object deserializePartitionValue(DeltaLakeColumnHandle column, Optional<String> valueString)
    {
        return valueString.map(value -> deserializeColumnValue(column, value, TransactionLogParser::readPartitionTimestamp)).orElse(null);
    }

    private static Long readPartitionTimestamp(String timestamp)
    {
        ZonedDateTime zonedDateTime = LocalDateTime.parse(timestamp, PARTITION_TIMESTAMP_FORMATTER).atZone(UTC);
        return packDateTimeWithZone(zonedDateTime.toInstant().toEpochMilli(), UTC_KEY);
    }

    public static Object deserializeColumnValue(DeltaLakeColumnHandle column, String valueString, Function<String, Long> timestampReader)
    {
        Type type = column.getType();
        try {
            if (type.equals(BOOLEAN)) {
                if (valueString.equalsIgnoreCase("true")) {
                    return true;
                }
                if (valueString.equalsIgnoreCase("false")) {
                    return false;
                }
            }
            if (type.equals(INTEGER)) {
                return (long) parseInt(valueString);
            }
            if (type.equals(SMALLINT)) {
                return (long) parseInt(valueString);
            }
            if (type.equals(TINYINT)) {
                return (long) parseInt(valueString);
            }
            if (type.equals(BIGINT)) {
                return parseLong(valueString);
            }
            if (type.getBaseName().equals(StandardTypes.DECIMAL)) {
                return parseDecimal((DecimalType) type, valueString);
            }
            if (type.equals(REAL)) {
                return (long) floatToRawIntBits(parseFloat(valueString));
            }
            if (type.equals(DOUBLE)) {
                return parseDouble(valueString);
            }
            if (type.equals(DATE)) {
                // date values are represented as yyyy-MM-dd
                return LocalDate.parse(valueString).toEpochDay();
            }
            if (type.equals(createTimestampWithTimeZoneType(3))) {
                return timestampReader.apply(valueString);
            }
            if (VARCHAR.equals(type)) {
                return utf8Slice(valueString);
            }
        }
        catch (RuntimeException e) {
            throw new TrinoException(
                    GENERIC_INTERNAL_ERROR,
                    format("Unable to parse value [%s] from column %s with type %s", valueString, column.getName(), column.getType()),
                    e);
        }
        // Anything else is not a supported DeltaLake column
        throw new TrinoException(
                GENERIC_INTERNAL_ERROR,
                format("Unable to parse value [%s] from column %s with type %s", valueString, column.getName(), column.getType()));
    }

    static Optional<LastCheckpoint> readLastCheckpoint(TrinoFileSystem fileSystem, String tableLocation)
    {
        return Failsafe.with(RetryPolicy.builder()
                        .withMaxRetries(5)
                        .withDelay(Duration.ofSeconds(1))
                        .onRetry(event -> {
                            // The _last_checkpoint file is malformed, it's probably in the middle of a rewrite (file rewrites on Azure are NOT atomic)
                            // Retry several times with a short delay, and if that fails, fall back to manually finding latest checkpoint.
                            log.debug(event.getLastException(), "Failure when accessing last checkpoint information, will be retried");
                        })
                        .build())
                .get(() -> tryReadLastCheckpoint(fileSystem, tableLocation));
    }

    private static Optional<LastCheckpoint> tryReadLastCheckpoint(TrinoFileSystem fileSystem, String tableLocation)
            throws JsonParseException, JsonMappingException
    {
        String checkpointPath = appendPath(getTransactionLogDir(tableLocation), LAST_CHECKPOINT_FILENAME);
        TrinoInputFile inputFile = fileSystem.newInputFile(checkpointPath);
        try (InputStream lastCheckpointInput = inputFile.newStream()) {
            // Note: there apparently is 8K buffering applied and _last_checkpoint should be much smaller.
            return Optional.of(JsonUtils.parseJson(OBJECT_MAPPER, lastCheckpointInput, LastCheckpoint.class));
        }
        catch (JsonParseException | JsonMappingException e) {
            // The _last_checkpoint file is malformed, it's probably in the middle of a rewrite (file rewrites on Azure are NOT atomic)
            throw e;
        }
        catch (IOException | UncheckedIOException e) {
            // _last_checkpoint file was not found, we need to find latest checkpoint manually
            // ideally, we'd detect the condition by catching FileNotFoundException, but some file system implementations
            // will throw different exceptions if the checkpoint is not found
            return Optional.empty();
        }
    }

    public static long getMandatoryCurrentVersion(TrinoFileSystem fileSystem, String tableLocation)
            throws IOException
    {
        long version = readLastCheckpoint(fileSystem, tableLocation).map(LastCheckpoint::getVersion).orElse(0L);

        String transactionLogDir = getTransactionLogDir(tableLocation);
        while (true) {
            String entryPath = getTransactionLogJsonEntryPath(transactionLogDir, version + 1);
            if (!fileSystem.newInputFile(entryPath).exists()) {
                return version;
            }
            version++;
        }
    }
}
