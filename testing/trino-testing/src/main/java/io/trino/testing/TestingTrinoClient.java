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
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.client.IntervalDayTime;
import io.trino.client.IntervalYearMonth;
import io.trino.client.QueryData;
import io.trino.client.QueryStatusInfo;
import io.trino.client.Row;
import io.trino.client.RowField;
import io.trino.client.Warning;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.type.SqlIntervalDayTime;
import io.trino.type.SqlIntervalYearMonth;
import okhttp3.OkHttpClient;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.testing.MaterializedResult.DEFAULT_PRECISION;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.trino.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static io.trino.type.IpAddressType.IPADDRESS;
import static io.trino.type.JsonType.JSON;
import static io.trino.util.MoreLists.mappedCopy;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.util.stream.Collectors.toList;

public class TestingTrinoClient
        extends AbstractTestingTrinoClient<MaterializedResult>
{
    private static final DateTimeFormatter timeWithZoneOffsetFormat = new DateTimeFormatterBuilder()
            .appendPattern("HH:mm:ss")
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .appendPattern("XXX")
            .toFormatter();

    private static final DateTimeFormatter timestampFormat = new DateTimeFormatterBuilder()
            .appendPattern("uuuu-MM-dd HH:mm:ss")
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .toFormatter();

    private static final DateTimeFormatter timestampWithTimeZoneFormat = new DateTimeFormatterBuilder()
            .append(timestampFormat)
            .appendPattern(" VV")
            .toFormatter();

    public TestingTrinoClient(TestingTrinoServer trinoServer, Session defaultSession)
    {
        super(trinoServer, defaultSession);
    }

    public TestingTrinoClient(TestingTrinoServer trinoServer, Session defaultSession, OkHttpClient httpClient)
    {
        super(trinoServer, defaultSession, httpClient);
    }

    @Override
    protected ResultsSession<MaterializedResult> getResultSession(Session session)
    {
        return new MaterializedResultSession();
    }

    private class MaterializedResultSession
            implements ResultsSession<MaterializedResult>
    {
        private final ImmutableList.Builder<MaterializedRow> rows = ImmutableList.builder();

        private final AtomicReference<List<Type>> types = new AtomicReference<>();

        private final AtomicReference<Optional<String>> updateType = new AtomicReference<>(Optional.empty());
        private final AtomicReference<OptionalLong> updateCount = new AtomicReference<>(OptionalLong.empty());
        private final AtomicReference<List<Warning>> warnings = new AtomicReference<>(ImmutableList.of());

        @Override
        public void setUpdateType(String type)
        {
            updateType.set(Optional.of(type));
        }

        @Override
        public void setUpdateCount(long count)
        {
            updateCount.set(OptionalLong.of(count));
        }

        @Override
        public void setWarnings(List<Warning> warnings)
        {
            this.warnings.set(warnings);
        }

        @Override
        public void addResults(QueryStatusInfo statusInfo, QueryData data)
        {
            if (types.get() == null && statusInfo.getColumns() != null) {
                types.set(getTypes(statusInfo.getColumns()));
            }

            if (data.getData() != null) {
                checkState(types.get() != null, "data received without types");
                rows.addAll(mappedCopy(data.getData(), dataToRow(types.get())));
            }
        }

        @Override
        public MaterializedResult build(Map<String, String> setSessionProperties, Set<String> resetSessionProperties)
        {
            checkState(types.get() != null, "never received types for the query");
            return new MaterializedResult(
                    rows.build(),
                    types.get(),
                    setSessionProperties,
                    resetSessionProperties,
                    updateType.get(),
                    updateCount.get(),
                    warnings.get());
        }
    }

    private static Function<List<Object>, MaterializedRow> dataToRow(List<Type> types)
    {
        return data -> {
            checkArgument(data.size() == types.size(), "columns size does not match types size");
            List<Object> row = new ArrayList<>();
            for (int i = 0; i < data.size(); i++) {
                Object value = data.get(i);
                Type type = types.get(i);
                row.add(convertToRowValue(type, value));
            }
            return new MaterializedRow(DEFAULT_PRECISION, row);
        };
    }

    private static Object convertToRowValue(Type type, Object value)
    {
        if (value == null) {
            return null;
        }

        if (BOOLEAN.equals(type)) {
            return value;
        }
        if (TINYINT.equals(type)) {
            return ((Number) value).byteValue();
        }
        if (SMALLINT.equals(type)) {
            return ((Number) value).shortValue();
        }
        if (INTEGER.equals(type)) {
            return ((Number) value).intValue();
        }
        if (BIGINT.equals(type)) {
            return ((Number) value).longValue();
        }
        if (DOUBLE.equals(type)) {
            return ((Number) value).doubleValue();
        }
        if (REAL.equals(type)) {
            return ((Number) value).floatValue();
        }
        if (UUID.equals(type)) {
            return java.util.UUID.fromString((String) value);
        }
        if (IPADDRESS.equals(type)) {
            return value;
        }
        if (type instanceof VarcharType) {
            return value;
        }
        if (type instanceof CharType) {
            return value;
        }
        if (VARBINARY.equals(type)) {
            return value;
        }
        if (DATE.equals(type)) {
            return DateTimeFormatter.ISO_LOCAL_DATE.parse(((String) value), LocalDate::from);
        }
        if (type instanceof TimeType) {
            if (((TimeType) type).getPrecision() > 9) {
                // String representation is not as nice as java.time, but it's currently the best available for picoseconds precision
                return (String) value;
            }
            return DateTimeFormatter.ISO_LOCAL_TIME.parse(((String) value), LocalTime::from);
        }
        if (type instanceof TimeWithTimeZoneType) {
            if (((TimeWithTimeZoneType) type).getPrecision() > 9) {
                // String representation is not as nice as java.time, but it's currently the best available for picoseconds precision
                return (String) value;
            }
            return timeWithZoneOffsetFormat.parse(((String) value), OffsetTime::from);
        }
        if (type instanceof TimestampType) {
            if (((TimestampType) type).getPrecision() > 9) {
                // String representation is not as nice as java.time, but it's currently the best available for picoseconds precision
                return (String) value;
            }
            return timestampFormat.parse((String) value, LocalDateTime::from);
        }
        if (type instanceof TimestampWithTimeZoneType) {
            if (((TimestampWithTimeZoneType) type).getPrecision() > 9) {
                // String representation is not as nice as java.time, but it's currently the best available for picoseconds precision
                return (String) value;
            }
            return timestampWithTimeZoneFormat.parse((String) value, ZonedDateTime::from);
        }
        if (INTERVAL_DAY_TIME.equals(type)) {
            return new SqlIntervalDayTime(IntervalDayTime.parseMillis(String.valueOf(value)));
        }
        if (INTERVAL_YEAR_MONTH.equals(type)) {
            return new SqlIntervalYearMonth(IntervalYearMonth.parseMonths(String.valueOf(value)));
        }
        if (type instanceof ArrayType) {
            return ((List<?>) value).stream()
                    .map(element -> convertToRowValue(((ArrayType) type).getElementType(), element))
                    .collect(toList());
        }
        if (type instanceof MapType) {
            Map<Object, Object> result = new HashMap<>();
            ((Map<?, ?>) value)
                    .forEach((k, v) -> result.put(
                            convertToRowValue(((MapType) type).getKeyType(), k),
                            convertToRowValue(((MapType) type).getValueType(), v)));
            return result;
        }
        if (type instanceof RowType) {
            List<Type> fieldTypes = type.getTypeParameters();
            List<Object> fieldValues = ((Row) value).getFields().stream()
                    .map(RowField::getValue)
                    .collect(toList()); // nullable
            return dataToRow(fieldTypes).apply(fieldValues);
        }
        if (type instanceof DecimalType) {
            return new BigDecimal((String) value);
        }
        if (type.getBaseName().equals("HyperLogLog")) {
            return value;
        }
        if (type.getBaseName().equals("Geometry")) {
            return value;
        }
        if (type.getBaseName().equals("SphericalGeography")) {
            return value;
        }
        if (type.getBaseName().equals("ObjectId")) {
            return value;
        }
        if (type.getBaseName().equals("Bogus")) {
            return value;
        }
        if (JSON.equals(type)) {
            return value;
        }
        throw new AssertionError("unhandled type: " + type);
    }
}
