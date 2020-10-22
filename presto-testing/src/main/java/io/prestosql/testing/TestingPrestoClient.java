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
package io.prestosql.testing;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.client.IntervalDayTime;
import io.prestosql.client.IntervalYearMonth;
import io.prestosql.client.QueryData;
import io.prestosql.client.QueryStatusInfo;
import io.prestosql.client.Warning;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.TimeType;
import io.prestosql.spi.type.TimeWithTimeZoneType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.type.SqlIntervalDayTime;
import io.prestosql.type.SqlIntervalYearMonth;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.Collection;
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
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.testing.MaterializedResult.DEFAULT_PRECISION;
import static io.prestosql.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.prestosql.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static io.prestosql.type.IpAddressType.IPADDRESS;
import static io.prestosql.type.JsonType.JSON;
import static io.prestosql.type.UuidType.UUID;
import static io.prestosql.util.MoreLists.mappedCopy;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.util.stream.Collectors.toList;

public class TestingPrestoClient
        extends AbstractTestingPrestoClient<MaterializedResult>
{
    private static final DateTimeFormatter timeWithZoneOffsetFormat = DateTimeFormatter.ofPattern("HH:mm:ss[.SSS]XXX");

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

    public TestingPrestoClient(TestingPrestoServer prestoServer, Session defaultSession)
    {
        super(prestoServer, defaultSession);
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
        else if (TINYINT.equals(type)) {
            return ((Number) value).byteValue();
        }
        else if (SMALLINT.equals(type)) {
            return ((Number) value).shortValue();
        }
        else if (INTEGER.equals(type)) {
            return ((Number) value).intValue();
        }
        else if (BIGINT.equals(type)) {
            return ((Number) value).longValue();
        }
        else if (DOUBLE.equals(type)) {
            return ((Number) value).doubleValue();
        }
        else if (REAL.equals(type)) {
            return ((Number) value).floatValue();
        }
        else if (UUID.equals(type)) {
            return java.util.UUID.fromString((String) value);
        }
        else if (IPADDRESS.equals(type)) {
            return value;
        }
        else if (type instanceof VarcharType) {
            return value;
        }
        else if (type instanceof CharType) {
            return value;
        }
        else if (VARBINARY.equals(type)) {
            return value;
        }
        else if (DATE.equals(type)) {
            return DateTimeFormatter.ISO_LOCAL_DATE.parse(((String) value), LocalDate::from);
        }
        else if (type instanceof TimeType) {
            return DateTimeFormatter.ISO_LOCAL_TIME.parse(((String) value), LocalTime::from);
        }
        else if (type instanceof TimeWithTimeZoneType) {
            return timeWithZoneOffsetFormat.parse(((String) value), OffsetTime::from);
        }
        else if (type instanceof TimestampType) {
            return timestampFormat.parse((String) value, LocalDateTime::from);
        }
        else if (type instanceof TimestampWithTimeZoneType) {
            return timestampWithTimeZoneFormat.parse((String) value, ZonedDateTime::from);
        }
        else if (INTERVAL_DAY_TIME.equals(type)) {
            return new SqlIntervalDayTime(IntervalDayTime.parseMillis(String.valueOf(value)));
        }
        else if (INTERVAL_YEAR_MONTH.equals(type)) {
            return new SqlIntervalYearMonth(IntervalYearMonth.parseMonths(String.valueOf(value)));
        }
        else if (type instanceof ArrayType) {
            return ((List<?>) value).stream()
                    .map(element -> convertToRowValue(((ArrayType) type).getElementType(), element))
                    .collect(toList());
        }
        else if (type instanceof MapType) {
            Map<Object, Object> result = new HashMap<>();
            ((Map<?, ?>) value)
                    .forEach((k, v) -> result.put(
                            convertToRowValue(((MapType) type).getKeyType(), k),
                            convertToRowValue(((MapType) type).getValueType(), v)));
            return result;
        }
        else if (type instanceof RowType) {
            List<Type> fieldTypes = type.getTypeParameters();
            Collection<?> values = ((Map<?, ?>) value).values();
            return dataToRow(fieldTypes).apply(new ArrayList<>(values));
        }
        else if (type instanceof DecimalType) {
            return new BigDecimal((String) value);
        }
        else if (type.getBaseName().equals("ObjectId")) {
            return value;
        }
        else if (type.getBaseName().equals("Bogus")) {
            return value;
        }
        else if (JSON.equals(type)) {
            return value;
        }
        else {
            throw new AssertionError("unhandled type: " + type);
        }
    }
}
