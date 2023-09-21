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
package io.trino.orc;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.SqlVarbinary;
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Lists.newArrayList;
import static io.trino.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.Chars.padSpaces;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.DateTimeTestingUtils.sqlTimestampOf;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.nCopies;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

public abstract class AbstractTestOrcReader
{
    private static final int CHAR_LENGTH = 10;

    private static final DecimalType DECIMAL_TYPE_PRECISION_2 = DecimalType.createDecimalType(2, 1);
    private static final DecimalType DECIMAL_TYPE_PRECISION_4 = DecimalType.createDecimalType(4, 2);
    private static final DecimalType DECIMAL_TYPE_PRECISION_8 = DecimalType.createDecimalType(8, 4);
    private static final DecimalType DECIMAL_TYPE_PRECISION_17 = DecimalType.createDecimalType(17, 8);
    private static final DecimalType DECIMAL_TYPE_PRECISION_18 = DecimalType.createDecimalType(18, 8);
    private static final DecimalType DECIMAL_TYPE_PRECISION_38 = DecimalType.createDecimalType(38, 16);
    private static final CharType CHAR = createCharType(CHAR_LENGTH);

    private final OrcTester tester;

    public AbstractTestOrcReader(OrcTester tester)
    {
        this.tester = tester;
    }

    @BeforeClass
    public void setUp()
    {
        assertEquals(DateTimeZone.getDefault(), HIVE_STORAGE_TIME_ZONE);
    }

    @Test
    public void testBooleanSequence()
            throws Exception
    {
        tester.testRoundTrip(BOOLEAN, newArrayList(limit(cycle(ImmutableList.of(true, false, false)), 30_000)));
    }

    @Test
    public void testLongSequence()
            throws Exception
    {
        testRoundTripNumeric(intsBetween(0, 31_234));
    }

    @Test
    public void testNegativeLongSequence()
            throws Exception
    {
        // A flaw in ORC encoding makes it impossible to represent timestamp
        // between 1969-12-31 23:59:59.000, exclusive, and 1970-01-01 00:00:00.000, exclusive.
        // Therefore, such data won't round trip and are skipped from test.
        testRoundTripNumeric(intsBetween(-31_234, -999));
    }

    @Test
    public void testLongSequenceWithHoles()
            throws Exception
    {
        testRoundTripNumeric(skipEvery(5, intsBetween(0, 31_234)));
    }

    @Test
    public void testLongDirect()
            throws Exception
    {
        testRoundTripNumeric(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 30_000));
    }

    @Test
    public void testLongDirect2()
            throws Exception
    {
        List<Integer> values = new ArrayList<>(31_234);
        for (int i = 0; i < 31_234; i++) {
            values.add(i);
        }
        Collections.shuffle(values, new Random(0));
        testRoundTripNumeric(values);
    }

    @Test
    public void testLongShortRepeat()
            throws Exception
    {
        testRoundTripNumeric(limit(repeatEach(4, cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17))), 30_000));
    }

    @Test
    public void testLongPatchedBase()
            throws Exception
    {
        testRoundTripNumeric(limit(cycle(concat(intsBetween(0, 18), intsBetween(0, 18), ImmutableList.of(30_000, 20_000, 400_000, 30_000, 20_000))), 30_000));
    }

    @Test
    public void testLongStrideDictionary()
            throws Exception
    {
        testRoundTripNumeric(concat(ImmutableList.of(1), nCopies(9999, 123), ImmutableList.of(2), nCopies(9999, 123)));
    }

    private void testRoundTripNumeric(Iterable<? extends Number> values)
            throws Exception
    {
        List<Long> writeValues = ImmutableList.copyOf(values).stream()
                .map(Number::longValue)
                .collect(toList());
        tester.testRoundTrip(
                TINYINT,
                writeValues.stream()
                        .map(Long::byteValue) // truncate values to byte range
                        .collect(toList()));

        tester.testRoundTrip(
                SMALLINT,
                writeValues.stream()
                        .map(Long::shortValue) // truncate values to short range
                        .collect(toList()));

        tester.testRoundTrip(
                INTEGER,
                writeValues.stream()
                        .map(Long::intValue) // truncate values to int range
                        .collect(toList()));

        tester.testRoundTrip(BIGINT, writeValues);

        tester.testRoundTrip(
                DATE,
                writeValues.stream()
                        .map(Long::intValue)
                        .map(SqlDate::new)
                        .collect(toList()));

        tester.testRoundTrip(
                TIMESTAMP_MILLIS,
                writeValues.stream()
                        .map(timestamp -> sqlTimestampOf(3, timestamp))
                        .collect(toList()));
    }

    @Test
    public void testFloatSequence()
            throws Exception
    {
        tester.testRoundTrip(REAL, floatSequence(0.0f, 0.1f, 30_000));
    }

    @Test
    public void testFloatNaNInfinity()
            throws Exception
    {
        tester.testRoundTrip(REAL, ImmutableList.of(1000.0f, -1.23f, Float.POSITIVE_INFINITY));
        tester.testRoundTrip(REAL, ImmutableList.of(-1000.0f, Float.NEGATIVE_INFINITY, 1.23f));
        tester.testRoundTrip(REAL, ImmutableList.of(0.0f, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY));

        tester.testRoundTrip(REAL, ImmutableList.of(Float.NaN, -0.0f, 1.0f));
        tester.testRoundTrip(REAL, ImmutableList.of(Float.NaN, -1.0f, Float.POSITIVE_INFINITY));
        tester.testRoundTrip(REAL, ImmutableList.of(Float.NaN, Float.NEGATIVE_INFINITY, 1.0f));
        tester.testRoundTrip(REAL, ImmutableList.of(Float.NaN, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY));
    }

    @Test
    public void testDoubleSequence()
            throws Exception
    {
        tester.testRoundTrip(DOUBLE, doubleSequence(0, 0.1, 30_000));
    }

    @Test
    public void testDecimalSequence()
            throws Exception
    {
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_2, decimalSequence("-30", "1", 60, 2, 1));
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_4, decimalSequence("-3000", "1", 60_00, 4, 2));
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_8, decimalSequence("-3000000", "100", 60_000, 8, 4));
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_17, decimalSequence("-30000000000", "1000000", 60_000, 17, 8));
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_18, decimalSequence("-30000000000", "1000000", 60_000, 18, 8));
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_38, decimalSequence("-3000000000000000000", "100000000000000", 60_000, 38, 16));

        Random random = new Random(0);
        List<SqlDecimal> values = new ArrayList<>();
        values.add(new SqlDecimal(new BigInteger("9".repeat(18)), DECIMAL_TYPE_PRECISION_18.getPrecision(), DECIMAL_TYPE_PRECISION_18.getScale()));
        values.add(new SqlDecimal(new BigInteger("-" + "9".repeat(18)), DECIMAL_TYPE_PRECISION_18.getPrecision(), DECIMAL_TYPE_PRECISION_18.getScale()));
        BigInteger nextValue = BigInteger.ONE;
        for (int i = 0; i < 59; i++) {
            values.add(new SqlDecimal(nextValue, DECIMAL_TYPE_PRECISION_18.getPrecision(), DECIMAL_TYPE_PRECISION_18.getScale()));
            values.add(new SqlDecimal(nextValue.negate(), DECIMAL_TYPE_PRECISION_18.getPrecision(), DECIMAL_TYPE_PRECISION_18.getScale()));
            nextValue = nextValue.multiply(BigInteger.valueOf(2));
        }
        for (int i = 0; i < 100_000; ++i) {
            BigInteger value = new BigInteger(59, random);
            if (random.nextBoolean()) {
                value = value.negate();
            }
            values.add(new SqlDecimal(value, DECIMAL_TYPE_PRECISION_18.getPrecision(), DECIMAL_TYPE_PRECISION_18.getScale()));
        }
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_18, values);

        random = new Random(0);
        values = new ArrayList<>();
        values.add(new SqlDecimal(new BigInteger("9".repeat(38)), DECIMAL_TYPE_PRECISION_38.getPrecision(), DECIMAL_TYPE_PRECISION_38.getScale()));
        values.add(new SqlDecimal(new BigInteger("-" + "9".repeat(38)), DECIMAL_TYPE_PRECISION_38.getPrecision(), DECIMAL_TYPE_PRECISION_38.getScale()));
        nextValue = BigInteger.ONE;
        for (int i = 0; i < 127; i++) {
            values.add(new SqlDecimal(nextValue, 38, 16));
            values.add(new SqlDecimal(nextValue.negate(), 38, 16));
            nextValue = nextValue.multiply(BigInteger.valueOf(2));
        }
        for (int i = 0; i < 100_000; ++i) {
            BigInteger value = new BigInteger(126, random);
            if (random.nextBoolean()) {
                value = value.negate();
            }
            values.add(new SqlDecimal(value, DECIMAL_TYPE_PRECISION_38.getPrecision(), DECIMAL_TYPE_PRECISION_38.getScale()));
        }
        tester.testRoundTrip(DECIMAL_TYPE_PRECISION_38, values);
    }

    @Test
    public void testDoubleNaNInfinity()
            throws Exception
    {
        tester.testRoundTrip(DOUBLE, ImmutableList.of(1000.0, -1.0, Double.POSITIVE_INFINITY));
        tester.testRoundTrip(DOUBLE, ImmutableList.of(-1000.0, Double.NEGATIVE_INFINITY, 1.0));
        tester.testRoundTrip(DOUBLE, ImmutableList.of(0.0, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY));

        tester.testRoundTrip(DOUBLE, ImmutableList.of(Double.NaN, -1.0, 1.0));
        tester.testRoundTrip(DOUBLE, ImmutableList.of(Double.NaN, -1.0, Double.POSITIVE_INFINITY));
        tester.testRoundTrip(DOUBLE, ImmutableList.of(Double.NaN, Double.NEGATIVE_INFINITY, 1.0));
        tester.testRoundTrip(DOUBLE, ImmutableList.of(Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY));
    }

    @Test
    public void testTimestampMillis()
            throws Exception
    {
        Map<String, SqlTimestamp> map = ImmutableMap.<String, SqlTimestamp>builder()
                .put("1970-01-01 00:00:00.000", SqlTimestamp.fromMillis(3, 0))
                .put("1970-01-01 00:00:00.010", SqlTimestamp.fromMillis(3, 10))
                .put("1969-12-20 13:39:05.679", SqlTimestamp.fromMillis(3, -987654321L))
                .put("1969-12-18 23:55:43.211", SqlTimestamp.fromMillis(3, -1123456789L))
                .put("1970-01-14 00:04:16.789", SqlTimestamp.fromMillis(3, 1123456789L))
                .put("2001-09-10 12:04:16.789", SqlTimestamp.fromMillis(3, 1000123456789L))
                .put("2019-12-05 13:41:39.564", SqlTimestamp.fromMillis(3, 1575553299564L))
                .buildOrThrow();
        map.forEach((expected, value) -> assertEquals(value.toString(), expected));
        tester.testRoundTrip(TIMESTAMP_MILLIS, newArrayList(limit(cycle(map.values()), 30_000)));
    }

    @Test
    public void testTimeMicros()
            throws Exception
    {
        Map<String, SqlTime> map = ImmutableMap.<String, SqlTime>builder()
                .put("00:00:00.000000", SqlTime.newInstance(6, 0L))
                .put("12:05:19.257000", SqlTime.newInstance(6, 43519257000000000L))
                .put("17:37:07.638000", SqlTime.newInstance(6, 63427638000000000L))
                .put("05:17:37.346000", SqlTime.newInstance(6, 19057346000000000L))
                .put("06:09:00.988000", SqlTime.newInstance(6, 22140988000000000L))
                .put("13:31:34.185000", SqlTime.newInstance(6, 48694185000000000L))
                .put("01:09:07.185000", SqlTime.newInstance(6, 4147185000000000L))
                .put("20:43:39.822000", SqlTime.newInstance(6, 74619822000000000L))
                .put("23:59:59.999000", SqlTime.newInstance(6, 86399999000000000L))
                .buildOrThrow();
        map.forEach((expected, value) -> assertEquals(value.toString(), expected));
        tester.testRoundTrip(TIME_MICROS, newArrayList(limit(cycle(map.values()), 30_000)));
    }

    @Test
    public void testTimestampMicros()
            throws Exception
    {
        Map<String, SqlTimestamp> map = ImmutableMap.<String, SqlTimestamp>builder()
                .put("1970-01-01 00:00:00.000000", SqlTimestamp.newInstance(6, 0, 0))
                .put("1970-01-01 00:00:00.010222", SqlTimestamp.newInstance(6, 10222, 0))
                .put("1969-12-20 13:39:05.678544", SqlTimestamp.newInstance(6, -987654321456L, 0))
                .put("1969-12-18 23:55:43.210235", SqlTimestamp.newInstance(6, -1123456789765L, 0))
                .put("1970-01-14 00:04:16.789123", SqlTimestamp.newInstance(6, 1123456789123L, 0))
                .put("2001-09-10 12:04:16.789123", SqlTimestamp.newInstance(6, 1000123456789123L, 0))
                .put("2019-12-05 13:41:39.564321", SqlTimestamp.newInstance(6, 1575553299564321L, 0))
                .buildOrThrow();
        map.forEach((expected, value) -> assertEquals(value.toString(), expected));
        tester.testRoundTrip(TIMESTAMP_MICROS, newArrayList(limit(cycle(map.values()), 30_000)));
    }

    @Test
    public void testTimestampNanos()
            throws Exception
    {
        Map<String, SqlTimestamp> map = ImmutableMap.<String, SqlTimestamp>builder()
                .put("1970-01-01 00:00:00.000000000", SqlTimestamp.newInstance(9, 0, 0))
                .put("1970-01-01 00:00:00.010222333", SqlTimestamp.newInstance(9, 10222, 333_000))
                .put("1969-12-20 13:39:05.678544123", SqlTimestamp.newInstance(9, -987654321456L, 123_000))
                .put("1969-12-18 23:55:43.210235123", SqlTimestamp.newInstance(9, -1123456789765L, 123_000))
                .put("1970-01-14 00:04:16.789123456", SqlTimestamp.newInstance(9, 1123456789123L, 456_000))
                .put("2001-09-10 12:04:16.789123456", SqlTimestamp.newInstance(9, 1000123456789123L, 456_000))
                .put("2019-12-05 13:41:39.564321789", SqlTimestamp.newInstance(9, 1575553299564321L, 789_000))
                .buildOrThrow();
        map.forEach((expected, value) -> assertEquals(value.toString(), expected));
        tester.testRoundTrip(TIMESTAMP_NANOS, newArrayList(limit(cycle(map.values()), 30_000)));
    }

    @Test
    public void testInstantMillis()
            throws Exception
    {
        Map<String, SqlTimestampWithTimeZone> map = ImmutableMap.<String, SqlTimestampWithTimeZone>builder()
                .put("1970-01-01 00:00:00.000 UTC", SqlTimestampWithTimeZone.newInstance(3, 0L, 0, UTC_KEY))
                .put("1970-01-01 00:00:00.010 UTC", SqlTimestampWithTimeZone.newInstance(3, 10, 0, UTC_KEY))
                .put("1969-12-20 13:39:05.679 UTC", SqlTimestampWithTimeZone.newInstance(3, -987654321L, 0, UTC_KEY))
                .put("1969-12-18 23:55:43.211 UTC", SqlTimestampWithTimeZone.newInstance(3, -1123456789L, 0, UTC_KEY))
                .put("1970-01-14 00:04:16.789 UTC", SqlTimestampWithTimeZone.newInstance(3, 1123456789L, 0, UTC_KEY))
                .put("2001-09-10 12:04:16.789 UTC", SqlTimestampWithTimeZone.newInstance(3, 1000123456789L, 0, UTC_KEY))
                .put("2019-12-05 13:41:39.564 UTC", SqlTimestampWithTimeZone.newInstance(3, 1575553299564L, 0, UTC_KEY))
                .buildOrThrow();
        map.forEach((expected, value) -> assertEquals(value.toString(), expected));
        tester.testRoundTrip(TIMESTAMP_TZ_MILLIS, newArrayList(limit(cycle(map.values()), 30_000)));
    }

    @Test
    public void testInstantMicros()
            throws Exception
    {
        Map<String, SqlTimestampWithTimeZone> map = ImmutableMap.<String, SqlTimestampWithTimeZone>builder()
                .put("1970-01-01 00:00:00.000000 UTC", SqlTimestampWithTimeZone.newInstance(6, 0, 0, UTC_KEY))
                .put("1970-01-01 00:00:00.010222 UTC", SqlTimestampWithTimeZone.newInstance(6, 10, 222_000_000, UTC_KEY))
                .put("1969-12-20 13:39:05.679456 UTC", SqlTimestampWithTimeZone.newInstance(6, -987654321L, 456_000_000, UTC_KEY))
                .put("1969-12-18 23:55:43.211765 UTC", SqlTimestampWithTimeZone.newInstance(6, -1123456789L, 765_000_000, UTC_KEY))
                .put("1970-01-14 00:04:16.789123 UTC", SqlTimestampWithTimeZone.newInstance(6, 1123456789L, 123_000_000, UTC_KEY))
                .put("2001-09-10 12:04:16.789123 UTC", SqlTimestampWithTimeZone.newInstance(6, 1000123456789L, 123_000_000, UTC_KEY))
                .put("2019-12-05 13:41:39.564321 UTC", SqlTimestampWithTimeZone.newInstance(6, 1575553299564L, 321_000_000, UTC_KEY))
                .buildOrThrow();
        map.forEach((expected, value) -> assertEquals(value.toString(), expected));
        tester.testRoundTrip(TIMESTAMP_TZ_MICROS, newArrayList(limit(cycle(map.values()), 30_000)));
    }

    @Test
    public void testInstantNanos()
            throws Exception
    {
        Map<String, SqlTimestampWithTimeZone> map = ImmutableMap.<String, SqlTimestampWithTimeZone>builder()
                .put("1970-01-01 00:00:00.000000000 UTC", SqlTimestampWithTimeZone.newInstance(9, 0, 0, UTC_KEY))
                .put("1970-01-01 00:00:00.010222333 UTC", SqlTimestampWithTimeZone.newInstance(9, 10, 222_333_000, UTC_KEY))
                .put("1969-12-20 13:39:05.679456123 UTC", SqlTimestampWithTimeZone.newInstance(9, -987654321L, 456_123_000, UTC_KEY))
                .put("1969-12-18 23:55:43.211765123 UTC", SqlTimestampWithTimeZone.newInstance(9, -1123456789L, 765_123_000, UTC_KEY))
                .put("1970-01-14 00:04:16.789123456 UTC", SqlTimestampWithTimeZone.newInstance(9, 1123456789L, 123_456_000, UTC_KEY))
                .put("2001-09-10 12:04:16.789123456 UTC", SqlTimestampWithTimeZone.newInstance(9, 1000123456789L, 123_456_000, UTC_KEY))
                .put("2019-12-05 13:41:39.564321789 UTC", SqlTimestampWithTimeZone.newInstance(9, 1575553299564L, 321_789_000, UTC_KEY))
                .buildOrThrow();
        map.forEach((expected, value) -> assertEquals(value.toString(), expected));
        tester.testRoundTrip(TIMESTAMP_TZ_NANOS, newArrayList(limit(cycle(map.values()), 30_000)));
    }

    @Test
    public void testStringUnicode()
            throws Exception
    {
        tester.testRoundTrip(VARCHAR, newArrayList(limit(cycle(ImmutableList.of("apple", "apple pie", "apple\uD835\uDC03", "apple\uFFFD")), 30_000)));
    }

    @Test
    public void testStringDirectSequence()
            throws Exception
    {
        tester.testRoundTrip(
                VARCHAR,
                intsBetween(0, 30_000).stream()
                        .map(Object::toString)
                        .collect(toList()));
    }

    @Test
    public void testStringDictionarySequence()
            throws Exception
    {
        tester.testRoundTrip(
                VARCHAR,
                newArrayList(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 30_000)).stream()
                        .map(Object::toString)
                        .collect(toList()));
    }

    @Test
    public void testStringStrideDictionary()
            throws Exception
    {
        tester.testRoundTrip(VARCHAR, newArrayList(concat(ImmutableList.of("a"), nCopies(9999, "123"), ImmutableList.of("b"), nCopies(9999, "123"))));
    }

    @Test
    public void testEmptyStringSequence()
            throws Exception
    {
        tester.testRoundTrip(VARCHAR, newArrayList(limit(cycle(""), 30_000)));
    }

    @Test
    public void testCharDirectSequence()
            throws Exception
    {
        tester.testRoundTrip(
                CHAR,
                intsBetween(0, 30_000).stream()
                        .map(this::toCharValue)
                        .collect(toList()));
    }

    @Test
    public void testCharDictionarySequence()
            throws Exception
    {
        tester.testRoundTrip(
                CHAR,
                newArrayList(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 30_000)).stream()
                        .map(this::toCharValue)
                        .collect(toList()));
    }

    @Test
    public void testEmptyCharSequence()
            throws Exception
    {
        tester.testRoundTrip(CHAR, newArrayList(limit(cycle("          "), 30_000)));
    }

    private String toCharValue(Object value)
    {
        return padSpaces(value.toString(), CHAR);
    }

    @Test
    public void testBinaryDirectSequence()
            throws Exception
    {
        tester.testRoundTrip(
                VARBINARY,
                intsBetween(0, 30_000).stream()
                        .map(Object::toString)
                        .map(string -> string.getBytes(UTF_8))
                        .map(SqlVarbinary::new)
                        .collect(toList()));
    }

    @Test
    public void testBinaryDictionarySequence()
            throws Exception
    {
        tester.testRoundTrip(
                VARBINARY, ImmutableList.copyOf(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 30_000)).stream()
                        .map(Object::toString)
                        .map(string -> string.getBytes(UTF_8))
                        .map(SqlVarbinary::new)
                        .collect(toList()));
    }

    @Test
    public void testEmptyBinarySequence()
            throws Exception
    {
        tester.testRoundTrip(VARBINARY, nCopies(30_000, new SqlVarbinary(new byte[0])));
    }

    @Test
    public void testUuidDirectSequence()
            throws Exception
    {
        tester.testRoundTrip(
                UUID,
                intsBetween(0, 30_000).stream()
                        .map(i -> randomUUID())
                        .collect(toList()));
    }

    @Test
    public void testUuidDictionarySequence()
            throws Exception
    {
        tester.testRoundTrip(
                UUID, ImmutableList.copyOf(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 30_000)).stream()
                        .map(i -> new UUID(i, i))
                        .collect(toList()));
    }

    private static <T> Iterable<T> skipEvery(int n, Iterable<T> iterable)
    {
        return () -> new AbstractIterator<>()
        {
            private final Iterator<T> delegate = iterable.iterator();
            private int position;

            @Override
            protected T computeNext()
            {
                while (true) {
                    if (!delegate.hasNext()) {
                        return endOfData();
                    }

                    T next = delegate.next();
                    position++;
                    if (position <= n) {
                        return next;
                    }
                    position = 0;
                }
            }
        };
    }

    private static <T> Iterable<T> repeatEach(int n, Iterable<T> iterable)
    {
        return () -> new AbstractIterator<>()
        {
            private final Iterator<T> delegate = iterable.iterator();
            private int position;
            private T value;

            @Override
            protected T computeNext()
            {
                if (position == 0) {
                    if (!delegate.hasNext()) {
                        return endOfData();
                    }
                    value = delegate.next();
                }

                position++;
                if (position >= n) {
                    position = 0;
                }
                return value;
            }
        };
    }

    protected static List<Double> doubleSequence(double start, double step, int items)
    {
        List<Double> values = new ArrayList<>();
        double nextValue = start;
        for (int i = 0; i < items; i++) {
            values.add(nextValue);
            nextValue += step;
        }
        return values;
    }

    private static List<Float> floatSequence(float start, float step, int items)
    {
        ImmutableList.Builder<Float> values = ImmutableList.builder();
        float nextValue = start;
        for (int i = 0; i < items; i++) {
            values.add(nextValue);
            nextValue += step;
        }
        return values.build();
    }

    private static List<SqlDecimal> decimalSequence(String start, String step, int items, int precision, int scale)
    {
        BigInteger decimalStep = new BigInteger(step);

        List<SqlDecimal> values = new ArrayList<>();
        BigInteger nextValue = new BigInteger(start);
        for (int i = 0; i < items; i++) {
            values.add(new SqlDecimal(nextValue, precision, scale));
            nextValue = nextValue.add(decimalStep);
        }
        return values;
    }

    private static ContiguousSet<Integer> intsBetween(int lowerInclusive, int upperExclusive)
    {
        return ContiguousSet.create(Range.closedOpen(lowerInclusive, upperExclusive), DiscreteDomain.integers());
    }
}
