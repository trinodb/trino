/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import io.prestosql.tests.datatype.DataType;
import io.prestosql.tests.datatype.DataTypeTest;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.function.BiFunction;
import java.util.function.IntFunction;
import java.util.function.ToIntFunction;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.starburstdata.presto.plugin.oracle.OracleDataTypes.dateDataType;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.tests.datatype.DataType.dataType;
import static java.lang.String.format;
import static java.math.RoundingMode.UNNECESSARY;
import static java.nio.charset.StandardCharsets.UTF_16LE;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class OracleTypeMappingData
{
    private OracleTypeMappingData() {}

    private static final ZoneId TEST_TIME_ZONE = ZoneId.of("America/Bahia_Banderas");

    /**
     * Return the JVM time zone if it is the test time zone, otherwise throw an
     * {@code IllegalStateException}.
     */
    public static ZoneId getJvmTestTimeZone()
    {
        ZoneId zone = ZoneId.systemDefault();
        checkState(TEST_TIME_ZONE.equals(zone), "Assumed JVM time zone was " +
                TEST_TIME_ZONE.getId() + ", but found " + zone.getId());
        return zone;
    }

    /**
     * Allow a {@link DataType} to test null values
     */
    private static <T> DataType<T> makeNullable(DataType<T> type)
    {
        return dataType(type.getInsertType(), type.getPrestoResultType(),
                type::toLiteral,
                value -> value == null ? null : type.toPrestoQueryResult(value));
    }

    /* DataType factories */

    static DataTypeTest floatTests(DataType<Float> floatType)
    {
        floatType = makeNullable(floatType);
        return DataTypeTest.create()
                .addRoundTrip(floatType, 123.45f)
                .addRoundTrip(floatType, Float.NaN)
                .addRoundTrip(floatType, Float.NEGATIVE_INFINITY)
                .addRoundTrip(floatType, Float.POSITIVE_INFINITY)
                .addRoundTrip(floatType, null);
    }

    static DataTypeTest doubleTests(DataType<Double> doubleType)
    {
        doubleType = makeNullable(doubleType);
        return DataTypeTest.create()
                .addRoundTrip(doubleType, 1e100d)
                .addRoundTrip(doubleType, Double.NaN)
                .addRoundTrip(doubleType, Double.POSITIVE_INFINITY)
                .addRoundTrip(doubleType, Double.NEGATIVE_INFINITY)
                .addRoundTrip(doubleType, null);
    }

    static DataTypeTest numericTests(BiFunction<Integer, Integer, DataType<BigDecimal>> decimalType)
    {
        return DataTypeTest.create()
                .addRoundTrip(decimalType.apply(3, 0), new BigDecimal("193")) // full p
                .addRoundTrip(decimalType.apply(3, 0), new BigDecimal("19")) // partial p
                .addRoundTrip(decimalType.apply(3, 0), new BigDecimal("-193")) // negative full p
                .addRoundTrip(decimalType.apply(3, 1), new BigDecimal("10.0")) // 0 decimal
                .addRoundTrip(decimalType.apply(3, 1), new BigDecimal("10.1")) // full ps
                .addRoundTrip(decimalType.apply(3, 1), new BigDecimal("-10.1")) // negative ps
                .addRoundTrip(decimalType.apply(4, 2), new BigDecimal("2")) //
                .addRoundTrip(decimalType.apply(4, 2), new BigDecimal("2.3"))
                .addRoundTrip(decimalType.apply(24, 2), new BigDecimal("2"))
                .addRoundTrip(decimalType.apply(24, 2), new BigDecimal("2.3"))
                .addRoundTrip(decimalType.apply(24, 2), new BigDecimal("123456789.3"))
                .addRoundTrip(decimalType.apply(24, 4), new BigDecimal("12345678901234567890.31"))
                .addRoundTrip(decimalType.apply(30, 5), new BigDecimal("3141592653589793238462643.38327"))
                .addRoundTrip(decimalType.apply(30, 5), new BigDecimal("-3141592653589793238462643.38327"))
                .addRoundTrip(decimalType.apply(38, 0), new BigDecimal("27182818284590452353602874713526624977"))
                .addRoundTrip(decimalType.apply(38, 0), new BigDecimal("-27182818284590452353602874713526624977"))
                .addRoundTrip(decimalType.apply(38, 38), new BigDecimal(".10000200003000040000500006000070000888"))
                .addRoundTrip(decimalType.apply(38, 38), new BigDecimal("-.27182818284590452353602874713526624977"))
                .addRoundTrip(makeNullable(decimalType.apply(10, 3)), null);
    }

    public static DataType<BigDecimal> oracleDecimalDataType(int precision, int scale)
    {
        String databaseType = format("decimal(%s, %s)", precision, scale);
        return dataType(
                databaseType,
                createDecimalType(precision, scale),
                bigDecimal -> format("CAST(TO_NUMBER('%s', '%s') AS %s)", bigDecimal.toPlainString(), toNumberFormatMask(bigDecimal), databaseType),
                bigDecimal -> bigDecimal.setScale(scale, UNNECESSARY));
    }

    private static String toNumberFormatMask(BigDecimal bigDecimal)
    {
        return bigDecimal.toPlainString()
                .replace("-", "")
                .replaceAll("[\\d]", "9");
    }

    static DataTypeTest varbinaryTests(DataType<byte[]> binaryType)
    {
        binaryType = makeNullable(binaryType);
        return DataTypeTest.create()
                .addRoundTrip(binaryType, "varbinary".getBytes(UTF_8))
                .addRoundTrip(binaryType, "Piękna łąka w 東京都".getBytes(UTF_8))
                .addRoundTrip(binaryType, "Bag full of \ud83d\udcb0".getBytes(UTF_16LE))
                .addRoundTrip(binaryType, null)
                .addRoundTrip(binaryType, new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 13, -7, 54, 122, -89, 0, 0, 0});
    }

    // TODO: Replace this to not take maxSize
    static DataTypeTest basicCharacterTests(
            IntFunction<DataType<String>> typeConstructor, int maxSize)
    {
        IntFunction<DataType<String>> type = i -> makeNullable(typeConstructor.apply(i));
        return DataTypeTest.create()
                .addRoundTrip(type.apply(10), "string 010")
                .addRoundTrip(type.apply(20), "string 20")
                .addRoundTrip(type.apply(maxSize), "string max size")
                .addRoundTrip(type.apply(5), null);
    }

    static DataTypeTest unicodeTests(IntFunction<DataType<String>> typeConstructor,
            ToIntFunction<String> stringLength, int maxSize)
    {
        String unicodeText = "攻殻機動隊";
        String nonBmpCharacter = "\ud83d\ude02";
        int unicodeLength = stringLength.applyAsInt(unicodeText);
        int nonBmpLength = stringLength.applyAsInt(nonBmpCharacter);

        IntFunction<DataType<String>> type = i -> makeNullable(typeConstructor.apply(i));
        return DataTypeTest.create()
                .addRoundTrip(type.apply(unicodeLength), unicodeText)
                .addRoundTrip(type.apply(unicodeLength + 8), unicodeText)
                .addRoundTrip(type.apply(maxSize), unicodeText)
                .addRoundTrip(type.apply(nonBmpLength), nonBmpCharacter)
                .addRoundTrip(type.apply(nonBmpLength + 5), nonBmpCharacter);
    }

    static DataTypeTest unboundedVarcharTests(DataType<String> dataType)
    {
        DataType<String> type = makeNullable(dataType);
        // The string length function and max size are placeholders;
        // the data type isn't parameterized.
        return unicodeTests(ignored -> dataType, ignored -> 0, 0)
                .addRoundTrip(type, "clob")
                .addRoundTrip(type, null);
    }

    /**
     * These tests should be executed in {@link io.prestosql.Session Sessions}
     * configured for the JVM time zone, Europe/Vilnius, and UTC.
     */
    static DataTypeTest legacyDateTests()
    {
        ZoneId someZone = ZoneId.of("Europe/Vilnius");

        LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone =
                LocalDate.of(1983, 10, 1);

        verify(someZone.getRules().getValidOffsets(
                dateOfLocalTimeChangeBackwardAtMidnightInSomeZone
                        .atStartOfDay().minusMinutes(1)).size() == 2);

        return DataTypeTest.create()
                // before epoch
                .addRoundTrip(dateDataType(), LocalDate.of(1952, 4, 3))
                .addRoundTrip(dateDataType(), LocalDate.of(1970, 2, 3))
                // summer on northern hemisphere (possible DST)
                .addRoundTrip(dateDataType(), LocalDate.of(2017, 7, 1))
                // winter on northern hemisphere
                // (possible DST on southern hemisphere)
                .addRoundTrip(dateDataType(), LocalDate.of(2017, 1, 1))
                .addRoundTrip(dateDataType(),
                        dateOfLocalTimeChangeBackwardAtMidnightInSomeZone);
    }

    /**
     * These tests should be executed in {@link io.prestosql.Session Sessions}
     * configured for the JVM time zone, Europe/Vilnius, and UTC.
     */
    static DataTypeTest nonLegacyDateTests()
    {
        // Note: these test cases are duplicates of those for PostgreSQL and MySQL.

        ZoneId jvmZone = getJvmTestTimeZone();

        LocalDate dateOfLocalTimeChangeForwardAtMidnightInJvmZone =
                LocalDate.of(1970, 1, 1);

        verify(jvmZone.getRules().getValidOffsets(
                dateOfLocalTimeChangeForwardAtMidnightInJvmZone
                        .atStartOfDay()).isEmpty());

        ZoneId someZone = ZoneId.of("Europe/Vilnius");

        LocalDate dateOfLocalTimeChangeForwardAtMidnightInSomeZone =
                LocalDate.of(1983, 4, 1);

        verify(someZone.getRules().getValidOffsets(
                dateOfLocalTimeChangeForwardAtMidnightInSomeZone
                        .atStartOfDay()).isEmpty());

        LocalDate dateOfLocalTimeChangeBackwardAtMidnightInSomeZone =
                LocalDate.of(1983, 10, 1);

        verify(someZone.getRules().getValidOffsets(
                dateOfLocalTimeChangeBackwardAtMidnightInSomeZone
                        .atStartOfDay().minusMinutes(1)).size() == 2);

        return DataTypeTest.create()
                // before epoch
                .addRoundTrip(dateDataType(), LocalDate.of(1952, 4, 3))
                .addRoundTrip(dateDataType(), LocalDate.of(1970, 1, 1))
                .addRoundTrip(dateDataType(), LocalDate.of(1970, 2, 3))
                // summer on northern hemisphere (possible DST)
                .addRoundTrip(dateDataType(), LocalDate.of(2017, 7, 1))
                // winter on northern hemisphere
                // (possible DST on southern hemisphere)
                .addRoundTrip(dateDataType(), LocalDate.of(2017, 1, 1))
                .addRoundTrip(dateDataType(),
                        dateOfLocalTimeChangeForwardAtMidnightInJvmZone)
                .addRoundTrip(dateDataType(),
                        dateOfLocalTimeChangeForwardAtMidnightInSomeZone)
                .addRoundTrip(dateDataType(),
                        dateOfLocalTimeChangeBackwardAtMidnightInSomeZone);
    }

    static DataTypeTest timestampTests(DataType<LocalDateTime> dataType)
    {
        DataType<LocalDateTime> type = makeNullable(dataType);
        return DataTypeTest.create()
                .addRoundTrip(type, LocalDateTime.parse("2000-01-01T00:00:00.000"))
                .addRoundTrip(type, LocalDateTime.parse("2018-07-02T12:12:43.321"))
                .addRoundTrip(type, LocalDateTime.parse("1970-01-01T00:00:00.000"))
                .addRoundTrip(type, null);
    }

    static DataTypeTest timestampWithTimeZoneTests(DataType<ZonedDateTime> dataType)
    {
        DataType<ZonedDateTime> type = makeNullable(dataType);
        return DataTypeTest.create()
                .addRoundTrip(type, ZonedDateTime.parse("2000-01-01T00:00:00.000-06:00"))
                .addRoundTrip(type, ZonedDateTime.parse("2018-07-02T12:11:35.123-04:00"))
                .addRoundTrip(type, ZonedDateTime.parse("1970-01-01T00:00:00.000+00:00"))
                .addRoundTrip(type, null);
    }
}
