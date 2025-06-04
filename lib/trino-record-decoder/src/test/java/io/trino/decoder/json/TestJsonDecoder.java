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
package io.trino.decoder.json;

import com.google.common.collect.ImmutableSet;
import io.airlift.json.ObjectMapperProvider;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.DecoderTestColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.decoder.RowDecoder;
import io.trino.decoder.RowDecoderSpec;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.decoder.util.DecoderTestUtil.TESTING_SESSION;
import static io.trino.decoder.util.DecoderTestUtil.checkIsNull;
import static io.trino.decoder.util.DecoderTestUtil.checkValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestJsonDecoder
{
    private static final JsonRowDecoderFactory DECODER_FACTORY = new JsonRowDecoderFactory(new ObjectMapperProvider().get());

    @Test
    public void testSimple()
            throws Exception
    {
        byte[] json = TestJsonDecoder.class.getResourceAsStream("/decoder/json/message.json").readAllBytes();

        DecoderTestColumnHandle column1 = new DecoderTestColumnHandle(0, "column1", createVarcharType(100), "source", null, null, false, false, false);
        DecoderTestColumnHandle column2 = new DecoderTestColumnHandle(1, "column2", createVarcharType(10), "user/screen_name", null, null, false, false, false);
        DecoderTestColumnHandle column3 = new DecoderTestColumnHandle(2, "column3", BIGINT, "id", null, null, false, false, false);
        DecoderTestColumnHandle column4 = new DecoderTestColumnHandle(3, "column4", BIGINT, "user/statuses_count", null, null, false, false, false);
        DecoderTestColumnHandle column5 = new DecoderTestColumnHandle(4, "column5", BOOLEAN, "user/geo_enabled", null, null, false, false, false);

        Set<DecoderColumnHandle> columns = ImmutableSet.of(column1, column2, column3, column4, column5);
        RowDecoder rowDecoder = DECODER_FACTORY.create(TESTING_SESSION, new RowDecoderSpec(JsonRowDecoder.NAME, emptyMap(), columns));

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = rowDecoder.decodeRow(json)
                .orElseThrow(AssertionError::new);

        assertThat(decodedRow).hasSize(columns.size());

        checkValue(decodedRow, column1, "<a href=\"http://twitterfeed.com\" rel=\"nofollow\">twitterfeed</a>");
        checkValue(decodedRow, column2, "EKentuckyN");
        checkValue(decodedRow, column3, 493857959588286460L);
        checkValue(decodedRow, column4, 7630);
        checkValue(decodedRow, column5, true);
    }

    @Test
    public void testNonExistent()
    {
        byte[] json = "{}".getBytes(StandardCharsets.UTF_8);

        DecoderTestColumnHandle column1 = new DecoderTestColumnHandle(0, "column1", createVarcharType(100), "very/deep/varchar", null, null, false, false, false);
        DecoderTestColumnHandle column2 = new DecoderTestColumnHandle(1, "column2", BIGINT, "no_bigint", null, null, false, false, false);
        DecoderTestColumnHandle column3 = new DecoderTestColumnHandle(2, "column3", DOUBLE, "double/is_missing", null, null, false, false, false);
        DecoderTestColumnHandle column4 = new DecoderTestColumnHandle(3, "column4", BOOLEAN, "hello", null, null, false, false, false);

        Set<DecoderColumnHandle> columns = ImmutableSet.of(column1, column2, column3, column4);
        RowDecoder rowDecoder = DECODER_FACTORY.create(TESTING_SESSION, new RowDecoderSpec(JsonRowDecoder.NAME, emptyMap(), columns));

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = rowDecoder.decodeRow(json)
                .orElseThrow(AssertionError::new);

        assertThat(decodedRow).hasSize(columns.size());

        checkIsNull(decodedRow, column1);
        checkIsNull(decodedRow, column2);
        checkIsNull(decodedRow, column3);
        checkIsNull(decodedRow, column4);
    }

    @Test
    public void testStringNumber()
    {
        byte[] json = "{\"a_number\":481516,\"a_string\":\"2342\"}".getBytes(StandardCharsets.UTF_8);

        DecoderTestColumnHandle column1 = new DecoderTestColumnHandle(0, "column1", createVarcharType(100), "a_number", null, null, false, false, false);
        DecoderTestColumnHandle column2 = new DecoderTestColumnHandle(1, "column2", BIGINT, "a_number", null, null, false, false, false);
        DecoderTestColumnHandle column3 = new DecoderTestColumnHandle(2, "column3", createVarcharType(100), "a_string", null, null, false, false, false);
        DecoderTestColumnHandle column4 = new DecoderTestColumnHandle(3, "column4", BIGINT, "a_string", null, null, false, false, false);

        Set<DecoderColumnHandle> columns = ImmutableSet.of(column1, column2, column3, column4);
        RowDecoder rowDecoder = DECODER_FACTORY.create(TESTING_SESSION, new RowDecoderSpec(JsonRowDecoder.NAME, emptyMap(), columns));

        Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedRow = rowDecoder.decodeRow(json);
        assertThat(decodedRow).isPresent();

        assertThat(decodedRow.get()).hasSize(columns.size());

        checkValue(decodedRow.get(), column1, "481516");
        checkValue(decodedRow.get(), column2, 481516);
        checkValue(decodedRow.get(), column3, "2342");
        checkValue(decodedRow.get(), column4, 2342);
    }

    @Test
    public void testSupportedDataTypeValidation()
    {
        // supported types
        singleColumnDecoder(BIGINT, null);
        singleColumnDecoder(INTEGER, null);
        singleColumnDecoder(SMALLINT, null);
        singleColumnDecoder(TINYINT, null);
        singleColumnDecoder(BOOLEAN, null);
        singleColumnDecoder(DOUBLE, null);
        singleColumnDecoder(createUnboundedVarcharType(), null);
        singleColumnDecoder(createVarcharType(100), null);

        singleColumnDecoder(TIMESTAMP_MILLIS, "rfc2822");
        singleColumnDecoder(TIMESTAMP_TZ_MILLIS, "rfc2822");

        for (String dataFormat : ImmutableSet.of("iso8601", "custom-date-time")) {
            singleColumnDecoder(DATE, dataFormat);
            singleColumnDecoder(TIME_MILLIS, dataFormat);
            singleColumnDecoder(TIME_TZ_MILLIS, dataFormat);
            singleColumnDecoder(TIMESTAMP_MILLIS, dataFormat);
            singleColumnDecoder(TIMESTAMP_TZ_MILLIS, dataFormat);
        }

        for (String dataFormat : ImmutableSet.of("seconds-since-epoch", "milliseconds-since-epoch")) {
            singleColumnDecoder(TIME_MILLIS, dataFormat);
            singleColumnDecoder(TIME_TZ_MILLIS, dataFormat);
            singleColumnDecoder(TIMESTAMP_MILLIS, dataFormat);
            singleColumnDecoder(TIMESTAMP_TZ_MILLIS, dataFormat);
        }

        // some unsupported types
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(REAL, null));
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(createDecimalType(10, 4), null));
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(VARBINARY, null));

        // temporal types are not supported for default field decoder
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(DATE, null));
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(TIME_MILLIS, null));
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(TIME_TZ_MILLIS, null));
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(TIMESTAMP_MILLIS, null));
        assertUnsupportedColumnTypeException(() -> singleColumnDecoder(TIMESTAMP_TZ_MILLIS, null));

        // non temporal types are not supported by temporal field decoders
        for (String dataFormat : ImmutableSet.of("iso8601", "custom-date-time", "seconds-since-epoch", "milliseconds-since-epoch", "rfc2822")) {
            assertUnsupportedColumnTypeException(() -> singleColumnDecoder(BIGINT, dataFormat));
            assertUnsupportedColumnTypeException(() -> singleColumnDecoder(INTEGER, dataFormat));
            assertUnsupportedColumnTypeException(() -> singleColumnDecoder(SMALLINT, dataFormat));
            assertUnsupportedColumnTypeException(() -> singleColumnDecoder(TINYINT, dataFormat));
            assertUnsupportedColumnTypeException(() -> singleColumnDecoder(BOOLEAN, dataFormat));
            assertUnsupportedColumnTypeException(() -> singleColumnDecoder(DOUBLE, dataFormat));
            assertUnsupportedColumnTypeException(() -> singleColumnDecoder(createUnboundedVarcharType(), dataFormat));
            assertUnsupportedColumnTypeException(() -> singleColumnDecoder(createVarcharType(100), dataFormat));
        }

        // date are not supported by seconds-since-epoch and milliseconds-since-epoch field decoders
        for (String dataFormat : ImmutableSet.of("seconds-since-epoch", "milliseconds-since-epoch")) {
            assertUnsupportedColumnTypeException(() -> singleColumnDecoder(DATE, dataFormat));
        }
    }

    private void assertUnsupportedColumnTypeException(ThrowingCallable callable)
    {
        assertThatThrownBy(callable)
                .isInstanceOf(TrinoException.class)
                .hasMessageMatching("unsupported column type .* for column .*");
    }

    @Test
    public void testDataFormatValidation()
    {
        for (Type type : asList(TIMESTAMP_MILLIS, DOUBLE)) {
            assertThatThrownBy(() -> singleColumnDecoder(type, "wrong_format"))
                    .isInstanceOf(TrinoException.class)
                    .hasMessage("unknown data format 'wrong_format' used for column 'some_column'");
        }
    }

    private void singleColumnDecoder(Type columnType, String dataFormat)
    {
        singleColumnDecoder(columnType, "mappedField", dataFormat);
    }

    private void singleColumnDecoder(Type columnType, String mapping, String dataFormat)
    {
        String formatHint = "custom-date-time".equals(dataFormat) ? "MM/yyyy/dd H:m:s" : null;
        DECODER_FACTORY.create(TESTING_SESSION, new RowDecoderSpec(JsonRowDecoder.NAME, emptyMap(), ImmutableSet.of(new DecoderTestColumnHandle(0, "some_column", columnType, mapping, dataFormat, formatHint, false, false, false))));
    }
}
