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
package io.trino.plugin.kafka.encoder.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.kafka.KafkaColumnHandle;
import io.trino.plugin.kafka.encoder.RowEncoderSpec;
import io.trino.plugin.kafka.encoder.json.format.DateTimeFormat;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.testing.TestingConnectorSession;
import org.assertj.core.api.ThrowableAssert;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.plugin.kafka.encoder.KafkaFieldType.MESSAGE;
import static io.trino.plugin.kafka.encoder.json.format.DateTimeFormat.CUSTOM_DATE_TIME;
import static io.trino.plugin.kafka.encoder.json.format.DateTimeFormat.ISO8601;
import static io.trino.plugin.kafka.encoder.json.format.DateTimeFormat.MILLISECONDS_SINCE_EPOCH;
import static io.trino.plugin.kafka.encoder.json.format.DateTimeFormat.RFC2822;
import static io.trino.plugin.kafka.encoder.json.format.DateTimeFormat.SECONDS_SINCE_EPOCH;
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
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestJsonEncoder
{
    private static final ConnectorSession SESSION = TestingConnectorSession.builder().build();
    private static final JsonRowEncoderFactory ENCODER_FACTORY = new JsonRowEncoderFactory(new ObjectMapper());
    private static final String TOPIC = "topic";

    private static void assertUnsupportedColumnTypeException(ThrowableAssert.ThrowingCallable callable)
    {
        assertThatThrownBy(callable)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Unsupported column type .* for column .*");
    }

    private static void assertUnsupportedDataFormatException(ThrowableAssert.ThrowingCallable callable)
    {
        assertThatThrownBy(callable)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Unsupported or no dataFormat .* defined for temporal column .*");
    }

    @FunctionalInterface
    interface EmptyFunctionalInterface
    {
        void apply();
    }

    private static void assertSupportedDataType(EmptyFunctionalInterface functionalInterface)
    {
        functionalInterface.apply();
    }

    private static void singleColumnEncoder(Type type)
    {
        ENCODER_FACTORY.create(SESSION, new RowEncoderSpec(JsonRowEncoder.NAME, Optional.empty(), ImmutableList.of(new KafkaColumnHandle("default", type, "default", null, null, false, false, false)), TOPIC, MESSAGE));
    }

    private static void singleColumnEncoder(Type type, DateTimeFormat dataFormat, String formatHint)
    {
        requireNonNull(dataFormat, "dataFormat is null");
        if (dataFormat.equals(CUSTOM_DATE_TIME)) {
            ENCODER_FACTORY.create(SESSION, new RowEncoderSpec(JsonRowEncoder.NAME, Optional.empty(), ImmutableList.of(new KafkaColumnHandle("default", type, "default", dataFormat.toString(), formatHint, false, false, false)), TOPIC, MESSAGE));
        }
        else {
            ENCODER_FACTORY.create(SESSION, new RowEncoderSpec(JsonRowEncoder.NAME, Optional.empty(), ImmutableList.of(new KafkaColumnHandle("default", type, "default", dataFormat.toString(), null, false, false, false)), TOPIC, MESSAGE));
        }
    }

    @Test
    public void testColumnValidation()
    {
        assertSupportedDataType(() -> singleColumnEncoder(BIGINT));
        assertSupportedDataType(() -> singleColumnEncoder(INTEGER));
        assertSupportedDataType(() -> singleColumnEncoder(SMALLINT));
        assertSupportedDataType(() -> singleColumnEncoder(TINYINT));
        assertSupportedDataType(() -> singleColumnEncoder(DOUBLE));
        assertSupportedDataType(() -> singleColumnEncoder(BOOLEAN));
        assertSupportedDataType(() -> singleColumnEncoder(createVarcharType(20)));
        assertSupportedDataType(() -> singleColumnEncoder(createUnboundedVarcharType()));

        assertSupportedDataType(() -> singleColumnEncoder(TIMESTAMP_MILLIS, RFC2822, ""));
        assertSupportedDataType(() -> singleColumnEncoder(TIMESTAMP_TZ_MILLIS, RFC2822, ""));

        for (DateTimeFormat dataFormat : ImmutableList.of(CUSTOM_DATE_TIME, ISO8601)) {
            assertSupportedDataType(() -> singleColumnEncoder(DATE, dataFormat, "yyyy-dd-MM"));
            assertSupportedDataType(() -> singleColumnEncoder(TIME_MILLIS, dataFormat, "kk:mm:ss.SSS"));
            assertSupportedDataType(() -> singleColumnEncoder(TIME_TZ_MILLIS, dataFormat, "kk:mm:ss.SSS Z"));
            assertSupportedDataType(() -> singleColumnEncoder(TIMESTAMP_MILLIS, dataFormat, "yyyy-dd-MM kk:mm:ss.SSS"));
            assertSupportedDataType(() -> singleColumnEncoder(TIMESTAMP_TZ_MILLIS, dataFormat, "yyyy-dd-MM kk:mm:ss.SSS Z"));
        }

        for (DateTimeFormat dataFormat : ImmutableList.of(MILLISECONDS_SINCE_EPOCH, SECONDS_SINCE_EPOCH)) {
            assertSupportedDataType(() -> singleColumnEncoder(TIME_MILLIS, dataFormat, null));
            assertSupportedDataType(() -> singleColumnEncoder(TIMESTAMP_MILLIS, dataFormat, null));
            assertSupportedDataType(() -> singleColumnEncoder(TIMESTAMP_TZ_MILLIS, dataFormat, null));
        }

        assertUnsupportedColumnTypeException(() -> singleColumnEncoder(REAL));
        assertUnsupportedColumnTypeException(() -> singleColumnEncoder(createDecimalType(10, 4)));
        assertUnsupportedColumnTypeException(() -> singleColumnEncoder(VARBINARY));

        assertUnsupportedDataFormatException(() -> singleColumnEncoder(DATE));
        assertUnsupportedDataFormatException(() -> singleColumnEncoder(TIME_MILLIS));
        assertUnsupportedDataFormatException(() -> singleColumnEncoder(TIME_TZ_MILLIS));
        assertUnsupportedDataFormatException(() -> singleColumnEncoder(TIMESTAMP_MILLIS));
        assertUnsupportedDataFormatException(() -> singleColumnEncoder(TIMESTAMP_TZ_MILLIS));

        for (DateTimeFormat dataFormat : ImmutableList.of(MILLISECONDS_SINCE_EPOCH, SECONDS_SINCE_EPOCH)) {
            assertUnsupportedColumnTypeException(() -> singleColumnEncoder(DATE, dataFormat, null));
        }
    }
}
