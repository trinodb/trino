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
package io.prestosql.plugin.kafka.encoder.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.kafka.KafkaColumnHandle;
import io.prestosql.plugin.kafka.encoder.json.format.DateTimeFormat;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;
import io.prestosql.testing.TestingConnectorSession;
import org.assertj.core.api.ThrowableAssert;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.plugin.kafka.encoder.json.format.DateTimeFormat.CUSTOM_DATE_TIME;
import static io.prestosql.plugin.kafka.encoder.json.format.DateTimeFormat.ISO8601;
import static io.prestosql.plugin.kafka.encoder.json.format.DateTimeFormat.MILLISECONDS_SINCE_EPOCH;
import static io.prestosql.plugin.kafka.encoder.json.format.DateTimeFormat.RFC2822;
import static io.prestosql.plugin.kafka.encoder.json.format.DateTimeFormat.SECONDS_SINCE_EPOCH;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestJsonEncoder
{
    private static final ConnectorSession SESSION = TestingConnectorSession.builder().build();
    private static final JsonRowEncoderFactory ENCODER_FACTORY = new JsonRowEncoderFactory(new ObjectMapper());

    private void assertUnsupportedColumnTypeException(ThrowableAssert.ThrowingCallable callable)
    {
        assertThatThrownBy(callable)
                .isInstanceOf(RuntimeException.class)
                .hasMessageMatching("Unsupported column type .* for column .*");
    }

    private void assertUnsupportedDataFormatException(ThrowableAssert.ThrowingCallable callable)
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

    private void assertSupportedDataType(EmptyFunctionalInterface functionalInterface)
    {
        functionalInterface.apply();
    }

    private void singleColumnEncoder(Type type)
    {
        ENCODER_FACTORY.create(SESSION, Optional.empty(), ImmutableList.of(new KafkaColumnHandle("default", type, "default", null, null, false, false, false)));
    }

    private void singleColumnEncoder(Type type, DateTimeFormat dataFormat, String formatHint)
    {
        requireNonNull(dataFormat, "dataFormat is null");
        if (dataFormat.equals(CUSTOM_DATE_TIME)) {
            ENCODER_FACTORY.create(SESSION, Optional.empty(), ImmutableList.of(new KafkaColumnHandle("default", type, "default", dataFormat.toString(), formatHint, false, false, false)));
        }
        else {
            ENCODER_FACTORY.create(SESSION, Optional.empty(), ImmutableList.of(new KafkaColumnHandle("default", type, "default", dataFormat.toString(), null, false, false, false)));
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

        assertSupportedDataType(() -> singleColumnEncoder(TIMESTAMP, RFC2822, ""));
        assertSupportedDataType(() -> singleColumnEncoder(TIMESTAMP_WITH_TIME_ZONE, RFC2822, ""));

        for (DateTimeFormat dataFormat : ImmutableList.of(CUSTOM_DATE_TIME, ISO8601)) {
            assertSupportedDataType(() -> singleColumnEncoder(DATE, dataFormat, "yyyy-dd-MM"));
            assertSupportedDataType(() -> singleColumnEncoder(TIME, dataFormat, "kk:mm:ss.SSS"));
            assertSupportedDataType(() -> singleColumnEncoder(TIME_WITH_TIME_ZONE, dataFormat, "kk:mm:ss.SSS Z"));
            assertSupportedDataType(() -> singleColumnEncoder(TIMESTAMP, dataFormat, "yyyy-dd-MM kk:mm:ss.SSS"));
            assertSupportedDataType(() -> singleColumnEncoder(TIMESTAMP_WITH_TIME_ZONE, dataFormat, "yyyy-dd-MM kk:mm:ss.SSS Z"));
        }

        for (DateTimeFormat dataFormat : ImmutableList.of(MILLISECONDS_SINCE_EPOCH, SECONDS_SINCE_EPOCH)) {
            assertSupportedDataType(() -> singleColumnEncoder(TIME, dataFormat, null));
            assertSupportedDataType(() -> singleColumnEncoder(TIMESTAMP, dataFormat, null));
        }

        assertUnsupportedColumnTypeException(() -> singleColumnEncoder(REAL));
        assertUnsupportedColumnTypeException(() -> singleColumnEncoder(createDecimalType(10, 4)));
        assertUnsupportedColumnTypeException(() -> singleColumnEncoder(VARBINARY));

        assertUnsupportedDataFormatException(() -> singleColumnEncoder(DATE));
        assertUnsupportedDataFormatException(() -> singleColumnEncoder(TIME));
        assertUnsupportedDataFormatException(() -> singleColumnEncoder(TIME_WITH_TIME_ZONE));
        assertUnsupportedDataFormatException(() -> singleColumnEncoder(TIMESTAMP));
        assertUnsupportedDataFormatException(() -> singleColumnEncoder(TIMESTAMP_WITH_TIME_ZONE));

        for (DateTimeFormat dataFormat : ImmutableList.of(MILLISECONDS_SINCE_EPOCH, SECONDS_SINCE_EPOCH)) {
            assertUnsupportedColumnTypeException(() -> singleColumnEncoder(DATE, dataFormat, null));
        }
    }
}
