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
package io.trino.server.protocol;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import io.trino.jmh.Benchmarks;
import io.trino.server.protocol.JsonEncodingUtils.TypeEncoder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimeWithTimeZone;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.Type;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import static io.trino.plugin.base.util.JsonUtils.jsonFactory;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TypeUtils.writeNativeValue;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
@Fork(3)
public class BenchmarkJsonEncoding
{
    private static final int POSITIONS = 1024;
    private final JsonFactory factory = jsonFactory();

    @Param({"decimal_short", "decimal_long", "date", "time", "time_tz_short", "time_tz_long", "timestamp_short", "timestamp_long", "timestamp_tz_short", "timestamp_tz_long"})
    public String typeName;

    @Param({"true", "false"})
    public boolean supportsParametricDateTime;

    private Type type;
    private Block block;
    private TypeEncoder encoder;

    @Setup
    public void setup()
    {
        type = switch (typeName) {
            case "decimal_short" -> createDecimalType(18, 6);
            case "decimal_long" -> createDecimalType(38, 12);
            case "date" -> DATE;
            case "time" -> createTimeType(12);
            case "time_tz_short" -> createTimeWithTimeZoneType(9);
            case "time_tz_long" -> createTimeWithTimeZoneType(12);
            case "timestamp_short" -> createTimestampType(6);
            case "timestamp_long" -> createTimestampType(12);
            case "timestamp_tz_short" -> createTimestampWithTimeZoneType(3);
            case "timestamp_tz_long" -> createTimestampWithTimeZoneType(12);
            default -> throw new IllegalArgumentException(typeName);
        };
        BlockBuilder builder = type.createBlockBuilder(null, POSITIONS);
        for (int position = 0; position < POSITIONS; position++) {
            if (position % 10 == 0) {
                builder.appendNull();
                continue;
            }
            long micros = 1_711_846_799_000_000L + position * 1001L;
            Object value = switch (typeName) {
                case "decimal_short" -> 123_456_789_012_345L + position;
                case "decimal_long" -> Int128.valueOf("1234567890123456789012345678901234" + (1000 + position));
                case "date" -> 20_000L + position;
                case "time" -> 43_200_000_000_000_000L + position;
                case "time_tz_short" -> packTimeWithTimeZone(43_200_000_000_000L + position, 345);
                case "time_tz_long" -> new LongTimeWithTimeZone(43_200_000_000_000_000L + position, 345);
                case "timestamp_short" -> micros;
                case "timestamp_long" -> new LongTimestamp(micros, position);
                case "timestamp_tz_short" -> packDateTimeWithZone(micros / 1000, getTimeZoneKey("Europe/Warsaw"));
                case "timestamp_tz_long" -> LongTimestampWithTimeZone.fromEpochMillisAndFraction(micros / 1000, position * 900_001, getTimeZoneKey("Europe/Warsaw"));
                default -> throw new IllegalArgumentException(typeName);
            };
            writeNativeValue(type, builder, value);
        }
        block = builder.build();
        encoder = JsonEncodingUtils.createTypeEncoder(type, supportsParametricDateTime, true, true, true);
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public void specialized()
            throws IOException
    {
        try (JsonGenerator generator = factory.createGenerator(OutputStream.nullOutputStream())) {
            generator.writeStartArray();
            for (int position = 0; position < POSITIONS; position++) {
                encoder.encode(generator, block, position);
            }
            generator.writeEndArray();
        }
    }

    @Benchmark
    @OperationsPerInvocation(POSITIONS)
    public void objectValue()
            throws IOException
    {
        try (JsonGenerator generator = factory.createGenerator(OutputStream.nullOutputStream())) {
            generator.writeStartArray();
            for (int position = 0; position < POSITIONS; position++) {
                Object value = type.getObjectValue(block, position);
                if (!supportsParametricDateTime) {
                    value = switch (value) {
                        case SqlTime time -> time.roundTo(3);
                        case SqlTimeWithTimeZone time -> time.roundTo(3);
                        case SqlTimestamp timestamp -> timestamp.roundTo(3);
                        case SqlTimestampWithTimeZone timestamp -> timestamp.roundTo(3);
                        case null, default -> value;
                    };
                }
                if (value == null) {
                    generator.writeNull();
                }
                else {
                    generator.writeString(value.toString());
                }
            }
            generator.writeEndArray();
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Benchmarks.benchmark(BenchmarkJsonEncoding.class).run();
    }
}
