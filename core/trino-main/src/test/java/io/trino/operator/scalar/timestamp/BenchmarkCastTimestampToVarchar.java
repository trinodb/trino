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
package io.trino.operator.scalar.timestamp;

import com.google.common.collect.ImmutableList;
import io.trino.execution.buffer.BenchmarkDataGenerator;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.project.PageProcessor;
import io.trino.operator.scalar.timestamptz.TimestampWithTimeZoneToTimestampWithTimeZoneCast;
import io.trino.operator.scalar.timetz.TimeWithTimeZoneToTimeWithTimeZoneCast;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.RowExpression;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.List;
import java.util.Optional;
import java.util.Random;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.openjdk.jmh.annotations.Mode.Throughput;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(SECONDS)
@BenchmarkMode(Throughput)
@Fork(1)
@Warmup(iterations = 5, time = 1, timeUnit = SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = SECONDS)
public class BenchmarkCastTimestampToVarchar
{
    private static final int POSITIONS_PER_PAGE = 1024;

    @Benchmark
    public List<Optional<Page>> benchmarkCastToVarchar(BenchmarkData data)
    {
        return ImmutableList.copyOf(data.pageProcessor.process(SESSION, data.yieldSignal, data.localMemoryContext, data.page));
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"TIME", "TIME_WITH_TIME_ZONE", "TIMESTAMP", "TIMESTAMP_WITH_TIME_ZONE"})
        private String type;
        @Param({"0", "3", "12"})
        private int precision;
        private Random random;

        private DriverYieldSignal yieldSignal;
        private LocalMemoryContext localMemoryContext;
        private PageProcessor pageProcessor;
        private Page page;

        @Setup
        public void setup()
        {
            random = new Random(0);
            yieldSignal = new DriverYieldSignal();
            localMemoryContext = newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName());

            Type sourceType;
            switch (type) {
                case "TIME":
                    TimeType timeType = TimeType.createTimeType(precision);
                    sourceType = timeType;
                    page = createTimePage(random, timeType);
                    break;
                case "TIME_WITH_TIME_ZONE":
                    TimeWithTimeZoneType timeTzType = TimeWithTimeZoneType.createTimeWithTimeZoneType(precision);
                    sourceType = timeTzType;
                    page = createTimeTzPage(random, timeTzType);
                    break;
                case "TIMESTAMP":
                    TimestampType timestampType = TimestampType.createTimestampType(precision);
                    sourceType = timestampType;
                    page = createTimestampPage(random, timestampType);
                    break;
                case "TIMESTAMP_WITH_TIME_ZONE":
                    TimestampWithTimeZoneType timestampTzType = TimestampWithTimeZoneType.createTimestampWithTimeZoneType(precision);
                    sourceType = timestampTzType;
                    page = createTimestampTzPage(random, timestampTzType);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported type: " + type);
            }

            TestingFunctionResolution functionResolution = new TestingFunctionResolution();
            List<RowExpression> timestampProjections = ImmutableList.of(new CallExpression(
                    functionResolution.getCoercion(sourceType, VarcharType.createUnboundedVarcharType()),
                    ImmutableList.of(field(0, sourceType))));
            pageProcessor = functionResolution.getExpressionCompiler()
                    .compilePageProcessor(Optional.empty(), timestampProjections)
                    .get();
        }

        private static Page createTimePage(Random random, TimeType timeType)
        {
            BlockBuilder builder = timeType.createBlockBuilder(null, POSITIONS_PER_PAGE);
            for (int i = 0; i < POSITIONS_PER_PAGE; i++) {
                timeType.writeLong(builder, SqlTime.newInstance(12, random.nextLong(PICOSECONDS_PER_DAY)).roundTo(timeType.getPrecision()).getPicos());
            }
            return new Page(builder.build());
        }

        private static Page createTimeTzPage(Random random, TimeWithTimeZoneType timeTzType)
        {
            BlockBuilder builder = timeTzType.createBlockBuilder(null, POSITIONS_PER_PAGE);
            for (int i = 0; i < POSITIONS_PER_PAGE; i++) {
                LongTimeWithTimeZone value = new LongTimeWithTimeZone(random.nextLong(PICOSECONDS_PER_DAY), 0);
                if (timeTzType.isShort()) {
                    timeTzType.writeLong(builder, TimeWithTimeZoneToTimeWithTimeZoneCast.longToShort(timeTzType.getPrecision(), value));
                }
                else {
                    timeTzType.writeObject(builder, TimeWithTimeZoneToTimeWithTimeZoneCast.longToLong(timeTzType.getPrecision(), value));
                }
            }
            return new Page(builder.build());
        }

        private static Page createTimestampPage(Random random, TimestampType timestampType)
        {
            BlockBuilder builder = timestampType.createBlockBuilder(null, POSITIONS_PER_PAGE);
            for (int i = 0; i < POSITIONS_PER_PAGE; i++) {
                LongTimestamp value = BenchmarkDataGenerator.randomTimestamp(random);
                if (timestampType.isShort()) {
                    timestampType.writeLong(builder, TimestampToTimestampCast.longToShort(timestampType.getPrecision(), value));
                }
                else {
                    timestampType.writeObject(builder, TimestampToTimestampCast.longToLong(timestampType.getPrecision(), value));
                }
            }
            return new Page(builder.build());
        }

        private static Page createTimestampTzPage(Random random, TimestampWithTimeZoneType timestampTzType)
        {
            BlockBuilder builder = timestampTzType.createBlockBuilder(null, POSITIONS_PER_PAGE);
            for (int i = 0; i < POSITIONS_PER_PAGE; i++) {
                long epochMillis = random.nextLong(1L << 11); // must stay within bounds of what short timestamps with time zones can support
                int picosFraction = random.nextInt(PICOSECONDS_PER_MILLISECOND);
                LongTimestampWithTimeZone value = LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, picosFraction, UTC_KEY);
                if (timestampTzType.isShort()) {
                    timestampTzType.writeLong(builder, TimestampWithTimeZoneToTimestampWithTimeZoneCast.longToShort(timestampTzType.getPrecision(), value));
                }
                else {
                    timestampTzType.writeObject(builder, TimestampWithTimeZoneToTimestampWithTimeZoneCast.longToLong(timestampTzType.getPrecision(), value));
                }
            }
            return new Page(builder.build());
        }
    }
}
