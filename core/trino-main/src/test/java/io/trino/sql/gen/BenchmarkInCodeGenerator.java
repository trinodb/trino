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
package io.trino.sql.gen;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.jmh.Benchmarks;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;
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
import org.openjdk.jmh.runner.RunnerException;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.sql.relational.SpecialForm.Form.IN;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(2)
@Warmup(iterations = 6, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 6, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(AverageTime)
@SuppressWarnings({"FieldMayBeFinal", "FieldCanBeLocal"})
public class BenchmarkInCodeGenerator
{
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"1", "5", "10", "25", "50", "75", "100", "150", "200", "250", "300", "350", "400", "450", "500", "750", "1000", "10000"})
        private int inListCount = 1;

        @Param({StandardTypes.BIGINT, StandardTypes.DOUBLE, StandardTypes.VARCHAR})
        private String type = StandardTypes.BIGINT;

        @Param({"0.0", "0.05", "0.50", "1.0"})
        private double hitRate;

        private Page inputPage;
        private PageProcessor processor;
        private Type trinoType;

        @Setup
        public void setup()
        {
            Random random = new Random(0);
            List<RowExpression> arguments = new ArrayList<>(1 + inListCount);

            switch (type) {
                case StandardTypes.BIGINT:
                    trinoType = BIGINT;
                    break;
                case StandardTypes.DOUBLE:
                    trinoType = DOUBLE;
                    break;
                case StandardTypes.VARCHAR:
                    trinoType = VARCHAR;
                    break;
                default:
                    throw new IllegalStateException();
            }

            List<Object> inList = new ArrayList<>();
            arguments.add(field(0, trinoType));
            switch (type) {
                case StandardTypes.BIGINT:
                    for (int i = 1; i <= inListCount; i++) {
                        int value = random.nextInt();
                        inList.add((long) value);
                        arguments.add(constant((long) value, BIGINT));
                    }
                    break;
                case StandardTypes.DOUBLE:
                    for (int i = 1; i <= inListCount; i++) {
                        double value = random.nextDouble();
                        inList.add(value);
                        arguments.add(constant(value, DOUBLE));
                    }
                    break;
                case StandardTypes.VARCHAR:
                    for (int i = 1; i <= inListCount; i++) {
                        Slice value = Slices.utf8Slice(Long.toString(random.nextLong()));
                        inList.add(value);
                        arguments.add(constant(value, VARCHAR));
                    }
                    break;
                default:
                    throw new IllegalStateException();
            }

            RowExpression project = field(0, trinoType);

            PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(trinoType));
            for (int i = 0; i < 10_000; i++) {
                pageBuilder.declarePosition();

                if (random.nextDouble() <= hitRate) {
                    // pick one of the values from in list as value written to page
                    switch (type) {
                        case StandardTypes.BIGINT:
                            BIGINT.writeLong(pageBuilder.getBlockBuilder(0), (long) inList.get(random.nextInt(inList.size())));
                            break;
                        case StandardTypes.DOUBLE:
                            DOUBLE.writeDouble(pageBuilder.getBlockBuilder(0), (double) inList.get(random.nextInt(inList.size())));
                            break;
                        case StandardTypes.VARCHAR:
                            VARCHAR.writeSlice(pageBuilder.getBlockBuilder(0), (Slice) inList.get(random.nextInt(inList.size())));
                            break;
                    }
                }
                else {
                    // use random value; universum is wide so we can safely assume it will not be on of values in inList
                    switch (type) {
                        case StandardTypes.BIGINT:
                            BIGINT.writeLong(pageBuilder.getBlockBuilder(0), random.nextInt());
                            break;
                        case StandardTypes.DOUBLE:
                            DOUBLE.writeDouble(pageBuilder.getBlockBuilder(0), random.nextDouble());
                            break;
                        case StandardTypes.VARCHAR:
                            VARCHAR.writeSlice(pageBuilder.getBlockBuilder(0), Slices.utf8Slice(Long.toString(random.nextLong())));
                            break;
                    }
                }
            }
            inputPage = pageBuilder.build();

            Metadata metadata = createTestMetadataManager();

            List<ResolvedFunction> functionalDependencies = ImmutableList.of(
                    metadata.resolveOperator(OperatorType.EQUAL, ImmutableList.of(trinoType, trinoType)),
                    metadata.resolveOperator(OperatorType.HASH_CODE, ImmutableList.of(trinoType)),
                    metadata.resolveOperator(OperatorType.INDETERMINATE, ImmutableList.of(trinoType)));
            RowExpression filter = new SpecialForm(IN, BOOLEAN, arguments, functionalDependencies);

            processor = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0)).compilePageProcessor(Optional.of(filter), ImmutableList.of(project)).get();
        }
    }

    @Benchmark
    public List<Optional<Page>> benchmark(BenchmarkData data)
    {
        return ImmutableList.copyOf(
                data.processor.process(
                        SESSION,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        data.inputPage));
    }

    @Test
    public void testBenchmarkInCodeGenerator()
    {
        BenchmarkData benchmarkData = new BenchmarkData();
        benchmarkData.setup();
        benchmark(benchmarkData);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Benchmarks.benchmark(BenchmarkInCodeGenerator.class).run();
    }
}
