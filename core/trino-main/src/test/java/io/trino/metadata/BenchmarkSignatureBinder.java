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
package io.trino.metadata;

import io.trino.spi.function.Signature;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.FunctionType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeTemplates;
import io.trino.sql.analyzer.TypeDescriptorProvider;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TypeTemplates.typeVariable;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.type.TypeResolutionPolicy.SQL_STANDARD;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 2, jvmArgsAppend = "-Xmx2g")
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkSignatureBinder
{
    @Param({"scalar", "array20", "array40", "row100", "row1000", "distinct1000", "arity100", "arity1000", "lambda", "unsatisfied"})
    public String shape = "row1000";

    @Param({"false", "true"})
    public boolean solver = true;

    private Signature signature;
    private List<TypeDescriptorProvider> arguments;

    @Setup
    public void setup()
    {
        Signature.Builder builder = Signature.builder().typeVariable("T").returnType(typeVariable("T"));
        Type left = INTEGER;
        Type right = BIGINT;
        if (shape.startsWith("array")) {
            int depth = Integer.parseInt(shape.substring("array".length()));
            for (int index = 0; index < depth; index++) {
                left = new ArrayType(left);
                right = new ArrayType(right);
            }
        }
        if (shape.startsWith("row") || shape.equals("unsatisfied")) {
            int width = shape.equals("unsatisfied") ? 1000 : Integer.parseInt(shape.substring("row".length()));
            left = RowType.anonymous(nCopies(width, INTEGER));
            List<Type> fields = new ArrayList<>(nCopies(width, BIGINT));
            if (shape.equals("unsatisfied")) {
                fields.set(width - 1, BOOLEAN);
            }
            right = RowType.anonymous(fields);
        }
        if (shape.equals("distinct1000")) {
            List<Type> leftFields = new ArrayList<>();
            List<Type> rightFields = new ArrayList<>();
            for (int index = 0; index < 1000; index++) {
                leftFields.add(createVarcharType(index + 1));
                rightFields.add(createVarcharType(index + 2));
            }
            left = RowType.anonymous(leftFields);
            right = RowType.anonymous(rightFields);
        }
        if (shape.startsWith("arity")) {
            int arity = Integer.parseInt(shape.substring("arity".length()));
            builder.argumentType(typeVariable("T")).variableArity();
            List<Type> types = new ArrayList<>(nCopies(arity, INTEGER));
            types.set(arity - 1, BIGINT);
            arguments = fromTypes(types);
        }
        else if (shape.equals("lambda")) {
            builder.typeVariable("U")
                    .argumentType(typeVariable("T"))
                    .argumentType(TypeTemplates.functionType(typeVariable("T"), typeVariable("U")))
                    .returnType(typeVariable("U"));
            arguments = List.of(
                    new TypeDescriptorProvider(INTEGER.getTypeDescriptor()),
                    new TypeDescriptorProvider(inputs -> new FunctionType(inputs, BIGINT).getTypeDescriptor()));
        }
        else {
            builder.argumentType(typeVariable("T")).argumentType(typeVariable("T"));
            arguments = fromTypes(List.of(left, right));
        }
        signature = builder.build();
    }

    @Benchmark
    public Optional<SignatureBinder.GroundSignature> bind()
    {
        if (solver) {
            return new SolverSignatureBinder(PLANNER_CONTEXT.getMetadata(), PLANNER_CONTEXT.getTypeManager(), signature, true, SQL_STANDARD).bind(arguments);
        }
        return new SignatureBinder(PLANNER_CONTEXT.getMetadata(), PLANNER_CONTEXT.getTypeManager(), signature, true, SQL_STANDARD).bind(arguments);
    }

    @Test
    public void testCases()
    {
        for (String input : List.of("scalar", "array20", "array40", "row100", "row1000", "distinct1000", "arity100", "arity1000", "lambda", "unsatisfied")) {
            shape = input;
            setup();
            solver = false;
            Optional<SignatureBinder.GroundSignature> expected = bind();
            assertThat(expected.isPresent()).isEqualTo(!input.equals("unsatisfied"));
            solver = true;
            assertThat(bind()).isEqualTo(expected);
        }
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkSignatureBinder.class).run();
    }
}
