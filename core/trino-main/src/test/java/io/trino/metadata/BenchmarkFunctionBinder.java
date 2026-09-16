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

import io.trino.metadata.FunctionBinder.CatalogFunctionBinding;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.analyzer.TypeDescriptorProvider;
import io.trino.type.TypeResolutionPolicy;
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

import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.trino.connector.system.GlobalSystemConnector.CATALOG_HANDLE;
import static io.trino.metadata.GlobalFunctionCatalog.BUILTIN_SCHEMA;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TypeTemplates.typeVariable;
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
public class BenchmarkFunctionBinder
{
    @Param({"exact", "coercing", "generic_row"})
    public String shape = "coercing";

    @Param({"false", "true"})
    public boolean solver = true;

    private FunctionBinder binder;
    private List<CatalogFunctionMetadata> candidates;
    private List<TypeDescriptorProvider> arguments;

    @Setup
    public void setup()
    {
        binder = new FunctionBinder(PLANNER_CONTEXT.getMetadata(), PLANNER_CONTEXT.getTypeManager());
        if (shape.equals("generic_row")) {
            candidates = List.of(
                    candidate(Signature.builder().argumentType(BOOLEAN).argumentType(BOOLEAN).returnType(BOOLEAN).build()),
                    candidate(Signature.builder().typeVariable("T").argumentType(typeVariable("T")).argumentType(typeVariable("T")).returnType(typeVariable("T")).build()));
            arguments = fromTypes(List.of(RowType.anonymous(nCopies(1000, INTEGER)), RowType.anonymous(nCopies(1000, BIGINT))));
        }
        else {
            candidates = List.of(scalar(BIGINT), scalar(DOUBLE), scalar(BOOLEAN));
            arguments = fromTypes(List.of(shape.equals("exact") ? BIGINT : INTEGER));
        }
    }

    private static CatalogFunctionMetadata scalar(Type type)
    {
        return candidate(Signature.builder().argumentType(type).returnType(type).build());
    }

    private static CatalogFunctionMetadata candidate(Signature signature)
    {
        return new CatalogFunctionMetadata(CATALOG_HANDLE, BUILTIN_SCHEMA, FunctionMetadata.scalarBuilder("benchmark")
                .signature(signature)
                .description("")
                .build());
    }

    @Benchmark
    public CatalogFunctionBinding bind()
    {
        return binder.bindFunction(new TypeResolutionPolicy(SQL_STANDARD.charVarcharCoercion(), !solver), arguments, candidates, "benchmark");
    }

    @Test
    public void testCases()
    {
        for (String input : List.of("exact", "coercing", "generic_row")) {
            shape = input;
            setup();
            solver = false;
            CatalogFunctionBinding expected = bind();
            solver = true;
            CatalogFunctionBinding actual = bind();
            assertThat(actual.functionBinding()).isEqualTo(expected.functionBinding());
            assertThat(actual.boundFunctionMetadata().getSignature()).isEqualTo(expected.boundFunctionMetadata().getSignature());
        }
    }
}
