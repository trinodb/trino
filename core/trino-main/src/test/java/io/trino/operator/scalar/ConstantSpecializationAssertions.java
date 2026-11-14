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
package io.trino.operator.scalar;

import io.airlift.slice.Slice;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.project.PageProjection;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionSpecializationContext;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that a function with a constant specialization returns the same result when the
 * constant arguments are literals as when they are runtime values, and that a specialization is selected.
 */
public final class ConstantSpecializationAssertions
{
    private final TestingFunctionResolution functionResolution;

    public ConstantSpecializationAssertions(TestingFunctionResolution functionResolution)
    {
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
    }

    public void assertParity(String functionName, Set<Integer> constantPositions, Typed... arguments)
    {
        List<Type> types = List.of(arguments).stream().map(Typed::type).toList();
        ResolvedFunction function = functionResolution.resolveFunction(functionName, fromTypes(types));

        Object runtimeResult = evaluate(function, arguments, Set.of());
        Object constantResult = evaluate(function, arguments, constantPositions);
        assertThat(constantResult)
                .describedAs("%s(%s) with constant positions %s", functionName, types, constantPositions)
                .isEqualTo(runtimeResult);

        assertThat(selectsSpecialization(function, arguments, constantPositions))
                .describedAs("%s(%s) uses a constant specialization for positions %s", functionName, types, constantPositions)
                .isTrue();
    }

    private Object evaluate(ResolvedFunction function, Typed[] arguments, Set<Integer> constantPositions)
    {
        List<Expression> expressions = new ArrayList<>();
        List<Block> columns = new ArrayList<>();
        Map<Symbol, Integer> layout = new HashMap<>();
        for (int index = 0; index < arguments.length; index++) {
            if (constantPositions.contains(index)) {
                expressions.add(new Constant(arguments[index].type(), arguments[index].value()));
            }
            else {
                String name = "c" + index;
                layout.put(new Symbol(arguments[index].type(), name), columns.size());
                expressions.add(new Reference(arguments[index].type(), name));
                columns.add(block(arguments[index]));
            }
        }

        PageProjection projection = functionResolution.getPageFunctionCompiler()
                .compileProjection(new Call(function, expressions), layout, SQL_STANDARD, Optional.empty())
                .get();
        Page page = columns.isEmpty() ? new Page(1) : new Page(columns.toArray(Block[]::new));
        SourcePage inputPage = projection.getInputChannels().getInputChannels(SourcePage.create(page));
        Block result = projection.project(SESSION, inputPage, SelectedPositions.positionsRange(0, 1));
        return function.signature().getReturnType().getObjectValue(result, 0);
    }

    private boolean selectsSpecialization(ResolvedFunction function, Typed[] arguments, Set<Integer> constantPositions)
    {
        List<Optional<io.trino.spi.expression.Constant>> constants = IntStream.range(0, arguments.length)
                .mapToObj(index -> constantPositions.contains(index)
                        ? Optional.of(new io.trino.spi.expression.Constant(arguments[index].value(), arguments[index].type()))
                        : Optional.<io.trino.spi.expression.Constant>empty())
                .toList();
        InvocationConvention convention = new InvocationConvention(
                nCopies(arguments.length, NEVER_NULL),
                function.functionNullability().isReturnNullable() ? NULLABLE_RETURN : FAIL_ON_NULL,
                true,
                true);
        return functionResolution.getPlannerContext().getFunctionManager()
                .getConstantSpecializedScalarFunctionImplementation(function, new ScalarFunctionSpecializationContext(convention, constants))
                .isPresent();
    }

    private static Block block(Typed typed)
    {
        Type type = typed.type();
        BlockBuilder builder = type.createBlockBuilder(null, 1);
        Class<?> javaType = type.getJavaType();
        if (javaType == long.class) {
            type.writeLong(builder, (Long) typed.value());
        }
        else if (javaType == Slice.class) {
            type.writeSlice(builder, (Slice) typed.value());
        }
        else {
            type.writeObject(builder, typed.value());
        }
        return builder.build();
    }

    public static Typed varchar(String value)
    {
        return new Typed(createVarcharType(value.length()), utf8Slice(value));
    }

    public record Typed(Type type, Object value) {}
}
