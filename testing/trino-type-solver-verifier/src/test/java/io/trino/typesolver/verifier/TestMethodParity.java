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
package io.trino.typesolver.verifier;

import io.trino.lib.TrinoPreset;
import io.trino.lib.TypeBridge;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeManager;
import io.trino.sql.analyzer.TypeDescriptorProvider;
import org.junit.jupiter.api.Test;
import org.weakref.solver.Expression;
import org.weakref.solver.FunctionResolver;
import org.weakref.solver.TypeLibrary;

import java.util.ArrayList;
import java.util.List;

import static io.trino.sql.analyzer.TypeDescriptorTranslator.parseTypeDescriptor;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Differential harness for method-call resolution: resolves the same instance ({@code receiver.m(args)})
 * and static ({@code Type::m(args)}) method calls through the curated solver preset ({@link TypeLibrary})
 * and through Trino's own {@code FunctionResolver}, and checks they pick the same overload and return
 * type. Instance methods present the receiver as the leading argument the way the analyzer does; static
 * methods treat the receiver type as a selector only.
 */
class TestMethodParity
{
    private final TestingFunctionResolution functions = new TestingFunctionResolution();
    private final TypeManager typeManager = functions.getPlannerContext().getTypeManager();
    private final TypeLibrary solver = TrinoPreset.library();

    private record MethodCall(String name, String receiver, List<String> arguments) {}

    private static final List<MethodCall> INSTANCE_CALLS = List.of(
            new MethodCall("length", "varchar(3)", List.of()),
            new MethodCall("reverse", "varchar(5)", List.of()),
            new MethodCall("substring", "varchar(10)", List.of("bigint")),
            new MethodCall("substring", "varchar(10)", List.of("integer", "integer")));

    private static final List<MethodCall> STATIC_CALLS = List.of(
            new MethodCall("from_utf8", "varchar", List.of("varbinary")),
            new MethodCall("from_utf8", "varchar", List.of("varbinary", "varchar(1)")));

    @Test
    void testInstanceMethodResolutionMatchesTrino()
    {
        for (MethodCall call : INSTANCE_CALLS) {
            Type receiverType = type(call.receiver());
            String receiverBase = receiverType.getTypeDescriptor().getBase();

            // The analyzer presents an instance method's parameters with the receiver (self) first
            List<Type> parameterTypes = new ArrayList<>();
            parameterTypes.add(receiverType);
            call.arguments().forEach(argument -> parameterTypes.add(type(argument)));

            String engineReturn = render(functions
                    .resolveInstanceMethod(receiverType.getTypeDescriptor(), call.name(), TypeDescriptorProvider.fromTypes(parameterTypes))
                    .signature().getReturnType());
            String solverReturn = solverReturn(solver.resolveInstanceMethod(call.name(), receiverBase, solverArguments(parameterTypes)));

            assertThat(solverReturn)
                    .as("%s.%s%s", call.receiver(), call.name(), call.arguments())
                    .isEqualTo(engineReturn);
        }
    }

    @Test
    void testStaticMethodResolutionMatchesTrino()
    {
        for (MethodCall call : STATIC_CALLS) {
            TypeDescriptor receiver = new TypeDescriptor(call.receiver());
            List<Type> parameterTypes = call.arguments().stream().map(this::type).toList();

            String engineReturn = render(functions
                    .resolveStaticMethod(receiver, call.name(), TypeDescriptorProvider.fromTypes(parameterTypes))
                    .signature().getReturnType());
            String solverReturn = solverReturn(solver.resolveStaticMethod(call.name(), receiver.getBase(), solverArguments(parameterTypes)));

            assertThat(solverReturn)
                    .as("%s::%s%s", call.receiver(), call.name(), call.arguments())
                    .isEqualTo(engineReturn);
        }
    }

    private Type type(String descriptor)
    {
        return typeManager.getType(parseTypeDescriptor(descriptor));
    }

    private static List<Expression> solverArguments(List<Type> parameterTypes)
    {
        return parameterTypes.stream().map(TypeBridge::toExpression).toList();
    }

    private static String render(Type type)
    {
        return TypeBridge.render(TypeBridge.toExpression(type));
    }

    private static String solverReturn(FunctionResolver.ResolutionOutcome outcome)
    {
        if (outcome instanceof FunctionResolver.Resolved resolved) {
            return TypeBridge.render(resolved.resolution().returnType());
        }
        throw new AssertionError("solver did not resolve the call: " + outcome);
    }
}
