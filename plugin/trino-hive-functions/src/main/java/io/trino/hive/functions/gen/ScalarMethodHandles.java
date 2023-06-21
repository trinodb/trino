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
package io.trino.hive.functions.gen;

import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.ParameterizedType;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.trino.spi.function.Signature;
import io.trino.spi.predicate.Primitives;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.Collections;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.bytecode.Access.*;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.newArray;
import static io.trino.hive.functions.gen.CompilerUtils.defineClass;
import static io.trino.hive.functions.gen.CompilerUtils.makeClassName;
import static java.lang.invoke.MethodHandles.lookup;

public final class ScalarMethodHandles
{
    private static final String CLASS_NAME = "HiveScalarFunction";
    private static final String METHOD_NAME = "callEvaluate";

    private ScalarMethodHandles() {}

    /**
     * Generates an unbound MethodHandle by {@code signature}.
     * <p>
     * For example, if {@code signature} describes a method like
     * <pre>
     * {@code
     * IntegerType func(
     *              IntegerType i,
     *              VarcharType j)
     * }
     * </pre>,
     * a method is generated as
     * <pre>
     * {@code
     * static Long callEvaluate(
     *              ScalarFunctionInvoker invoker,
     *              Long input_0,
     *              Slice input_1) {
     * return (Long) Invoker.evaluate(input_0, input_1);
     * }
     * }
     * </pre>,
     * and its MethodHandle returns.
     */
    public static MethodHandle generateUnbound(Signature signature, TypeManager typeManager)
    {
        Class<?> returnType = Primitives.wrap(typeManager.getType(signature.getReturnType()).getJavaType());
        List<TypeSignature> argumentTypes = signature.getArgumentTypes();
        List<Class<?>> argumentJavaTypes = argumentTypes.stream()
                .map(t -> typeManager.getType(t).getJavaType())
                .map(Primitives::wrap)
                .collect(toImmutableList());

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(CLASS_NAME),
                type(Object.class));

        // Step 1: Declare default constructor
        definition.declareDefaultConstructor(a(PRIVATE));

        // Step 2: Declare method
        Parameter[] declareParameters = new Parameter[argumentTypes.size() + 1];
        declareParameters[0] = arg("invoker", ScalarFunctionInvoker.class);
        for (int i = 0; i < argumentTypes.size(); i++) {
            declareParameters[i + 1] = arg("input_" + i, argumentJavaTypes.get(i));
        }
        MethodDefinition method = definition.declareMethod(
                a(PUBLIC, STATIC),
                METHOD_NAME,
                type(returnType),
                declareParameters);

        // Step 3: Implement method body
        BytecodeExpression[] evaluateInputs = new BytecodeExpression[argumentTypes.size()];
        for (int i = 0; i < argumentTypes.size(); i++) {
            evaluateInputs[i] = declareParameters[i + 1];
        }
        method.getBody().append(declareParameters[0].invoke("evaluate",
                        Object.class,
                        newArray(type(Object[].class), evaluateInputs))
                .cast(returnType)
                .ret());

        // Step 4: Generate class
        Class<?> generatedClass = defineClass(definition,
                Object.class,
                Collections.emptyMap(),
                ScalarMethodHandles.class.getClassLoader());

        // Step 5: Lookup MethodHandle
        Class<?>[] lookupClasses = new Class[argumentTypes.size() + 1];
        lookupClasses[0] = ScalarFunctionInvoker.class;
        for (int i = 0; i < argumentTypes.size(); i++) {
            lookupClasses[i + 1] = Primitives.wrap(typeManager.getType(argumentTypes.get(i)).getJavaType());
        }
        return lookup().unreflect(generatedClass.getName(), lookupClasses);
    }
}
