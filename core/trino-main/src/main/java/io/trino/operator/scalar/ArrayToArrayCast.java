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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.trino.metadata.FunctionBinding;
import io.trino.metadata.FunctionDependencies;
import io.trino.metadata.FunctionDependencyDeclaration;
import io.trino.metadata.FunctionInvoker;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.SqlOperator;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.gen.ArrayGeneratorUtils;
import io.trino.sql.gen.ArrayMapBytecodeExpression;
import io.trino.sql.gen.CachedInstanceBinder;
import io.trino.sql.gen.CallSiteBinder;

import java.lang.invoke.MethodHandle;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantBoolean;
import static io.trino.metadata.Signature.castableToTypeParameter;
import static io.trino.metadata.Signature.typeVariable;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.util.CompilerUtils.defineClass;
import static io.trino.util.CompilerUtils.makeClassName;
import static io.trino.util.Reflection.methodHandle;

public class ArrayToArrayCast
        extends SqlOperator
{
    public static final ArrayToArrayCast ARRAY_TO_ARRAY_CAST = new ArrayToArrayCast();

    private ArrayToArrayCast()
    {
        super(CAST,
                ImmutableList.of(castableToTypeParameter("F", new TypeSignature("T")), typeVariable("T")),
                ImmutableList.of(),
                arrayType(new TypeSignature("T")),
                ImmutableList.of(arrayType(new TypeSignature("F"))),
                false);
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies()
    {
        return FunctionDependencyDeclaration.builder()
                .addCastSignature(new TypeSignature("F"), new TypeSignature("T"))
                .build();
    }

    @Override
    public ScalarFunctionImplementation specialize(FunctionBinding functionBinding, FunctionDependencies functionDependencies)
    {
        checkArgument(functionBinding.getArity() == 1, "Expected arity to be 1");
        Type fromType = functionBinding.getTypeVariable("F");
        Type toType = functionBinding.getTypeVariable("T");

        FunctionMetadata castMetadata = functionDependencies.getCastMetadata(fromType, toType);
        Function<InvocationConvention, FunctionInvoker> castInvokerProvider = invocationConvention -> functionDependencies.getCastInvoker(fromType, toType, invocationConvention);
        Class<?> castOperatorClass = generateArrayCast(fromType, toType, castMetadata, castInvokerProvider);
        MethodHandle methodHandle = methodHandle(castOperatorClass, "castArray", ConnectorSession.class, Block.class);
        return new ChoicesScalarFunctionImplementation(
                functionBinding,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL),
                methodHandle);
    }

    private static Class<?> generateArrayCast(Type fromElementType, Type toElementType, FunctionMetadata castMetadata, Function<InvocationConvention, FunctionInvoker> castInvokerProvider)
    {
        CallSiteBinder binder = new CallSiteBinder();

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(Joiner.on("$").join("ArrayCast", fromElementType, toElementType)),
                type(Object.class));

        Parameter session = arg("session", ConnectorSession.class);
        Parameter value = arg("value", Block.class);

        MethodDefinition method = definition.declareMethod(
                a(PUBLIC, STATIC),
                "castArray",
                type(Block.class),
                session,
                value);

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        Variable wasNull = scope.declareVariable(boolean.class, "wasNull");
        body.append(wasNull.set(constantBoolean(false)));

        // cast map elements
        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(definition, binder);
        ArrayMapBytecodeExpression newArray = ArrayGeneratorUtils.map(scope, cachedInstanceBinder, fromElementType, toElementType, value, castMetadata, castInvokerProvider);

        // return the block
        body.append(newArray.ret());

        MethodDefinition constructorDefinition = definition.declareConstructor(a(PUBLIC));
        BytecodeBlock constructorBody = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();
        constructorBody.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);
        cachedInstanceBinder.generateInitializations(thisVariable, constructorBody);
        constructorBody.ret();

        return defineClass(definition, Object.class, binder.getBindings(), ArrayToArrayCast.class.getClassLoader());
    }
}
