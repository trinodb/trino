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
package io.prestosql.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.airlift.bytecode.instruction.LabelNode;
import io.prestosql.metadata.FunctionBinding;
import io.prestosql.metadata.FunctionDependencies;
import io.prestosql.metadata.FunctionDependencyDeclaration;
import io.prestosql.metadata.FunctionDependencyDeclaration.FunctionDependencyDeclarationBuilder;
import io.prestosql.metadata.FunctionInvoker;
import io.prestosql.metadata.FunctionMetadata;
import io.prestosql.metadata.SqlOperator;
import io.prestosql.spi.function.InvocationConvention;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.gen.CachedInstanceBinder;
import io.prestosql.sql.gen.CallSiteBinder;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.prestosql.metadata.Signature.withVariadicBound;
import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.prestosql.spi.function.OperatorType.INDETERMINATE;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.sql.gen.InvokeFunctionBytecodeExpression.invokeFunction;
import static io.prestosql.sql.gen.SqlTypeBytecodeExpression.constantType;
import static io.prestosql.type.UnknownType.UNKNOWN;
import static io.prestosql.util.CompilerUtils.defineClass;
import static io.prestosql.util.CompilerUtils.makeClassName;
import static io.prestosql.util.Reflection.methodHandle;

public class RowIndeterminateOperator
        extends SqlOperator
{
    public static final RowIndeterminateOperator ROW_INDETERMINATE = new RowIndeterminateOperator();

    private RowIndeterminateOperator()
    {
        super(INDETERMINATE,
                ImmutableList.of(withVariadicBound("T", "row")),
                ImmutableList.of(),
                BOOLEAN.getTypeSignature(),
                ImmutableList.of(new TypeSignature("T")),
                false);
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies(FunctionBinding functionBinding)
    {
        RowType rowType = (RowType) functionBinding.getTypeVariable("T");
        FunctionDependencyDeclarationBuilder builder = FunctionDependencyDeclaration.builder();
        rowType.getTypeParameters()
                .forEach(type -> builder.addOperator(INDETERMINATE, ImmutableList.of(type)));
        return builder.build();
    }

    @Override
    public ScalarFunctionImplementation specialize(FunctionBinding functionBinding, FunctionDependencies functionDependencies)
    {
        checkArgument(functionBinding.getArity() == 1, "Expected arity to be 1");
        Type type = functionBinding.getTypeVariable("T");
        Class<?> indeterminateOperatorClass = generateIndeterminate(type, functionDependencies);
        MethodHandle indeterminateMethod = methodHandle(indeterminateOperatorClass, "indeterminate", type.getJavaType(), boolean.class);
        return new ScalarFunctionImplementation(
                FAIL_ON_NULL,
                ImmutableList.of(NULL_FLAG),
                indeterminateMethod);
    }

    private static Class<?> generateIndeterminate(Type type, FunctionDependencies functionDependencies)
    {
        CallSiteBinder binder = new CallSiteBinder();

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("RowIndeterminateOperator"),
                type(Object.class));

        Parameter value = arg("value", type.getJavaType());
        Parameter isNull = arg("isNull", boolean.class);

        MethodDefinition method = definition.declareMethod(
                a(PUBLIC, STATIC),
                "indeterminate",
                type(boolean.class),
                value,
                isNull);

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        Variable wasNull = scope.declareVariable(boolean.class, "wasNull");
        body.append(wasNull.set(constantFalse()));

        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(definition, binder);

        LabelNode end = new LabelNode("end");
        List<Type> fieldTypes = type.getTypeParameters();
        boolean hasUnknownFields = fieldTypes.stream().anyMatch(fieldType -> fieldType.equals(UNKNOWN));

        body.append(new IfStatement("if isNull is true...")
                .condition(isNull)
                .ifTrue(new BytecodeBlock()
                        .push(true)
                        .gotoLabel(end)));

        if (hasUnknownFields) {
            // if the current field type is UNKNOWN which means this field is null, directly return true
            body.push(true)
                    .gotoLabel(end);
        }
        else {
            for (int i = 0; i < fieldTypes.size(); i++) {
                IfStatement ifNullField = new IfStatement("if the field is null...");
                ifNullField.condition(value.invoke("isNull", boolean.class, constantInt(i)))
                        .ifTrue(new BytecodeBlock()
                                .push(true)
                                .gotoLabel(end));

                Type fieldType = fieldTypes.get(i);
                FunctionMetadata functionMetadata = functionDependencies.getOperatorMetadata(INDETERMINATE, ImmutableList.of(fieldType));
                Function<InvocationConvention, FunctionInvoker> functionInvokerProvider = invocationConvention ->
                        functionDependencies.getOperatorInvoker(INDETERMINATE, ImmutableList.of(fieldType), Optional.of(invocationConvention));
                BytecodeExpression element = constantType(binder, fieldType).getValue(value, constantInt(i));

                ifNullField.ifFalse(new IfStatement("if the field is not null but indeterminate...")
                        .condition(invokeFunction(scope, cachedInstanceBinder, BOOLEAN, functionMetadata, functionInvokerProvider, element))
                        .ifTrue(new BytecodeBlock()
                                .push(true)
                                .gotoLabel(end)));

                body.append(ifNullField);
            }
            // if none of the fields is indeterminate, then push false
            body.push(false);
        }

        body.visitLabel(end)
                .retBoolean();

        // create constructor
        MethodDefinition constructorDefinition = definition.declareConstructor(a(PUBLIC));
        BytecodeBlock constructorBody = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();
        constructorBody.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);
        cachedInstanceBinder.generateInitializations(thisVariable, constructorBody);
        constructorBody.ret();

        return defineClass(definition, Object.class, binder.getBindings(), RowIndeterminateOperator.class.getClassLoader());
    }
}
