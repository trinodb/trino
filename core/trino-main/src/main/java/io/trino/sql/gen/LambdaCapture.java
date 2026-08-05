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
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.expression.BytecodeExpression;

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.LambdaConversionException;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantClassDataAt;
import static io.trino.util.CompilerUtils.defineHiddenClass;
import static io.trino.util.CompilerUtils.makeClassName;
import static java.lang.invoke.MethodType.methodType;

public final class LambdaCapture
{
    public static final Method LAMBDA_CAPTURE_METHOD;

    static {
        try {
            LAMBDA_CAPTURE_METHOD = LambdaCapture.class.getMethod("lambdaCapture", MethodHandles.Lookup.class, String.class, MethodType.class, MethodType.class, String.class, MethodType.class, MethodType.class);
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private LambdaCapture() {}

    public static CallSite lambdaCapture(
            MethodHandles.Lookup callerLookup,
            String name,
            MethodType type,
            MethodType samMethodType,
            String implMethodName,
            MethodType implMethodType,
            MethodType instantiatedMethodType)
    {
        try {
            // The implementation method is identified by name and type instead of a constant
            // method handle: a hidden class cannot resolve a constant pool entry that names
            // itself, but the full-privilege caller lookup can always find its own methods.
            Class<?> callerClass = callerLookup.lookupClass();
            MethodHandle implMethod = callerLookup.findVirtual(callerClass, implMethodName, implMethodType);

            if (callerClass.isHidden()) {
                // LambdaMetafactory cannot name a hidden implementation class in the proxy
                // it spins, so generate the proxy ourselves around the method handle
                return generateLambdaProxy(callerLookup, name, type, samMethodType, implMethod);
            }

            // The receiver is captured as Object in the invoked type. The metafactory
            // requires the precise receiver type, so restore it and adapt the resulting
            // factory back to the call site type.
            MethodType factoryType = type.changeParameterType(0, callerClass);
            // delegate to metafactory, we may choose to generate code ourselves in the future.
            CallSite callSite = LambdaMetafactory.metafactory(
                    callerLookup,
                    name,
                    factoryType,
                    samMethodType,
                    implMethod,
                    instantiatedMethodType);
            return new ConstantCallSite(callSite.getTarget().asType(type));
        }
        catch (ReflectiveOperationException | LambdaConversionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Generates a hidden class implementing the functional interface. Captured values are
     * stored in fields, and the interface method invokes the adapted implementation handle,
     * which is attached as class data and loaded as a dynamic constant, so the JIT can
     * inline through it just like a metafactory proxy.
     */
    private static CallSite generateLambdaProxy(
            MethodHandles.Lookup callerLookup,
            String name,
            MethodType type,
            MethodType samMethodType,
            MethodHandle implMethod)
            throws ReflectiveOperationException
    {
        Class<?> lambdaInterface = type.returnType();
        List<Class<?>> captureTypes = type.parameterList();

        // (captures..., interface method arguments...) -> interface method return type
        MethodType adaptedType = methodType(samMethodType.returnType(), captureTypes)
                .appendParameterTypes(samMethodType.parameterList());
        MethodHandle adaptedImplMethod = implMethod.asType(adaptedType);

        ClassDefinition proxy = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(lambdaInterface.getSimpleName() + "Lambda"),
                type(Object.class),
                type(lambdaInterface));

        List<FieldDefinition> captureFields = new ArrayList<>();
        List<Parameter> constructorParameters = new ArrayList<>();
        for (int i = 0; i < captureTypes.size(); i++) {
            captureFields.add(proxy.declareField(a(PRIVATE, FINAL), "capture_" + i, captureTypes.get(i)));
            constructorParameters.add(arg("capture_" + i, captureTypes.get(i)));
        }

        MethodDefinition constructor = proxy.declareConstructor(a(PUBLIC), constructorParameters);
        BytecodeBlock constructorBody = constructor.getBody()
                .append(constructor.getThis())
                .invokeConstructor(Object.class);
        for (int i = 0; i < captureTypes.size(); i++) {
            constructorBody.append(constructor.getThis().setField(captureFields.get(i), constructorParameters.get(i)));
        }
        constructorBody.ret();

        List<Parameter> methodParameters = new ArrayList<>();
        for (int i = 0; i < samMethodType.parameterCount(); i++) {
            methodParameters.add(arg("argument_" + i, samMethodType.parameterType(i)));
        }
        MethodDefinition method = proxy.declareMethod(a(PUBLIC), name, type(samMethodType.returnType()), methodParameters);

        ImmutableList.Builder<BytecodeExpression> invocationArguments = ImmutableList.builder();
        for (FieldDefinition captureField : captureFields) {
            invocationArguments.add(method.getThis().getField(captureField));
        }
        invocationArguments.addAll(methodParameters);

        method.getBody()
                .append(constantClassDataAt(0, MethodHandle.class)
                        .invoke("invokeExact", samMethodType.returnType(), invocationArguments.build())
                        .ret());

        Class<?> proxyClass = defineHiddenClass(proxy, lambdaInterface, ImmutableList.of(adaptedImplMethod));
        MethodHandle constructorHandle = callerLookup.findConstructor(proxyClass, methodType(void.class, captureTypes));
        return new ConstantCallSite(constructorHandle.asType(type));
    }
}
