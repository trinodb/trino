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

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Primitives;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.ParameterizedType;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.trino.metadata.FunctionManager;
import io.trino.spi.connector.ConnectorSession;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.ConstantExpression;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.LambdaDefinitionExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.RowExpressionVisitor;
import io.trino.sql.relational.SpecialForm;
import io.trino.sql.relational.VariableReferenceExpression;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaConversionException;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeDynamic;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static io.trino.spi.StandardErrorCode.COMPILER_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.sql.gen.BytecodeUtils.boxPrimitiveIfNecessary;
import static io.trino.sql.gen.BytecodeUtils.unboxPrimitiveIfNecessary;
import static io.trino.sql.gen.LambdaCapture.LAMBDA_CAPTURE_METHOD;
import static io.trino.sql.gen.LambdaExpressionExtractor.extractLambdaExpressions;
import static io.trino.util.Failures.checkCondition;
import static java.lang.invoke.MethodHandles.explicitCastArguments;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodHandles.privateLookupIn;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Objects.requireNonNull;
import static org.objectweb.asm.Type.getMethodType;
import static org.objectweb.asm.Type.getType;

public final class LambdaBytecodeGenerator
{
    private LambdaBytecodeGenerator() {}

    public static Map<LambdaDefinitionExpression, CompiledLambda> generateMethodsForLambda(
            RowExpression expression,
            CachedInstanceBinder callerCachedInstanceBinder,
            FunctionManager functionManager)
    {
        if (extractLambdaExpressions(expression).isEmpty()) {
            return ImmutableMap.of();
        }

        // Lambda functions are generated into a new non-hidden class, because lambda metafactory
        // currently requires direct method handles on non-hidden classes
        ClassBuilder classBuilder = ClassBuilder.createStandardClass(
                lookup(),
                a(PUBLIC, FINAL),
                "LambdaMethods",
                type(Object.class));

        // Private method to fetch a full access Lookup for this generated class
        // This is required because this class is in a separate classloader and thus a different unnamed module
        // and there does not seem any other way to get a full access method handle across class loaders
        classBuilder.declareMethod(a(PRIVATE, STATIC), "lookup", type(Lookup.class))
                .getBody()
                .append(invokeStatic(MethodHandles.class, "lookup", Lookup.class).ret());

        // generate the lambda methods inside the new class
        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classBuilder);
        Map<LambdaDefinitionExpression, CompiledLambda> generatedMethods = generateMethodsForLambdaInternal(
                classBuilder,
                cachedInstanceBinder,
                expression,
                functionManager);

        // create constructor which initializes the fields inside the cached instance binder
        MethodDefinition constructorDefinition = classBuilder.declareConstructor(a(PUBLIC));
        BytecodeBlock constructorBody = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();
        constructorBody.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);
        cachedInstanceBinder.generateInitializations(thisVariable, constructorBody);
        constructorBody.ret();

        // define the new class
        Class<?> lambdaClass = classBuilder.defineClass();

        // create a cached instance of the new class in the callers cached instance binder
        MethodHandle constructor;
        try {
            constructor = lookup().findConstructor(lambdaClass, methodType(void.class));
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
        // the field must be of type Object, because the new class is not visible to the caller's class
        FieldDefinition cachedInstance = callerCachedInstanceBinder.getCachedInstance(constructor.asType(methodType(Object.class)));

        // update all CompiledLambda with references to the new class, so a lambda metafactory can be bound directly into the caller
        return generatedMethods.entrySet()
                .stream()
                .collect(toImmutableMap(Entry::getKey, entry -> entry.getValue().withLoadedClass(lambdaClass, cachedInstance)));
    }

    private static Map<LambdaDefinitionExpression, CompiledLambda> generateMethodsForLambdaInternal(
            ClassBuilder classBuilder,
            CachedInstanceBinder cachedInstanceBinder,
            RowExpression expression,
            FunctionManager functionManager)
    {
        Set<LambdaDefinitionExpression> lambdaExpressions = ImmutableSet.copyOf(extractLambdaExpressions(expression));
        ImmutableMap.Builder<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap = ImmutableMap.builder();

        int counter = 0;
        for (LambdaDefinitionExpression lambdaExpression : lambdaExpressions) {
            CompiledLambda compiledLambda = preGenerateLambdaExpression(
                    lambdaExpression,
                    "lambda_" + counter,
                    classBuilder,
                    compiledLambdaMap.buildOrThrow(),
                    cachedInstanceBinder,
                    functionManager);
            compiledLambdaMap.put(lambdaExpression, compiledLambda);
            counter++;
        }

        return compiledLambdaMap.buildOrThrow();
    }

    /**
     * @return a MethodHandle field that represents the lambda expression
     */
    private static CompiledLambda preGenerateLambdaExpression(
            LambdaDefinitionExpression lambdaExpression,
            String methodName,
            ClassBuilder classBuilder,
            Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap,
            CachedInstanceBinder cachedInstanceBinder,
            FunctionManager functionManager)
    {
        ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
        ImmutableMap.Builder<String, ParameterAndType> parameterMapBuilder = ImmutableMap.builder();

        parameters.add(arg("session", ConnectorSession.class));
        for (int i = 0; i < lambdaExpression.getArguments().size(); i++) {
            Class<?> type = Primitives.wrap(lambdaExpression.getArgumentTypes().get(i).getJavaType());
            String argumentName = lambdaExpression.getArguments().get(i);
            Parameter arg = arg("lambda_" + i + "_" + BytecodeUtils.sanitizeName(argumentName), type);
            parameters.add(arg);
            parameterMapBuilder.put(argumentName, new ParameterAndType(arg, type));
        }

        RowExpressionCompiler innerExpressionCompiler = new RowExpressionCompiler(
                classBuilder,
                cachedInstanceBinder,
                variableReferenceCompiler(parameterMapBuilder.buildOrThrow()),
                functionManager,
                compiledLambdaMap);

        return defineLambdaMethod(
                innerExpressionCompiler,
                classBuilder,
                methodName,
                parameters.build(),
                lambdaExpression);
    }

    private static CompiledLambda defineLambdaMethod(
            RowExpressionCompiler innerExpressionCompiler,
            ClassBuilder classBuilder,
            String methodName,
            List<Parameter> inputParameters,
            LambdaDefinitionExpression lambda)
    {
        checkCondition(inputParameters.size() <= 254, NOT_SUPPORTED, "Too many arguments for lambda expression");
        Class<?> returnType = Primitives.wrap(lambda.getBody().getType().getJavaType());
        MethodDefinition method = classBuilder.declareMethod(a(PUBLIC), methodName, type(returnType), inputParameters);

        Scope scope = method.getScope();
        Variable wasNull = scope.declareVariable(boolean.class, "wasNull");
        BytecodeNode compiledBody = innerExpressionCompiler.compile(lambda.getBody(), scope);
        method.getBody()
                .putVariable(wasNull, false)
                .append(compiledBody)
                .append(boxPrimitiveIfNecessary(scope, returnType))
                .ret(returnType);

        return new CompiledLambda(method);
    }

    public static BytecodeNode generateLambda(
            BytecodeGeneratorContext context,
            List<RowExpression> captureExpressions,
            CompiledLambda compiledLambda,
            Class<?> lambdaInterface)
    {
        if (!lambdaInterface.isAnnotationPresent(FunctionalInterface.class)) {
            // lambdaInterface is checked to be annotated with FunctionalInterface when generating ScalarFunctionImplementation
            throw new VerifyException("lambda should be generated as class annotated with FunctionalInterface");
        }

        BytecodeBlock block = new BytecodeBlock().setDescription("Partial apply");
        Scope scope = context.getScope();

        Variable wasNull = scope.getVariable("wasNull");

        // generate values to be captured
        ImmutableList.Builder<BytecodeExpression> captureVariableBuilder = ImmutableList.builder();
        for (RowExpression captureExpression : captureExpressions) {
            Class<?> valueType = Primitives.wrap(captureExpression.getType().getJavaType());
            Variable valueVariable = scope.createTempVariable(valueType);
            block.append(context.generate(captureExpression));
            block.append(boxPrimitiveIfNecessary(scope, valueType));
            block.putVariable(valueVariable);
            block.append(wasNull.set(constantFalse()));
            captureVariableBuilder.add(valueVariable);
        }

        // is this a nested lambda call or a direct call from another class
        Method singleApplyMethod = getSingleApplyMethod(lambdaInterface);
        if (compiledLambda.getLambdaClass() == null) {
            List<BytecodeExpression> captureVariables = ImmutableList.<BytecodeExpression>builder()
                    .add(scope.getThis())
                    .add(scope.getVariable("session"))
                    .addAll(captureVariableBuilder.build())
                    .build();

            Type instantiatedMethodAsmType = getMethodType(
                    compiledLambda.getReturnType().getAsmType(),
                    compiledLambda.getParameterTypes().stream()
                            .skip(captureExpressions.size() + 1) // skip capture variables and ConnectorSession
                            .map(ParameterizedType::getAsmType)
                            .toArray(Type[]::new));

            // generate invoke dynamic to lambda meta factory for a method on the class currently being built
            block.append(
                    invokeDynamic(
                            LAMBDA_CAPTURE_METHOD,
                            ImmutableList.of(
                                    getType(singleApplyMethod),
                                    compiledLambda.getLambdaAsmHandle(),
                                    instantiatedMethodAsmType),
                            singleApplyMethod.getName(),
                            type(lambdaInterface),
                            captureVariables));
        }
        else {
            ImmutableList.Builder<Class<?>> callSiteParameterTypes = ImmutableList.builder();
            callSiteParameterTypes.add(compiledLambda.getLambdaClass());
            callSiteParameterTypes.add(ConnectorSession.class);
            for (RowExpression captureExpression : captureExpressions) {
                callSiteParameterTypes.add(Primitives.wrap(captureExpression.getType().getJavaType()));
            }

            List<BytecodeExpression> captureVariables = ImmutableList.<BytecodeExpression>builder()
                    .add(scope.getThis().getField(compiledLambda.getCachedInstance()), scope.getVariable("session"))
                    .addAll(captureVariableBuilder.build())
                    .build();

            try {
                // create the lambda meta right here
                CallSite metafactory = LambdaMetafactory.metafactory(
                        compiledLambda.getLambdaLookup(),
                        singleApplyMethod.getName(),
                        methodType(lambdaInterface, callSiteParameterTypes.build()),
                        methodType(singleApplyMethod.getReturnType(), singleApplyMethod.getParameterTypes()),
                        compiledLambda.getMethodHandle(),
                        compiledLambda.getMethodHandle().type().dropParameterTypes(0, captureVariables.size()));

                // extract the method handle from the call site
                MethodHandle factoryMethodHandle = metafactory.getTarget();
                // modify the factory handle to accept an Object instead of the actual target type, because the caller class
                // cannot see the lambda class, which is in a separate class loader
                factoryMethodHandle = explicitCastArguments(factoryMethodHandle, factoryMethodHandle.type().changeParameterType(0, Object.class));
                block.append(context.getClassBuilder().invoke(factoryMethodHandle, "ignored", captureVariables));
            }
            catch (LambdaConversionException e) {
                throw new RuntimeException(e);
            }
        }
        return block;
    }

    public static Class<? extends Supplier<Object>> compileLambdaProvider(LambdaDefinitionExpression lambdaExpression, FunctionManager functionManager, Class<?> lambdaInterface)
    {
        ClassBuilder lambdaProviderClassBuilder = ClassBuilder.createHiddenClass(
                lookup(),
                a(PUBLIC, FINAL),
                "LambdaProvider",
                type(Object.class),
                type(Supplier.class, Object.class));

        FieldDefinition sessionField = lambdaProviderClassBuilder.declareField(a(PRIVATE), "session", ConnectorSession.class);

        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(lambdaProviderClassBuilder);

        Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap = generateMethodsForLambda(
                lambdaExpression,
                cachedInstanceBinder,
                functionManager);

        MethodDefinition method = lambdaProviderClassBuilder.declareMethod(
                a(PUBLIC),
                "get",
                type(Object.class),
                ImmutableList.of());

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();
        scope.declareVariable("wasNull", body, constantFalse());
        scope.declareVariable("session", body, method.getThis().getField(sessionField));

        RowExpressionCompiler rowExpressionCompiler = new RowExpressionCompiler(
                lambdaProviderClassBuilder,
                cachedInstanceBinder,
                variableReferenceCompiler(ImmutableMap.of()),
                functionManager,
                compiledLambdaMap);

        BytecodeGeneratorContext generatorContext = new BytecodeGeneratorContext(
                rowExpressionCompiler,
                scope,
                lambdaProviderClassBuilder,
                cachedInstanceBinder,
                functionManager);

        body.append(
                generateLambda(
                        generatorContext,
                        ImmutableList.of(),
                        compiledLambdaMap.get(lambdaExpression),
                        lambdaInterface))
                .retObject();

        // constructor
        Parameter sessionParameter = arg("session", ConnectorSession.class);

        MethodDefinition constructorDefinition = lambdaProviderClassBuilder.declareConstructor(a(PUBLIC), sessionParameter);
        BytecodeBlock constructorBody = constructorDefinition.getBody();
        Variable constructorThisVariable = constructorDefinition.getThis();

        constructorBody.comment("super();")
                .append(constructorThisVariable)
                .invokeConstructor(Object.class)
                .append(constructorThisVariable.setField(sessionField, sessionParameter));

        cachedInstanceBinder.generateInitializations(constructorThisVariable, constructorBody);
        constructorBody.ret();

        //noinspection unchecked
        return (Class<? extends Supplier<Object>>) lambdaProviderClassBuilder.defineClass(Supplier.class);
    }

    private static Method getSingleApplyMethod(Class<?> lambdaFunctionInterface)
    {
        checkCondition(lambdaFunctionInterface.isAnnotationPresent(FunctionalInterface.class), COMPILER_ERROR, "Lambda function interface is required to be annotated with FunctionalInterface");

        List<Method> applyMethods = Arrays.stream(lambdaFunctionInterface.getMethods())
                .filter(method -> method.getName().equals("apply"))
                .collect(toImmutableList());

        checkCondition(applyMethods.size() == 1, COMPILER_ERROR, "Expect to have exactly 1 method with name 'apply' in interface " + lambdaFunctionInterface.getName());
        return applyMethods.get(0);
    }

    private static RowExpressionVisitor<BytecodeNode, Scope> variableReferenceCompiler(Map<String, ParameterAndType> parameterMap)
    {
        return new RowExpressionVisitor<>()
        {
            @Override
            public BytecodeNode visitInputReference(InputReferenceExpression node, Scope scope)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public BytecodeNode visitCall(CallExpression call, Scope scope)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public BytecodeNode visitSpecialForm(SpecialForm specialForm, Scope context)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public BytecodeNode visitConstant(ConstantExpression literal, Scope scope)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public BytecodeNode visitLambda(LambdaDefinitionExpression lambda, Scope context)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public BytecodeNode visitVariableReference(VariableReferenceExpression reference, Scope context)
            {
                ParameterAndType parameterAndType = parameterMap.get(reference.getName());
                Parameter parameter = parameterAndType.getParameter();
                Class<?> type = parameterAndType.getType();
                return new BytecodeBlock()
                        .append(parameter)
                        .append(unboxPrimitiveIfNecessary(context, type));
            }
        };
    }

    static class CompiledLambda
    {
        // Information for nested lambda calls which use indy to lambda metafactory
        private final Handle lambdaAsmHandle;
        private final ParameterizedType returnType;
        private final List<ParameterizedType> parameterTypes;

        // Information for normal lambda calls from expressions, which use constant dynamic to a pre created lambda metafactory
        private final Class<?> lambdaClass;
        private final FieldDefinition cachedInstance;
        private final Lookup lambdaLookup;
        private final MethodHandle methodHandle;

        private CompiledLambda(MethodDefinition method)
        {
            lambdaAsmHandle = new Handle(
                    Opcodes.H_INVOKEVIRTUAL,
                    method.getThis().getType().getClassName(),
                    method.getName(),
                    method.getMethodDescriptor(),
                    false);
            returnType = method.getReturnType();
            parameterTypes = method.getParameterTypes();

            cachedInstance = null;
            lambdaLookup = null;
            lambdaClass = null;
            methodHandle = null;
        }

        private CompiledLambda(
                Handle lambdaAsmHandle,
                ParameterizedType returnType,
                List<ParameterizedType> parameterTypes,
                Class<?> lambdaClass,
                FieldDefinition cachedInstance)
        {
            this.lambdaAsmHandle = requireNonNull(lambdaAsmHandle, "lambdaAsmHandle is null");
            this.returnType = requireNonNull(returnType, "returnType is null");
            this.parameterTypes = requireNonNull(parameterTypes, "parameterTypes is null");

            this.lambdaClass = lambdaClass;
            this.cachedInstance = cachedInstance;
            try {
                this.lambdaLookup = (Lookup) privateLookupIn(lambdaClass, lookup()).findStatic(lambdaClass, "lookup", methodType(Lookup.class)).invoke();
                Method method = Arrays.stream(lambdaClass.getMethods()).filter(m -> lambdaAsmHandle.getName().equals(m.getName())).findFirst().orElseThrow();
                this.methodHandle = lambdaLookup.unreflect(method);
            }
            catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        public Handle getLambdaAsmHandle()
        {
            return lambdaAsmHandle;
        }

        public ParameterizedType getReturnType()
        {
            return returnType;
        }

        public List<ParameterizedType> getParameterTypes()
        {
            return parameterTypes;
        }

        public Class<?> getLambdaClass()
        {
            return lambdaClass;
        }

        public FieldDefinition getCachedInstance()
        {
            return cachedInstance;
        }

        public Lookup getLambdaLookup()
        {
            return lambdaLookup;
        }

        public MethodHandle getMethodHandle()
        {
            return methodHandle;
        }

        public CompiledLambda withLoadedClass(Class<?> lambdaClass, FieldDefinition cachedInstance)
        {
            return new CompiledLambda(lambdaAsmHandle, returnType, parameterTypes, lambdaClass, cachedInstance);
        }
    }
}
