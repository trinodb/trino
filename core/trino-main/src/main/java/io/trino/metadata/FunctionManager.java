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

import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;
import io.trino.FeaturesConfig;
import io.trino.collect.cache.NonEvictableCache;
import io.trino.connector.CatalogServiceProvider;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.AggregationImplementation;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.InOut;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.InvocationConvention.InvocationArgumentConvention;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.function.WindowFunctionSupplier;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.type.BlockTypeOperators;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.primitives.Primitives.wrap;
import static io.trino.client.NodeVersion.UNKNOWN;
import static io.trino.collect.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.HOURS;

public class FunctionManager
{
    private final NonEvictableCache<FunctionKey, ScalarFunctionImplementation> specializedScalarCache;
    private final NonEvictableCache<FunctionKey, AggregationImplementation> specializedAggregationCache;
    private final NonEvictableCache<FunctionKey, WindowFunctionSupplier> specializedWindowCache;

    private final CatalogServiceProvider<FunctionProvider> functionProviders;
    private final GlobalFunctionCatalog globalFunctionCatalog;

    @Inject
    public FunctionManager(CatalogServiceProvider<FunctionProvider> functionProviders, GlobalFunctionCatalog globalFunctionCatalog)
    {
        specializedScalarCache = buildNonEvictableCache(CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, HOURS));

        specializedAggregationCache = buildNonEvictableCache(CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, HOURS));

        specializedWindowCache = buildNonEvictableCache(CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, HOURS));

        this.functionProviders = requireNonNull(functionProviders, "functionProviders is null");
        this.globalFunctionCatalog = requireNonNull(globalFunctionCatalog, "globalFunctionCatalog is null");
    }

    public ScalarFunctionImplementation getScalarFunctionImplementation(ResolvedFunction resolvedFunction, InvocationConvention invocationConvention)
    {
        try {
            return uncheckedCacheGet(specializedScalarCache, new FunctionKey(resolvedFunction, invocationConvention), () -> getScalarFunctionImplementationInternal(resolvedFunction, invocationConvention));
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new RuntimeException(e.getCause());
        }
    }

    private ScalarFunctionImplementation getScalarFunctionImplementationInternal(ResolvedFunction resolvedFunction, InvocationConvention invocationConvention)
    {
        FunctionDependencies functionDependencies = getFunctionDependencies(resolvedFunction);
        ScalarFunctionImplementation scalarFunctionImplementation = getFunctionProvider(resolvedFunction).getScalarFunctionImplementation(
                resolvedFunction.getFunctionId(),
                resolvedFunction.getSignature(),
                functionDependencies,
                invocationConvention);
        verifyMethodHandleSignature(resolvedFunction.getSignature(), scalarFunctionImplementation, invocationConvention);
        return scalarFunctionImplementation;
    }

    public AggregationImplementation getAggregationImplementation(ResolvedFunction resolvedFunction)
    {
        try {
            return uncheckedCacheGet(specializedAggregationCache, new FunctionKey(resolvedFunction), () -> getAggregationImplementationInternal(resolvedFunction));
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new RuntimeException(e.getCause());
        }
    }

    private AggregationImplementation getAggregationImplementationInternal(ResolvedFunction resolvedFunction)
    {
        FunctionDependencies functionDependencies = getFunctionDependencies(resolvedFunction);
        return getFunctionProvider(resolvedFunction).getAggregationImplementation(
                resolvedFunction.getFunctionId(),
                resolvedFunction.getSignature(),
                functionDependencies);
    }

    public WindowFunctionSupplier getWindowFunctionSupplier(ResolvedFunction resolvedFunction)
    {
        try {
            return uncheckedCacheGet(specializedWindowCache, new FunctionKey(resolvedFunction), () -> getWindowFunctionSupplierInternal(resolvedFunction));
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new RuntimeException(e.getCause());
        }
    }

    private WindowFunctionSupplier getWindowFunctionSupplierInternal(ResolvedFunction resolvedFunction)
    {
        FunctionDependencies functionDependencies = getFunctionDependencies(resolvedFunction);
        return getFunctionProvider(resolvedFunction).getWindowFunctionSupplier(
                resolvedFunction.getFunctionId(),
                resolvedFunction.getSignature(),
                functionDependencies);
    }

    public TableFunctionProcessorProvider getTableFunctionProcessorProvider(TableFunctionHandle tableFunctionHandle)
    {
        CatalogHandle catalogHandle = tableFunctionHandle.getCatalogHandle();

        FunctionProvider provider;
        if (catalogHandle.equals(GlobalSystemConnector.CATALOG_HANDLE)) {
            provider = globalFunctionCatalog;
        }
        else {
            provider = functionProviders.getService(catalogHandle);
            checkArgument(provider != null, "No function provider for catalog: '%s'", catalogHandle);
        }

        return provider.getTableFunctionProcessorProvider(tableFunctionHandle.getFunctionHandle());
    }

    private FunctionDependencies getFunctionDependencies(ResolvedFunction resolvedFunction)
    {
        return new InternalFunctionDependencies(this::getScalarFunctionImplementation, resolvedFunction.getTypeDependencies(), resolvedFunction.getFunctionDependencies());
    }

    private FunctionProvider getFunctionProvider(ResolvedFunction resolvedFunction)
    {
        if (resolvedFunction.getCatalogHandle().equals(GlobalSystemConnector.CATALOG_HANDLE)) {
            return globalFunctionCatalog;
        }

        FunctionProvider functionProvider = functionProviders.getService(resolvedFunction.getCatalogHandle());
        checkArgument(functionProvider != null, "No function provider for catalog: '%s' (function '%s')", resolvedFunction.getCatalogHandle(), resolvedFunction.getSignature().getName());
        return functionProvider;
    }

    private static void verifyMethodHandleSignature(BoundSignature boundSignature, ScalarFunctionImplementation scalarFunctionImplementation, InvocationConvention convention)
    {
        MethodHandle methodHandle = scalarFunctionImplementation.getMethodHandle();
        MethodType methodType = methodHandle.type();

        checkArgument(convention.getArgumentConventions().size() == boundSignature.getArgumentTypes().size(),
                "Expected %s arguments, but got %s", boundSignature.getArgumentTypes().size(), convention.getArgumentConventions().size());

        int expectedParameterCount = convention.getArgumentConventions().stream()
                .mapToInt(InvocationArgumentConvention::getParameterCount)
                .sum();
        expectedParameterCount += methodType.parameterList().stream().filter(ConnectorSession.class::equals).count();
        if (scalarFunctionImplementation.getInstanceFactory().isPresent()) {
            expectedParameterCount++;
        }
        checkArgument(expectedParameterCount == methodType.parameterCount(),
                "Expected %s method parameters, but got %s", expectedParameterCount, methodType.parameterCount());

        int parameterIndex = 0;
        if (scalarFunctionImplementation.getInstanceFactory().isPresent()) {
            verifyFunctionSignature(convention.supportsInstanceFactory(), "Method requires instance factory, but calling convention does not support an instance factory");
            MethodHandle factoryMethod = scalarFunctionImplementation.getInstanceFactory().orElseThrow();
            verifyFunctionSignature(methodType.parameterType(parameterIndex).equals(factoryMethod.type().returnType()), "Invalid return type");
            parameterIndex++;
        }

        int lambdaArgumentIndex = 0;
        for (int argumentIndex = 0; argumentIndex < boundSignature.getArgumentTypes().size(); argumentIndex++) {
            // skip session parameters
            while (methodType.parameterType(parameterIndex).equals(ConnectorSession.class)) {
                verifyFunctionSignature(convention.supportsSession(), "Method requires session, but calling convention does not support session");
                parameterIndex++;
            }

            Class<?> parameterType = methodType.parameterType(parameterIndex);
            Type argumentType = boundSignature.getArgumentTypes().get(argumentIndex);
            InvocationArgumentConvention argumentConvention = convention.getArgumentConvention(argumentIndex);
            switch (argumentConvention) {
                case NEVER_NULL:
                    verifyFunctionSignature(parameterType.isAssignableFrom(argumentType.getJavaType()),
                            "Expected argument type to be %s, but is %s", argumentType, parameterType);
                    break;
                case NULL_FLAG:
                    verifyFunctionSignature(parameterType.isAssignableFrom(argumentType.getJavaType()),
                            "Expected argument type to be %s, but is %s", argumentType.getJavaType(), parameterType);
                    verifyFunctionSignature(methodType.parameterType(parameterIndex + 1).equals(boolean.class),
                            "Expected null flag parameter to be followed by a boolean parameter");
                    break;
                case BOXED_NULLABLE:
                    verifyFunctionSignature(parameterType.isAssignableFrom(wrap(argumentType.getJavaType())),
                            "Expected argument type to be %s, but is %s", wrap(argumentType.getJavaType()), parameterType);
                    break;
                case BLOCK_POSITION:
                    verifyFunctionSignature(parameterType.equals(Block.class) && methodType.parameterType(parameterIndex + 1).equals(int.class),
                            "Expected BLOCK_POSITION argument types to be Block and int");
                    break;
                case IN_OUT:
                    verifyFunctionSignature(parameterType.equals(InOut.class), "Expected IN_OUT argument type to be InOut");
                    break;
                case FUNCTION:
                    Class<?> lambdaInterface = scalarFunctionImplementation.getLambdaInterfaces().get(lambdaArgumentIndex);
                    verifyFunctionSignature(parameterType.equals(lambdaInterface),
                            "Expected function interface to be %s, but is %s", lambdaInterface, parameterType);
                    lambdaArgumentIndex++;
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown argument convention: " + argumentConvention);
            }
            parameterIndex += argumentConvention.getParameterCount();
        }

        Type returnType = boundSignature.getReturnType();
        switch (convention.getReturnConvention()) {
            case FAIL_ON_NULL:
                verifyFunctionSignature(methodType.returnType().isAssignableFrom(returnType.getJavaType()),
                        "Expected return type to be %s, but is %s", returnType.getJavaType(), methodType.returnType());
                break;
            case NULLABLE_RETURN:
                verifyFunctionSignature(methodType.returnType().isAssignableFrom(wrap(returnType.getJavaType())),
                        "Expected return type to be %s, but is %s", returnType.getJavaType(), wrap(methodType.returnType()));
                break;
            default:
                throw new UnsupportedOperationException("Unknown return convention: " + convention.getReturnConvention());
        }
    }

    private static void verifyFunctionSignature(boolean check, String message, Object... args)
    {
        if (!check) {
            throw new TrinoException(FUNCTION_IMPLEMENTATION_ERROR, format(message, args));
        }
    }

    private static class FunctionKey
    {
        private final FunctionId functionId;
        private final BoundSignature boundSignature;
        private final Optional<InvocationConvention> invocationConvention;

        public FunctionKey(ResolvedFunction resolvedFunction)
        {
            this(resolvedFunction.getFunctionId(), resolvedFunction.getSignature(), Optional.empty());
        }

        public FunctionKey(ResolvedFunction resolvedFunction, InvocationConvention invocationConvention)
        {
            this(resolvedFunction.getFunctionId(), resolvedFunction.getSignature(), Optional.of(invocationConvention));
        }

        public FunctionKey(FunctionId functionId, BoundSignature boundSignature, Optional<InvocationConvention> invocationConvention)
        {
            this.functionId = requireNonNull(functionId, "functionId is null");
            this.boundSignature = requireNonNull(boundSignature, "boundSignature is null");
            this.invocationConvention = requireNonNull(invocationConvention, "invocationConvention is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FunctionKey that = (FunctionKey) o;
            return functionId.equals(that.functionId) &&
                    boundSignature.equals(that.boundSignature) &&
                    invocationConvention.equals(that.invocationConvention);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(functionId, boundSignature, invocationConvention);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this).omitNullValues()
                    .add("functionId", functionId)
                    .add("boundSignature", boundSignature)
                    .add("invocationConvention", invocationConvention.orElse(null))
                    .toString();
        }
    }

    public static FunctionManager createTestingFunctionManager()
    {
        TypeOperators typeOperators = new TypeOperators();
        GlobalFunctionCatalog functionCatalog = new GlobalFunctionCatalog();
        functionCatalog.addFunctions(SystemFunctionBundle.create(new FeaturesConfig(), typeOperators, new BlockTypeOperators(typeOperators), UNKNOWN));
        functionCatalog.addFunctions(new InternalFunctionBundle(new LiteralFunction(new InternalBlockEncodingSerde(new BlockEncodingManager(), TESTING_TYPE_MANAGER))));
        return new FunctionManager(CatalogServiceProvider.fail(), functionCatalog);
    }
}
