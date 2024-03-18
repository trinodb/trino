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
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.trino.cache.NonEvictableCache;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.metadata.FunctionBinder.CatalogFunctionBinding;
import io.trino.spi.TrinoException;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.Signature;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.sql.analyzer.TypeSignatureProvider;

import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.metadata.FunctionResolver.resolveFunctionBinding;
import static io.trino.metadata.GlobalFunctionCatalog.BUILTIN_SCHEMA;
import static io.trino.metadata.GlobalFunctionCatalog.isBuiltinFunctionName;
import static io.trino.metadata.OperatorNameUtil.mangleOperatorName;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * This class is designed for the exclusive use of Metadata, and is not intended for any other use.
 */
class BuiltinFunctionResolver
{
    private final Metadata metadata;
    private final TypeManager typeManager;
    private final GlobalFunctionCatalog globalFunctionCatalog;
    private final FunctionBinder functionBinder;

    private final NonEvictableCache<OperatorCacheKey, ResolvedFunction> operatorCache;
    private final NonEvictableCache<CoercionCacheKey, ResolvedFunction> coercionCache;

    public BuiltinFunctionResolver(Metadata metadata, TypeManager typeManager, GlobalFunctionCatalog globalFunctionCatalog)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.globalFunctionCatalog = requireNonNull(globalFunctionCatalog, "globalFunctionCatalog is null");
        this.functionBinder = new FunctionBinder(metadata, typeManager);

        operatorCache = buildNonEvictableCache(CacheBuilder.newBuilder().maximumSize(1000));
        coercionCache = buildNonEvictableCache(CacheBuilder.newBuilder().maximumSize(1000));
    }

    ResolvedFunction resolveBuiltinFunction(String name, List<TypeSignatureProvider> parameterTypes)
    {
        CatalogFunctionBinding functionBinding = functionBinder.bindFunction(parameterTypes, getBuiltinFunctions(name), name);
        return resolveBuiltin(functionBinding);
    }

    ResolvedFunction resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
            throws OperatorNotFoundException
    {
        try {
            return uncheckedCacheGet(operatorCache, new OperatorCacheKey(operatorType, argumentTypes),
                    () -> resolveBuiltinFunction(
                            mangleOperatorName(operatorType),
                            argumentTypes.stream()
                                    .map(Type::getTypeSignature)
                                    .map(TypeSignatureProvider::new)
                                    .collect(toImmutableList())));
        }
        catch (UncheckedExecutionException e) {
            if (e.getCause() instanceof TrinoException cause) {
                if (cause.getErrorCode().getCode() == FUNCTION_NOT_FOUND.toErrorCode().getCode()) {
                    throw new OperatorNotFoundException(operatorType, argumentTypes, cause);
                }
                throw cause;
            }
            throw e;
        }
    }

    ResolvedFunction resolveCoercion(OperatorType operatorType, Type fromType, Type toType)
    {
        checkArgument(operatorType == OperatorType.CAST || operatorType == OperatorType.SATURATED_FLOOR_CAST);
        try {
            return uncheckedCacheGet(coercionCache, new CoercionCacheKey(operatorType, fromType, toType),
                    () -> resolveCoercion(mangleOperatorName(operatorType), fromType, toType));
        }
        catch (UncheckedExecutionException e) {
            if (e.getCause() instanceof TrinoException cause) {
                if (cause.getErrorCode().getCode() == FUNCTION_IMPLEMENTATION_MISSING.toErrorCode().getCode()) {
                    throw new OperatorNotFoundException(operatorType, ImmutableList.of(fromType), toType.getTypeSignature(), cause);
                }
                throw cause;
            }
            throw e;
        }
    }

    ResolvedFunction resolveCoercion(String functionName, Type fromType, Type toType)
    {
        CatalogFunctionBinding functionBinding = functionBinder.bindCoercion(
                Signature.builder()
                        .returnType(toType)
                        .argumentType(fromType)
                        .build(),
                getBuiltinFunctions(functionName));
        return resolveBuiltin(functionBinding);
    }

    private ResolvedFunction resolveBuiltin(CatalogFunctionBinding functionBinding)
    {
        FunctionBinding binding = functionBinding.functionBinding();
        FunctionDependencyDeclaration dependencies = globalFunctionCatalog.getFunctionDependencies(binding.getFunctionId(), binding.getBoundSignature());

        return resolveFunctionBinding(
                metadata,
                typeManager,
                functionBinder,
                GlobalSystemConnector.CATALOG_HANDLE,
                functionBinding.functionBinding(),
                functionBinding.boundFunctionMetadata(),
                dependencies,
                catalogSchemaFunctionName -> {
                    // builtin functions can only depend on other builtin functions
                    if (!isBuiltinFunctionName(catalogSchemaFunctionName)) {
                        throw new TrinoException(
                                FUNCTION_IMPLEMENTATION_ERROR,
                                format("Builtin function %s cannot depend on a non-builtin function: %s", functionBinding.functionBinding().getBoundSignature().getName(), catalogSchemaFunctionName));
                    }
                    return getBuiltinFunctions(catalogSchemaFunctionName.getFunctionName());
                },
                this::resolveBuiltin);
    }

    private Collection<CatalogFunctionMetadata> getBuiltinFunctions(String functionName)
    {
        return globalFunctionCatalog.getBuiltInFunctions(functionName).stream()
                .map(function -> new CatalogFunctionMetadata(GlobalSystemConnector.CATALOG_HANDLE, BUILTIN_SCHEMA, function))
                .collect(toImmutableList());
    }

    private record OperatorCacheKey(OperatorType operatorType, List<? extends Type> argumentTypes)
    {
        private OperatorCacheKey
        {
            requireNonNull(operatorType, "operatorType is null");
            argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
        }
    }

    private record CoercionCacheKey(OperatorType operatorType, Type fromType, Type toType)
    {
        private CoercionCacheKey
        {
            requireNonNull(operatorType, "operatorType is null");
            requireNonNull(fromType, "fromType is null");
            requireNonNull(toType, "toType is null");
        }
    }
}
