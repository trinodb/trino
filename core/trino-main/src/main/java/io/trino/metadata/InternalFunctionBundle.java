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
import io.trino.collect.cache.NonEvictableCache;
import io.trino.operator.scalar.SpecializedSqlScalarFunction;
import io.trino.operator.scalar.annotations.ScalarFromAnnotationsParser;
import io.trino.operator.window.SqlWindowFunction;
import io.trino.operator.window.WindowAnnotationsParser;
import io.trino.spi.TrinoException;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationFunctionMetadata;
import io.trino.spi.function.AggregationImplementation;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.WindowFunction;
import io.trino.spi.function.WindowFunctionSupplier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.collect.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.HOURS;

public class InternalFunctionBundle
        implements FunctionBundle
{
    // scalar function specialization may involve expensive code generation
    private final NonEvictableCache<FunctionKey, SpecializedSqlScalarFunction> specializedScalarCache;
    private final NonEvictableCache<FunctionKey, AggregationImplementation> specializedAggregationCache;
    private final NonEvictableCache<FunctionKey, WindowFunctionSupplier> specializedWindowCache;
    private final Map<FunctionId, SqlFunction> functions;

    public InternalFunctionBundle(SqlFunction... functions)
    {
        this(ImmutableList.copyOf(functions));
    }

    public InternalFunctionBundle(List<? extends SqlFunction> functions)
    {
        // We have observed repeated compilation of MethodHandle that leads to full GCs.
        // We notice that flushing the following caches mitigate the problem.
        // We suspect that it is a JVM bug that is related to stale/corrupted profiling data associated
        // with generated classes and/or dynamically-created MethodHandles.
        // This might also mitigate problems like deoptimization storm or unintended interpreted execution.

        specializedScalarCache = buildNonEvictableCache(CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, HOURS));

        specializedAggregationCache = buildNonEvictableCache(CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, HOURS));
        specializedWindowCache = buildNonEvictableCache(CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, HOURS));

        this.functions = functions.stream()
                .collect(toImmutableMap(function -> function.getFunctionMetadata().getFunctionId(), Function.identity()));
    }

    @Override
    public Collection<FunctionMetadata> getFunctions()
    {
        return functions.values().stream()
                .map(SqlFunction::getFunctionMetadata)
                .collect(toImmutableList());
    }

    @Override
    public AggregationFunctionMetadata getAggregationFunctionMetadata(FunctionId functionId)
    {
        SqlFunction function = getSqlFunction(functionId);
        checkArgument(function instanceof SqlAggregationFunction, "%s is not an aggregation function", function.getFunctionMetadata().getSignature());

        SqlAggregationFunction aggregationFunction = (SqlAggregationFunction) function;
        return aggregationFunction.getAggregationMetadata();
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies(FunctionId functionId, BoundSignature boundSignature)
    {
        return getSqlFunction(functionId).getFunctionDependencies(boundSignature);
    }

    @Override
    public ScalarFunctionImplementation getScalarFunctionImplementation(
            FunctionId functionId,
            BoundSignature boundSignature,
            FunctionDependencies functionDependencies,
            InvocationConvention invocationConvention)
    {
        SpecializedSqlScalarFunction specializedSqlScalarFunction;
        try {
            specializedSqlScalarFunction = uncheckedCacheGet(
                    specializedScalarCache,
                    new FunctionKey(functionId, boundSignature),
                    () -> specializeScalarFunction(functionId, boundSignature, functionDependencies));
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new RuntimeException(e.getCause());
        }
        return specializedSqlScalarFunction.getScalarFunctionImplementation(invocationConvention);
    }

    private SpecializedSqlScalarFunction specializeScalarFunction(FunctionId functionId, BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        SqlScalarFunction function = (SqlScalarFunction) getSqlFunction(functionId);
        return function.specialize(boundSignature, functionDependencies);
    }

    @Override
    public AggregationImplementation getAggregationImplementation(FunctionId functionId, BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        try {
            return uncheckedCacheGet(specializedAggregationCache, new FunctionKey(functionId, boundSignature), () -> specializedAggregation(functionId, boundSignature, functionDependencies));
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new RuntimeException(e.getCause());
        }
    }

    private AggregationImplementation specializedAggregation(FunctionId functionId, BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        SqlAggregationFunction aggregationFunction = (SqlAggregationFunction) functions.get(functionId);
        return aggregationFunction.specialize(boundSignature, functionDependencies);
    }

    @Override
    public WindowFunctionSupplier getWindowFunctionSupplier(FunctionId functionId, BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        try {
            return uncheckedCacheGet(specializedWindowCache, new FunctionKey(functionId, boundSignature), () -> specializeWindow(functionId, boundSignature, functionDependencies));
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new RuntimeException(e.getCause());
        }
    }

    private WindowFunctionSupplier specializeWindow(FunctionId functionId, BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        SqlWindowFunction function = (SqlWindowFunction) functions.get(functionId);
        return function.specialize(boundSignature, functionDependencies);
    }

    private SqlFunction getSqlFunction(FunctionId functionId)
    {
        SqlFunction function = functions.get(functionId);
        checkArgument(function != null, "Unknown function implementation: " + functionId);
        return function;
    }

    public static InternalFunctionBundle extractFunctions(Class<?> functionClass)
    {
        return builder().functions(functionClass).build();
    }

    public static InternalFunctionBundle extractFunctions(Collection<Class<?>> functionClasses)
    {
        InternalFunctionBundleBuilder builder = builder();
        functionClasses.forEach(builder::functions);
        return builder.build();
    }

    public static InternalFunctionBundleBuilder builder()
    {
        return new InternalFunctionBundleBuilder();
    }

    public static class InternalFunctionBundleBuilder
    {
        private final List<SqlFunction> functions = new ArrayList<>();

        private InternalFunctionBundleBuilder() {}

        public InternalFunctionBundleBuilder window(Class<? extends WindowFunction> clazz)
        {
            functions.addAll(WindowAnnotationsParser.parseFunctionDefinition(clazz));
            return this;
        }

        public InternalFunctionBundleBuilder aggregates(Class<?> aggregationDefinition)
        {
            functions.addAll(SqlAggregationFunction.createFunctionsByAnnotations(aggregationDefinition));
            return this;
        }

        public InternalFunctionBundleBuilder scalar(Class<?> clazz)
        {
            functions.addAll(ScalarFromAnnotationsParser.parseFunctionDefinition(clazz));
            return this;
        }

        public InternalFunctionBundleBuilder scalars(Class<?> clazz)
        {
            functions.addAll(ScalarFromAnnotationsParser.parseFunctionDefinitions(clazz));
            return this;
        }

        public InternalFunctionBundleBuilder functions(Class<?> clazz)
        {
            if (WindowFunction.class.isAssignableFrom(clazz)) {
                @SuppressWarnings("unchecked")
                Class<? extends WindowFunction> windowClazz = (Class<? extends WindowFunction>) clazz;
                window(windowClazz);
                return this;
            }

            if (clazz.isAnnotationPresent(AggregationFunction.class)) {
                aggregates(clazz);
                return this;
            }

            if (clazz.isAnnotationPresent(ScalarFunction.class) ||
                    clazz.isAnnotationPresent(ScalarOperator.class)) {
                scalar(clazz);
                return this;
            }

            scalars(clazz);
            return this;
        }

        public InternalFunctionBundleBuilder functions(SqlFunction... sqlFunctions)
        {
            for (SqlFunction sqlFunction : sqlFunctions) {
                function(sqlFunction);
            }
            return this;
        }

        public InternalFunctionBundleBuilder function(SqlFunction sqlFunction)
        {
            requireNonNull(sqlFunction, "sqlFunction is null");
            functions.add(sqlFunction);
            return this;
        }

        public InternalFunctionBundle build()
        {
            return new InternalFunctionBundle(functions);
        }
    }

    private static class FunctionKey
    {
        private final FunctionId functionId;
        private final BoundSignature boundSignature;

        public FunctionKey(FunctionId functionId, BoundSignature boundSignature)
        {
            this.functionId = requireNonNull(functionId, "functionId is null");
            this.boundSignature = requireNonNull(boundSignature, "boundSignature is null");
        }

        public FunctionId getFunctionId()
        {
            return functionId;
        }

        public BoundSignature getBoundSignature()
        {
            return boundSignature;
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
                    boundSignature.equals(that.boundSignature);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(functionId, boundSignature);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("functionId", functionId)
                    .add("boundSignature", boundSignature)
                    .toString();
        }
    }
}
