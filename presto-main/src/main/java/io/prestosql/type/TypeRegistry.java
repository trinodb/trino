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
package io.prestosql.type;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.ParametricType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeNotFoundException;
import io.prestosql.spi.type.TypeParameter;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.P4HyperLogLogType.P4_HYPER_LOG_LOG;
import static io.prestosql.spi.type.QuantileDigestParametricType.QDIGEST;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.type.ArrayParametricType.ARRAY;
import static io.prestosql.type.CodePointsType.CODE_POINTS;
import static io.prestosql.type.ColorType.COLOR;
import static io.prestosql.type.FunctionParametricType.FUNCTION;
import static io.prestosql.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.prestosql.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static io.prestosql.type.IpAddressType.IPADDRESS;
import static io.prestosql.type.JoniRegexpType.JONI_REGEXP;
import static io.prestosql.type.JsonPathType.JSON_PATH;
import static io.prestosql.type.JsonType.JSON;
import static io.prestosql.type.LikePatternType.LIKE_PATTERN;
import static io.prestosql.type.MapParametricType.MAP;
import static io.prestosql.type.Re2JRegexpType.RE2J_REGEXP;
import static io.prestosql.type.RowParametricType.ROW;
import static io.prestosql.type.UnknownType.UNKNOWN;
import static io.prestosql.type.UuidType.UUID;
import static io.prestosql.type.setdigest.SetDigestType.SET_DIGEST;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public final class TypeRegistry
        implements TypeManager
{
    private final TypeCoercion typeCoercion = new TypeCoercion(this::getType);

    private final ConcurrentMap<TypeSignature, Type> types = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ParametricType> parametricTypes = new ConcurrentHashMap<>();

    private FunctionRegistry functionRegistry;

    private final LoadingCache<TypeSignature, Type> parametricTypeCache;

    @VisibleForTesting
    public TypeRegistry()
    {
        this(ImmutableSet.of());
    }

    @Inject
    public TypeRegistry(Set<Type> types)
    {
        requireNonNull(types, "types is null");

        // Manually register UNKNOWN type without a verifyTypeClass call since it is a special type that can not be used by functions
        this.types.put(UNKNOWN.getTypeSignature(), UNKNOWN);

        // always add the built-in types; Presto will not function without these
        addType(BOOLEAN);
        addType(BIGINT);
        addType(INTEGER);
        addType(SMALLINT);
        addType(TINYINT);
        addType(DOUBLE);
        addType(REAL);
        addType(VARBINARY);
        addType(DATE);
        addType(TIME);
        addType(TIME_WITH_TIME_ZONE);
        addType(TIMESTAMP);
        addType(TIMESTAMP_WITH_TIME_ZONE);
        addType(INTERVAL_YEAR_MONTH);
        addType(INTERVAL_DAY_TIME);
        addType(HYPER_LOG_LOG);
        addType(SET_DIGEST);
        addType(P4_HYPER_LOG_LOG);
        addType(JONI_REGEXP);
        addType(RE2J_REGEXP);
        addType(LIKE_PATTERN);
        addType(JSON_PATH);
        addType(COLOR);
        addType(JSON);
        addType(CODE_POINTS);
        addType(IPADDRESS);
        addType(UUID);
        addParametricType(VarcharParametricType.VARCHAR);
        addParametricType(CharParametricType.CHAR);
        addParametricType(DecimalParametricType.DECIMAL);
        addParametricType(ROW);
        addParametricType(ARRAY);
        addParametricType(MAP);
        addParametricType(FUNCTION);
        addParametricType(QDIGEST);

        for (Type type : types) {
            addType(type);
        }
        parametricTypeCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .build(CacheLoader.from(this::instantiateParametricType));
    }

    public void setFunctionRegistry(FunctionRegistry functionRegistry)
    {
        checkState(this.functionRegistry == null, "TypeRegistry can only be associated with a single FunctionRegistry");
        this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry is null");
    }

    @Override
    public Type getType(TypeSignature signature)
    {
        Type type = types.get(signature);
        if (type == null) {
            try {
                return parametricTypeCache.getUnchecked(signature);
            }
            catch (UncheckedExecutionException e) {
                throwIfUnchecked(e.getCause());
                throw new RuntimeException(e.getCause());
            }
        }
        return type;
    }

    @Override
    public Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
    {
        return getType(new TypeSignature(baseTypeName, typeParameters));
    }

    private Type instantiateParametricType(TypeSignature signature)
    {
        List<TypeParameter> parameters = new ArrayList<>();

        for (TypeSignatureParameter parameter : signature.getParameters()) {
            TypeParameter typeParameter = TypeParameter.of(parameter, this);
            parameters.add(typeParameter);
        }

        ParametricType parametricType = parametricTypes.get(signature.getBase().toLowerCase(Locale.ENGLISH));
        if (parametricType == null) {
            throw new TypeNotFoundException(signature);
        }

        Type instantiatedType;
        try {
            instantiatedType = parametricType.createType(this, parameters);
        }
        catch (IllegalArgumentException e) {
            throw new TypeNotFoundException(signature, e);
        }

        // TODO: reimplement this check? Currently "varchar(Integer.MAX_VALUE)" fails with "varchar"
        //checkState(instantiatedType.equalsSignature(signature), "Instantiated parametric type name (%s) does not match expected name (%s)", instantiatedType, signature);
        return instantiatedType;
    }

    @Override
    public List<Type> getTypes()
    {
        return ImmutableList.copyOf(types.values());
    }

    @Override
    public boolean isTypeOnlyCoercion(Type source, Type result)
    {
        return typeCoercion.isTypeOnlyCoercion(source, result);
    }

    @Override
    public Optional<Type> getCommonSuperType(Type firstType, Type secondType)
    {
        return typeCoercion.getCommonSuperType(firstType, secondType);
    }

    @Override
    public boolean canCoerce(Type fromType, Type toType)
    {
        return typeCoercion.canCoerce(fromType, toType);
    }

    public void addType(Type type)
    {
        requireNonNull(type, "type is null");
        Type existingType = types.putIfAbsent(type.getTypeSignature(), type);
        checkState(existingType == null || existingType.equals(type), "Type %s is already registered", type);
    }

    public void addParametricType(ParametricType parametricType)
    {
        String name = parametricType.getName().toLowerCase(Locale.ENGLISH);
        checkArgument(!parametricTypes.containsKey(name), "Parametric type already registered: %s", name);
        parametricTypes.putIfAbsent(name, parametricType);
    }

    @Override
    public Collection<ParametricType> getParametricTypes()
    {
        return ImmutableList.copyOf(parametricTypes.values());
    }

    @Override
    public Optional<Type> coerceTypeBase(Type sourceType, String resultTypeBase)
    {
        return typeCoercion.coerceTypeBase(sourceType, resultTypeBase);
    }

    @Override
    public MethodHandle resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
    {
        requireNonNull(functionRegistry, "functionRegistry is null");
        return functionRegistry.getScalarFunctionImplementation(functionRegistry.resolveOperator(operatorType, argumentTypes)).getMethodHandle();
    }
}
