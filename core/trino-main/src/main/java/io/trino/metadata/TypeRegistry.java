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

import com.google.common.base.Joiner;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.trino.FeaturesConfig;
import io.trino.collect.cache.NonEvictableCache;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.ParametricType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeNotFoundException;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeParameter;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.sql.parser.SqlParser;
import io.trino.type.CharParametricType;
import io.trino.type.DecimalParametricType;
import io.trino.type.Re2JRegexpType;
import io.trino.type.VarcharParametricType;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.collect.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_FIRST;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.XX_HASH_64;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.P4HyperLogLogType.P4_HYPER_LOG_LOG;
import static io.trino.spi.type.QuantileDigestParametricType.QDIGEST;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeParametricType.TIME;
import static io.trino.spi.type.TimeWithTimeZoneParametricType.TIME_WITH_TIME_ZONE;
import static io.trino.spi.type.TimestampParametricType.TIMESTAMP;
import static io.trino.spi.type.TimestampWithTimeZoneParametricType.TIMESTAMP_WITH_TIME_ZONE;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toTypeSignature;
import static io.trino.type.ArrayParametricType.ARRAY;
import static io.trino.type.CodePointsType.CODE_POINTS;
import static io.trino.type.ColorType.COLOR;
import static io.trino.type.FunctionParametricType.FUNCTION;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.trino.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static io.trino.type.IpAddressType.IPADDRESS;
import static io.trino.type.JoniRegexpType.JONI_REGEXP;
import static io.trino.type.Json2016Type.JSON_2016;
import static io.trino.type.JsonPathType.JSON_PATH;
import static io.trino.type.JsonType.JSON;
import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static io.trino.type.MapParametricType.MAP;
import static io.trino.type.RowParametricType.ROW;
import static io.trino.type.TDigestType.TDIGEST;
import static io.trino.type.UnknownType.UNKNOWN;
import static io.trino.type.setdigest.SetDigestType.SET_DIGEST;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public final class TypeRegistry
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    private final ConcurrentMap<TypeSignature, Type> types = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ParametricType> parametricTypes = new ConcurrentHashMap<>();

    private final NonEvictableCache<TypeSignature, Type> parametricTypeCache;
    private final TypeManager typeManager;
    private final TypeOperators typeOperators;

    @Inject
    public TypeRegistry(TypeOperators typeOperators, FeaturesConfig featuresConfig)
    {
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
        requireNonNull(featuresConfig, "featuresConfig is null");

        // Manually register UNKNOWN type without a verifyTypeClass call since it is a special type that cannot be used by functions
        this.types.put(UNKNOWN.getTypeSignature(), UNKNOWN);

        // always add the built-in types; Trino will not function without these
        addType(BOOLEAN);
        addType(BIGINT);
        addType(INTEGER);
        addType("int", INTEGER);
        addType(SMALLINT);
        addType(TINYINT);
        addType(DOUBLE);
        addType(REAL);
        addType(VARBINARY);
        addType(DATE);
        addType(INTERVAL_YEAR_MONTH);
        addType(INTERVAL_DAY_TIME);
        addType(HYPER_LOG_LOG);
        addType(SET_DIGEST);
        addType(P4_HYPER_LOG_LOG);
        addType(JONI_REGEXP);
        addType(new Re2JRegexpType(featuresConfig.getRe2JDfaStatesLimit(), featuresConfig.getRe2JDfaRetries()));
        addType(LIKE_PATTERN);
        addType(JSON_PATH);
        addType(JSON_2016);
        addType(COLOR);
        addType(JSON);
        addType(CODE_POINTS);
        addType(IPADDRESS);
        addType(UUID);
        addType(TDIGEST);
        addParametricType(VarcharParametricType.VARCHAR);
        addParametricType(CharParametricType.CHAR);
        addParametricType(DecimalParametricType.DECIMAL);
        addParametricType(ROW);
        addParametricType(ARRAY);
        addParametricType(MAP);
        addParametricType(FUNCTION);
        addParametricType(QDIGEST);
        addParametricType(TIMESTAMP);
        addParametricType(TIMESTAMP_WITH_TIME_ZONE);
        addParametricType(TIME);
        addParametricType(TIME_WITH_TIME_ZONE);

        parametricTypeCache = buildNonEvictableCache(CacheBuilder.newBuilder().maximumSize(1000));

        typeManager = new InternalTypeManager(this, typeOperators);

        verifyTypes();
    }

    public Type getType(TypeSignature signature)
    {
        Type type = types.get(signature);
        if (type == null) {
            try {
                return uncheckedCacheGet(parametricTypeCache, signature, () -> instantiateParametricType(signature));
            }
            catch (UncheckedExecutionException e) {
                throwIfUnchecked(e.getCause());
                throw new RuntimeException(e.getCause());
            }
        }
        return type;
    }

    public Type getType(TypeId id)
    {
        // TODO: ID should be encoded in a more canonical form than SQL
        return fromSqlType(id.getId());
    }

    public Type fromSqlType(String sqlType)
    {
        return getType(toTypeSignature(SQL_PARSER.createType(sqlType)));
    }

    private Type instantiateParametricType(TypeSignature signature)
    {
        List<TypeParameter> parameters = new ArrayList<>();

        for (TypeSignatureParameter parameter : signature.getParameters()) {
            TypeParameter typeParameter = TypeParameter.of(parameter, typeManager);
            parameters.add(typeParameter);
        }

        ParametricType parametricType = parametricTypes.get(signature.getBase().toLowerCase(Locale.ENGLISH));
        if (parametricType == null) {
            throw new TypeNotFoundException(signature);
        }

        Type instantiatedType;
        try {
            instantiatedType = parametricType.createType(typeManager, parameters);
        }
        catch (IllegalArgumentException e) {
            throw new TypeNotFoundException(signature, e);
        }

        // TODO: reimplement this check? Currently "varchar(Integer.MAX_VALUE)" fails with "varchar"
        //checkState(instantiatedType.equalsSignature(signature), "Instantiated parametric type name (%s) does not match expected name (%s)", instantiatedType, signature);
        return instantiatedType;
    }

    public Collection<Type> getTypes()
    {
        return ImmutableList.copyOf(types.values());
    }

    public Collection<ParametricType> getParametricTypes()
    {
        return ImmutableList.copyOf(parametricTypes.values());
    }

    public void addType(Type type)
    {
        requireNonNull(type, "type is null");
        Type existingType = types.putIfAbsent(type.getTypeSignature(), type);
        checkState(existingType == null || existingType.equals(type), "Type %s is already registered", type);
    }

    public void addType(String alias, Type type)
    {
        requireNonNull(alias, "alias is null");
        requireNonNull(type, "type is null");

        Type existingType = types.putIfAbsent(new TypeSignature(alias), type);
        checkState(existingType == null || existingType.equals(type), "Alias %s is already mapped to %s", alias, type);
    }

    public void addParametricType(ParametricType parametricType)
    {
        String name = parametricType.getName().toLowerCase(Locale.ENGLISH);
        checkArgument(!parametricTypes.containsKey(name), "Parametric type already registered: %s", name);
        parametricTypes.putIfAbsent(name, parametricType);
    }

    public TypeOperators getTypeOperators()
    {
        return typeOperators;
    }

    public void verifyTypes()
    {
        Set<Type> missingOperatorDeclaration = new HashSet<>();
        Multimap<Type, OperatorType> missingOperators = HashMultimap.create();
        for (Type type : ImmutableList.copyOf(types.values())) {
            if (type.getTypeOperatorDeclaration(typeOperators) == null) {
                missingOperatorDeclaration.add(type);
                continue;
            }
            if (type.isComparable()) {
                if (!hasEqualMethod(type)) {
                    missingOperators.put(type, EQUAL);
                }
                if (!hasHashCodeMethod(type)) {
                    missingOperators.put(type, HASH_CODE);
                }
                if (!hasXxHash64Method(type)) {
                    missingOperators.put(type, XX_HASH_64);
                }
                if (!hasDistinctFromMethod(type)) {
                    missingOperators.put(type, IS_DISTINCT_FROM);
                }
                if (!hasIndeterminateMethod(type)) {
                    missingOperators.put(type, INDETERMINATE);
                }
            }
            if (type.isOrderable()) {
                if (!hasComparisonUnorderedLastMethod(type)) {
                    missingOperators.put(type, COMPARISON_UNORDERED_LAST);
                }
                if (!hasComparisonUnorderedFirstMethod(type)) {
                    missingOperators.put(type, COMPARISON_UNORDERED_FIRST);
                }
                if (!hasLessThanMethod(type)) {
                    missingOperators.put(type, LESS_THAN);
                }
                if (!hasLessThanOrEqualMethod(type)) {
                    missingOperators.put(type, LESS_THAN_OR_EQUAL);
                }
            }
        }
        // TODO: verify the parametric types too
        if (!missingOperators.isEmpty()) {
            List<String> messages = new ArrayList<>();
            for (Type type : missingOperatorDeclaration) {
                messages.add(format("%s types operators is null", type));
            }
            for (Type type : missingOperators.keySet()) {
                messages.add(format("%s missing for %s", missingOperators.get(type), type));
            }
            throw new IllegalStateException(Joiner.on(", ").join(messages));
        }
    }

    private boolean hasEqualMethod(Type type)
    {
        try {
            typeOperators.getEqualOperator(type, simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL));
            return true;
        }
        catch (RuntimeException e) {
            return false;
        }
    }

    private boolean hasHashCodeMethod(Type type)
    {
        try {
            typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL));
            return true;
        }
        catch (RuntimeException e) {
            return false;
        }
    }

    private boolean hasXxHash64Method(Type type)
    {
        try {
            typeOperators.getXxHash64Operator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL));
            return true;
        }
        catch (RuntimeException e) {
            return false;
        }
    }

    private boolean hasDistinctFromMethod(Type type)
    {
        try {
            typeOperators.getDistinctFromOperator(type, simpleConvention(FAIL_ON_NULL, BOXED_NULLABLE, BOXED_NULLABLE));
            return true;
        }
        catch (RuntimeException e) {
            return false;
        }
    }

    private boolean hasIndeterminateMethod(Type type)
    {
        try {
            typeOperators.getIndeterminateOperator(type, simpleConvention(FAIL_ON_NULL, BOXED_NULLABLE));
            return true;
        }
        catch (RuntimeException e) {
            return false;
        }
    }

    private boolean hasComparisonUnorderedLastMethod(Type type)
    {
        try {
            typeOperators.getComparisonUnorderedLastOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
            return true;
        }
        catch (UnsupportedOperationException e) {
            return false;
        }
    }

    private boolean hasComparisonUnorderedFirstMethod(Type type)
    {
        try {
            typeOperators.getComparisonUnorderedFirstOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
            return true;
        }
        catch (UnsupportedOperationException e) {
            return false;
        }
    }

    private boolean hasLessThanMethod(Type type)
    {
        try {
            typeOperators.getLessThanOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
            return true;
        }
        catch (UnsupportedOperationException e) {
            return false;
        }
    }

    private boolean hasLessThanOrEqualMethod(Type type)
    {
        try {
            typeOperators.getLessThanOrEqualOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
            return true;
        }
        catch (UnsupportedOperationException e) {
            return false;
        }
    }

    private static final class InternalTypeManager
            implements TypeManager
    {
        private final TypeRegistry typeRegistry;
        private final TypeOperators typeOperators;

        @Inject
        public InternalTypeManager(TypeRegistry typeRegistry, TypeOperators typeOperators)
        {
            this.typeRegistry = requireNonNull(typeRegistry, "typeRegistry is null");
            this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
        }

        @Override
        public Type getType(TypeSignature signature)
        {
            return typeRegistry.getType(signature);
        }

        @Override
        public Type fromSqlType(String type)
        {
            return typeRegistry.fromSqlType(type);
        }

        @Override
        public Type getType(TypeId id)
        {
            return typeRegistry.getType(id);
        }

        @Override
        public TypeOperators getTypeOperators()
        {
            return typeOperators;
        }
    }
}
