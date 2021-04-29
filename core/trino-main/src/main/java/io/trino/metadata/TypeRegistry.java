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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.trino.spi.type.ParametricType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeNotFoundException;
import io.trino.spi.type.TypeParameter;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.sql.analyzer.FeaturesConfig;
import io.trino.sql.parser.SqlParser;
import io.trino.type.CharParametricType;
import io.trino.type.DecimalParametricType;
import io.trino.type.Re2JRegexpType;
import io.trino.type.VarcharParametricType;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
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
import static io.trino.type.JsonPathType.JSON_PATH;
import static io.trino.type.JsonType.JSON;
import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static io.trino.type.MapParametricType.MAP;
import static io.trino.type.RowParametricType.ROW;
import static io.trino.type.TDigestType.TDIGEST;
import static io.trino.type.UnknownType.UNKNOWN;
import static io.trino.type.setdigest.SetDigestType.SET_DIGEST;
import static java.util.Objects.requireNonNull;

@ThreadSafe
final class TypeRegistry
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    private final ConcurrentMap<TypeSignature, Type> types = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ParametricType> parametricTypes = new ConcurrentHashMap<>();

    private final Cache<TypeSignature, Type> parametricTypeCache;

    public TypeRegistry(FeaturesConfig featuresConfig)
    {
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

        parametricTypeCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .build();
    }

    public Type getType(TypeManager typeManager, TypeSignature signature)
    {
        Type type = types.get(signature);
        if (type == null) {
            try {
                return parametricTypeCache.get(signature, () -> instantiateParametricType(typeManager, signature));
            }
            catch (ExecutionException | UncheckedExecutionException e) {
                throwIfUnchecked(e.getCause());
                throw new RuntimeException(e.getCause());
            }
        }
        return type;
    }

    public Type getType(TypeManager typeManager, TypeId id)
    {
        // TODO: ID should be encoded in a more canonical form than SQL
        return fromSqlType(typeManager, id.getId());
    }

    public Type fromSqlType(TypeManager typeManager, String sqlType)
    {
        return getType(typeManager, toTypeSignature(SQL_PARSER.createType(sqlType)));
    }

    private Type instantiateParametricType(TypeManager typeManager, TypeSignature signature)
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
}
