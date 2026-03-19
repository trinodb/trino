package io.trino.plugin.couchbase.translations;

import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.trino.spi.type.*;
import it.unimi.dsi.fastutil.doubles.DoubleAVLTreeSet;
import jakarta.annotation.Nullable;

import java.util.Optional;
import java.util.Set;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;

public final class TrinoToCbType {

    private static final Set<Type> PUSHDOWN_SUPPORTED_PRIMITIVE_TYPES = ImmutableSet.of(
            BOOLEAN,
            TINYINT,
            SMALLINT,
            INTEGER,
            BIGINT,
            REAL,
            DOUBLE,
            DATE,
            TIME_MILLIS,
            TIMESTAMP_MILLIS,
            TIMESTAMP_TZ_MILLIS);

    @Nullable
    public static Object serialize(Type type, Object value) {
        if (value instanceof Optional optional) {
            value = optional.orElse(null);
        }
        if (value == null) {
            return null;
        }
        if (type == DateType.DATE || type.equals(BigintType.BIGINT) || type.equals(DoubleType.DOUBLE)) {
            return value;
        } else if (type instanceof VarcharType) {
            Slice slice = (Slice) value;
            return slice.toStringUtf8();
        } else {
            throw new RuntimeException("Unsupported domain value type: " + type);
        }
    }

    public static boolean isPushdownSupportedType(Type type)
    {
        return type instanceof CharType
                || type instanceof VarcharType
                || type instanceof DecimalType
                || PUSHDOWN_SUPPORTED_PRIMITIVE_TYPES.contains(type);
    }
}
