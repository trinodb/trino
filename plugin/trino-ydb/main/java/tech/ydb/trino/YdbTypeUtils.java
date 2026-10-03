package tech.ydb.trino;

import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.sql.Types;
import java.util.Optional;

public final class YdbTypeUtils {

    private YdbTypeUtils() {}

    public static Optional<JdbcTypeHandle> toTypeHandle(Type type) {
        return switch (type) {
            case BooleanType _ ->
                    Optional.of(new JdbcTypeHandle(Types.BOOLEAN, Optional.of("Bool"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case TinyintType _ ->
                    Optional.of(new JdbcTypeHandle(Types.TINYINT, Optional.of("Int8"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case SmallintType _ ->
                    Optional.of(new JdbcTypeHandle(Types.SMALLINT, Optional.of("Int16"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case IntegerType _ ->
                    Optional.of(new JdbcTypeHandle(Types.INTEGER, Optional.of("Int32"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case BigintType _ ->
                    Optional.of(new JdbcTypeHandle(Types.BIGINT, Optional.of("Int64"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case RealType _ ->
                    Optional.of(new JdbcTypeHandle(Types.REAL, Optional.of("Float"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case DoubleType _ ->
                    Optional.of(new JdbcTypeHandle(Types.DOUBLE, Optional.of("Double"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case DateType _ ->
                    Optional.of(new JdbcTypeHandle(Types.DATE, Optional.of("Date32"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case TimestampType timestampType when timestampType.getPrecision() == 3 || timestampType.getPrecision() == 6 ->
                    Optional.of(new JdbcTypeHandle(Types.TIMESTAMP, Optional.of("Timestamp64"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case DecimalType decimalType when decimalType.getPrecision() <= 35 ->
                    Optional.of(new JdbcTypeHandle(Types.DECIMAL, Optional.of("Decimal"), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
            case VarcharType _ ->
                    Optional.of(new JdbcTypeHandle(Types.VARCHAR, Optional.of("Text"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            case VarbinaryType _ ->
                    Optional.of(new JdbcTypeHandle(Types.BINARY, Optional.of("Bytes"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            default ->
                    Optional.empty();
        };
    }
}
