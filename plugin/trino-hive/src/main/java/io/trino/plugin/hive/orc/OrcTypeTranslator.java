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
package io.trino.plugin.hive.orc;

import io.trino.orc.metadata.OrcType;
import io.trino.orc.metadata.OrcType.OrcTypeKind;
import io.trino.plugin.hive.coercions.BooleanCoercer.BooleanToVarcharCoercer;
import io.trino.plugin.hive.coercions.BooleanCoercer.OrcVarcharToBooleanCoercer;
import io.trino.plugin.hive.coercions.DateCoercer.DateToVarcharCoercer;
import io.trino.plugin.hive.coercions.DateCoercer.VarcharToDateCoercer;
import io.trino.plugin.hive.coercions.IntegerNumberToDoubleCoercer;
import io.trino.plugin.hive.coercions.IntegerNumberToVarcharCoercer;
import io.trino.plugin.hive.coercions.TimestampCoercer.LongTimestampToDateCoercer;
import io.trino.plugin.hive.coercions.TimestampCoercer.LongTimestampToVarcharCoercer;
import io.trino.plugin.hive.coercions.TimestampCoercer.VarcharToLongTimestampCoercer;
import io.trino.plugin.hive.coercions.TimestampCoercer.VarcharToShortTimestampCoercer;
import io.trino.plugin.hive.coercions.TypeCoercer;
import io.trino.plugin.hive.coercions.VarcharToDoubleCoercer;
import io.trino.plugin.hive.coercions.VarcharToFloatCoercer;
import io.trino.plugin.hive.coercions.VarcharToIntegralNumericCoercers.OrcVarcharToIntegralNumericCoercer;
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
import io.trino.spi.type.VarcharType;

import java.util.Optional;

import static io.trino.orc.metadata.OrcType.OrcTypeKind.BOOLEAN;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.BYTE;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.DATE;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.DECIMAL;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.DOUBLE;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.FLOAT;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.INT;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.LONG;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.SHORT;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.STRING;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.TIMESTAMP;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.VARCHAR;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDecimalToVarcharCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createIntegerNumberToDecimalCoercer;
import static io.trino.plugin.hive.coercions.DoubleToVarcharCoercers.createDoubleToVarcharCoercer;
import static io.trino.plugin.hive.coercions.FloatToVarcharCoercers.createFloatToVarcharCoercer;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TinyintType.TINYINT;

public final class OrcTypeTranslator
{
    private OrcTypeTranslator() {}

    public static Optional<TypeCoercer<? extends Type, ? extends Type>> createCoercer(OrcType fromOrcType, Type toTrinoType)
    {
        OrcTypeKind fromOrcTypeKind = fromOrcType.getOrcTypeKind();

        if (fromOrcTypeKind == BOOLEAN) {
            return switch (toTrinoType) {
                case VarcharType varcharType -> Optional.of(new BooleanToVarcharCoercer(varcharType));
                default -> Optional.empty();
            };
        }

        if (fromOrcTypeKind == BYTE) {
            return switch (toTrinoType) {
                case DoubleType ignored -> Optional.of(new IntegerNumberToDoubleCoercer<>(TINYINT));
                case DecimalType decimalType -> Optional.of(createIntegerNumberToDecimalCoercer(TINYINT, decimalType));
                case VarcharType varcharType -> Optional.of(new IntegerNumberToVarcharCoercer<>(TINYINT, varcharType));
                default -> Optional.empty();
            };
        }

        if (fromOrcTypeKind == SHORT) {
            return switch (toTrinoType) {
                case DoubleType ignored -> Optional.of(new IntegerNumberToDoubleCoercer<>(SMALLINT));
                case DecimalType decimalType -> Optional.of(createIntegerNumberToDecimalCoercer(SMALLINT, decimalType));
                case VarcharType varcharType -> Optional.of(new IntegerNumberToVarcharCoercer<>(SMALLINT, varcharType));
                default -> Optional.empty();
            };
        }

        if (fromOrcTypeKind == INT) {
            return switch (toTrinoType) {
                case DoubleType ignored -> Optional.of(new IntegerNumberToDoubleCoercer<>(INTEGER));
                case DecimalType decimalType -> Optional.of(createIntegerNumberToDecimalCoercer(INTEGER, decimalType));
                case VarcharType varcharType -> Optional.of(new IntegerNumberToVarcharCoercer<>(INTEGER, varcharType));
                default -> Optional.empty();
            };
        }

        if (fromOrcTypeKind == LONG) {
            return switch (toTrinoType) {
                case DoubleType ignored -> Optional.of(new IntegerNumberToDoubleCoercer<>(BIGINT));
                case DecimalType decimalType -> Optional.of(createIntegerNumberToDecimalCoercer(BIGINT, decimalType));
                case VarcharType varcharType -> Optional.of(new IntegerNumberToVarcharCoercer<>(BIGINT, varcharType));
                default -> Optional.empty();
            };
        }

        if (fromOrcTypeKind == FLOAT) {
            return switch (toTrinoType) {
                case VarcharType varcharType -> Optional.of(createFloatToVarcharCoercer(varcharType, true));
                default -> Optional.empty();
            };
        }

        if (fromOrcTypeKind == DOUBLE) {
            return switch (toTrinoType) {
                case VarcharType varcharType -> Optional.of(createDoubleToVarcharCoercer(varcharType, true));
                default -> Optional.empty();
            };
        }

        if (fromOrcTypeKind == DECIMAL) {
            DecimalType sourceType = DecimalType.createDecimalType(fromOrcType.getPrecision().orElseThrow(), fromOrcType.getScale().orElseThrow());
            return switch (toTrinoType) {
                case VarcharType varcharType -> Optional.of(createDecimalToVarcharCoercer(sourceType, varcharType));
                default -> Optional.empty();
            };
        }

        if (fromOrcTypeKind == DATE) {
            return switch (toTrinoType) {
                case VarcharType varcharType -> Optional.of(new DateToVarcharCoercer(varcharType));
                default -> Optional.empty();
            };
        }

        if (fromOrcTypeKind == TIMESTAMP) {
            return switch (toTrinoType) {
                case DateType dateType -> Optional.of(new LongTimestampToDateCoercer(TIMESTAMP_NANOS, dateType));
                case VarcharType varcharType -> Optional.of(new LongTimestampToVarcharCoercer(TIMESTAMP_NANOS, varcharType));
                default -> Optional.empty();
            };
        }

        if (isVarcharType(fromOrcTypeKind)) {
            return switch (toTrinoType) {
                case BooleanType ignored -> Optional.of(new OrcVarcharToBooleanCoercer(VarcharType.VARCHAR));
                case TinyintType tinyintType -> Optional.of(new OrcVarcharToIntegralNumericCoercer<>(VarcharType.VARCHAR, tinyintType));
                case SmallintType smallintType -> Optional.of(new OrcVarcharToIntegralNumericCoercer<>(VarcharType.VARCHAR, smallintType));
                case IntegerType integerType -> Optional.of(new OrcVarcharToIntegralNumericCoercer<>(VarcharType.VARCHAR, integerType));
                case BigintType bigintType -> Optional.of(new OrcVarcharToIntegralNumericCoercer<>(VarcharType.VARCHAR, bigintType));
                case RealType ignored -> Optional.of(new VarcharToFloatCoercer(VarcharType.VARCHAR, true));
                case DoubleType ignored -> Optional.of(new VarcharToDoubleCoercer(VarcharType.VARCHAR, true));
                case DateType dateType -> Optional.of(new VarcharToDateCoercer(VarcharType.VARCHAR, dateType));
                case TimestampType timestampType -> Optional.of(timestampType.isShort()
                        ? new VarcharToShortTimestampCoercer(VarcharType.VARCHAR, timestampType)
                        : new VarcharToLongTimestampCoercer(VarcharType.VARCHAR, timestampType));
                default -> Optional.empty();
            };
        }

        return Optional.empty();
    }

    private static boolean isVarcharType(OrcTypeKind orcTypeKind)
    {
        return orcTypeKind == STRING || orcTypeKind == VARCHAR;
    }
}
