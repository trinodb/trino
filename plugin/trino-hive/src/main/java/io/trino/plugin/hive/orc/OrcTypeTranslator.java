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

import com.google.common.collect.ImmutableList;
import io.trino.orc.OrcColumn;
import io.trino.orc.metadata.OrcType;
import io.trino.orc.metadata.OrcType.OrcTypeKind;
import io.trino.plugin.hive.coercions.BooleanCoercer.BooleanToVarcharCoercer;
import io.trino.plugin.hive.coercions.BooleanCoercer.OrcVarcharToBooleanCoercer;
import io.trino.plugin.hive.coercions.CoercionUtils.ListCoercer;
import io.trino.plugin.hive.coercions.CoercionUtils.MapCoercer;
import io.trino.plugin.hive.coercions.CoercionUtils.StructCoercer;
import io.trino.plugin.hive.coercions.DateCoercer.DateToVarcharCoercer;
import io.trino.plugin.hive.coercions.DateCoercer.VarcharToDateCoercer;
import io.trino.plugin.hive.coercions.DoubleToFloatCoercer;
import io.trino.plugin.hive.coercions.FloatToDoubleCoercer;
import io.trino.plugin.hive.coercions.IntegerNumberToDoubleCoercer;
import io.trino.plugin.hive.coercions.IntegerNumberToVarcharCoercer;
import io.trino.plugin.hive.coercions.IntegerNumberUpscaleCoercer;
import io.trino.plugin.hive.coercions.TimestampCoercer.LongTimestampToDateCoercer;
import io.trino.plugin.hive.coercions.TimestampCoercer.LongTimestampToVarcharCoercer;
import io.trino.plugin.hive.coercions.TimestampCoercer.VarcharToLongTimestampCoercer;
import io.trino.plugin.hive.coercions.TimestampCoercer.VarcharToShortTimestampCoercer;
import io.trino.plugin.hive.coercions.TypeCoercer;
import io.trino.plugin.hive.coercions.VarcharToDoubleCoercer;
import io.trino.plugin.hive.coercions.VarcharToFloatCoercer;
import io.trino.plugin.hive.coercions.VarcharToIntegralNumericCoercers.OrcVarcharToIntegralNumericCoercer;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.BINARY;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.BOOLEAN;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.BYTE;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.DATE;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.DECIMAL;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.DOUBLE;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.FLOAT;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.INT;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.LIST;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.LONG;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.MAP;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.SHORT;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.STRING;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.STRUCT;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.TIMESTAMP;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.VARCHAR;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDecimalToDecimalCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDecimalToDoubleCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDecimalToInteger;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDecimalToRealCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDecimalToVarcharCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDoubleToDecimalCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createIntegerNumberToDecimalCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createRealToDecimalCoercer;
import static io.trino.plugin.hive.coercions.DoubleToVarcharCoercers.createDoubleToVarcharCoercer;
import static io.trino.plugin.hive.coercions.FloatToVarcharCoercers.createFloatToVarcharCoercer;
import static io.trino.plugin.hive.coercions.VarbinaryToVarcharCoercers.createVarbinaryToVarcharCoercer;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TinyintType.TINYINT;

public final class OrcTypeTranslator
{
    private OrcTypeTranslator() {}

    public static Optional<TypeCoercer<? extends Type, ? extends Type>> createCoercer(OrcType fromOrcType, List<OrcColumn> nestedColumns, Type toTrinoType)
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
                case SmallintType smallintType -> Optional.of(new IntegerNumberUpscaleCoercer<>(TINYINT, smallintType));
                case IntegerType integerType -> Optional.of(new IntegerNumberUpscaleCoercer<>(TINYINT, integerType));
                case BigintType bigintType -> Optional.of(new IntegerNumberUpscaleCoercer<>(TINYINT, bigintType));
                case DoubleType ignored -> Optional.of(new IntegerNumberToDoubleCoercer<>(TINYINT));
                case DecimalType decimalType -> Optional.of(createIntegerNumberToDecimalCoercer(TINYINT, decimalType));
                case VarcharType varcharType -> Optional.of(new IntegerNumberToVarcharCoercer<>(TINYINT, varcharType));
                default -> Optional.empty();
            };
        }

        if (fromOrcTypeKind == SHORT) {
            return switch (toTrinoType) {
                case IntegerType integerType -> Optional.of(new IntegerNumberUpscaleCoercer<>(SMALLINT, integerType));
                case BigintType bigintType -> Optional.of(new IntegerNumberUpscaleCoercer<>(SMALLINT, bigintType));
                case DoubleType ignored -> Optional.of(new IntegerNumberToDoubleCoercer<>(SMALLINT));
                case DecimalType decimalType -> Optional.of(createIntegerNumberToDecimalCoercer(SMALLINT, decimalType));
                case VarcharType varcharType -> Optional.of(new IntegerNumberToVarcharCoercer<>(SMALLINT, varcharType));
                default -> Optional.empty();
            };
        }

        if (fromOrcTypeKind == INT) {
            return switch (toTrinoType) {
                case BigintType bigintType -> Optional.of(new IntegerNumberUpscaleCoercer<>(INTEGER, bigintType));
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
                case DoubleType ignored -> Optional.of(new FloatToDoubleCoercer());
                case DecimalType decimalType -> Optional.of(createRealToDecimalCoercer(decimalType));
                case VarcharType varcharType -> Optional.of(createFloatToVarcharCoercer(varcharType, true));
                default -> Optional.empty();
            };
        }

        if (fromOrcTypeKind == DOUBLE) {
            return switch (toTrinoType) {
                case RealType ignored -> Optional.of(new DoubleToFloatCoercer());
                case DecimalType decimalType -> Optional.of(createDoubleToDecimalCoercer(decimalType));
                case VarcharType varcharType -> Optional.of(createDoubleToVarcharCoercer(varcharType, true));
                default -> Optional.empty();
            };
        }

        if (fromOrcTypeKind == DECIMAL) {
            DecimalType sourceType = DecimalType.createDecimalType(fromOrcType.getPrecision().orElseThrow(), fromOrcType.getScale().orElseThrow());
            return switch (toTrinoType) {
                case TinyintType tinyintType -> Optional.of(createDecimalToInteger(sourceType, tinyintType));
                case SmallintType smallintType -> Optional.of(createDecimalToInteger(sourceType, smallintType));
                case IntegerType integerType -> Optional.of(createDecimalToInteger(sourceType, integerType));
                case BigintType bigintType -> Optional.of(createDecimalToInteger(sourceType, bigintType));
                case RealType ignored -> Optional.of(createDecimalToRealCoercer(sourceType));
                case DoubleType ignored -> Optional.of(createDecimalToDoubleCoercer(sourceType));
                case DecimalType decimalType -> Optional.of(createDecimalToDecimalCoercer(sourceType, decimalType));
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

        if (fromOrcType.getOrcTypeKind() == STRUCT && toTrinoType instanceof RowType rowType) {
            ImmutableList.Builder<Optional<TypeCoercer<? extends Type, ? extends Type>>> coercersBuilder = ImmutableList.builder();
            ImmutableList.Builder<RowType.Field> fromField = ImmutableList.builder();
            ImmutableList.Builder<RowType.Field> toField = ImmutableList.builder();

            List<String> fromStructFieldName = fromOrcType.getFieldNames();
            List<String> toStructFieldNames = rowType.getFields().stream()
                    .map(RowType.Field::getName)
                    .map(Optional::get)
                    .collect(toImmutableList());

            for (int i = 0; i < toStructFieldNames.size(); i++) {
                if (i >= fromStructFieldName.size()) {
                    toField.add(new RowType.Field(
                            Optional.of(toStructFieldNames.get(i)),
                            rowType.getFields().get(i).getType()));
                    coercersBuilder.add(Optional.empty());
                }
                else {
                    // If the field names are not in correct order don't apply coercion
                    if (!fromStructFieldName.get(i).equalsIgnoreCase(toStructFieldNames.get(i))) {
                        return Optional.empty();
                    }
                    Optional<TypeCoercer<? extends Type, ? extends Type>> coercer = createCoercer(
                            nestedColumns.get(i).getColumnType(),
                            nestedColumns.get(i).getNestedColumns(),
                            rowType.getFields().get(i).getType());
                    coercersBuilder.add(coercer);

                    Type rowFieldType = rowType.getFields().get(i).getType();
                    fromField.add(new RowType.Field(
                            Optional.of(fromStructFieldName.get(i)),
                            coercer.map(TypeCoercer::getFromType).orElse(rowFieldType)));
                    toField.add(new RowType.Field(
                            Optional.of(toStructFieldNames.get(i)),
                            coercer.map(TypeCoercer::getToType).orElse(rowFieldType)));
                }
            }

            List<Optional<TypeCoercer<? extends Type, ? extends Type>>> coercers = coercersBuilder.build();

            if (coercers.stream().anyMatch(Optional::isPresent)) {
                return Optional.of(new StructCoercer(RowType.from(fromField.build()), RowType.from(toField.build()), coercers));
            }

            return Optional.empty();
        }

        if (fromOrcType.getOrcTypeKind() == LIST && toTrinoType instanceof ArrayType arrayType) {
            return createCoercer(getOnlyElement(nestedColumns).getColumnType(), getOnlyElement(nestedColumns).getNestedColumns(), arrayType.getElementType())
                    .map(elementCoercer -> new ListCoercer(new ArrayType(elementCoercer.getFromType()), new ArrayType(elementCoercer.getToType()), elementCoercer));
        }

        if (fromOrcType.getOrcTypeKind() == MAP && toTrinoType instanceof MapType mapType) {
            Optional<TypeCoercer<? extends Type, ? extends Type>> keyCoercer = createCoercer(nestedColumns.get(0).getColumnType(), nestedColumns.get(0).getNestedColumns(), mapType.getKeyType());
            Optional<TypeCoercer<? extends Type, ? extends Type>> valueCoercer = createCoercer(nestedColumns.get(1).getColumnType(), nestedColumns.get(1).getNestedColumns(), mapType.getValueType());
            TypeOperators typeOperators = new TypeOperators();
            MapType fromType = new MapType(
                    keyCoercer.map(TypeCoercer::getFromType).orElseGet(mapType::getKeyType),
                    valueCoercer.map(TypeCoercer::getFromType).orElseGet(mapType::getValueType),
                    typeOperators);

            MapType toType = new MapType(
                    keyCoercer.map(TypeCoercer::getToType).orElseGet(mapType::getKeyType),
                    valueCoercer.map(TypeCoercer::getToType).orElseGet(mapType::getKeyType),
                    typeOperators);

            if (keyCoercer.isPresent() || valueCoercer.isPresent()) {
                return Optional.of(new MapCoercer(fromType, toType, keyCoercer, valueCoercer));
            }
            return Optional.empty();
        }

        if (fromOrcTypeKind == BINARY && toTrinoType instanceof VarcharType varcharType) {
            return Optional.of(createVarbinaryToVarcharCoercer(varcharType, ORC));
        }
        return Optional.empty();
    }

    private static boolean isVarcharType(OrcTypeKind orcTypeKind)
    {
        return orcTypeKind == STRING || orcTypeKind == VARCHAR;
    }
}
