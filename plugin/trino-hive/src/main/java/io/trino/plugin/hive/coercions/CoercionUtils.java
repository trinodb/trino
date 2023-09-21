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
package io.trino.plugin.hive.coercions;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.coercions.TimestampCoercer.VarcharToLongTimestampCoercer;
import io.trino.plugin.hive.coercions.TimestampCoercer.VarcharToShortTimestampCoercer;
import io.trino.plugin.hive.type.Category;
import io.trino.plugin.hive.type.ListTypeInfo;
import io.trino.plugin.hive.type.MapTypeInfo;
import io.trino.plugin.hive.type.StructTypeInfo;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.ColumnarMap;
import io.trino.spi.block.ColumnarRow;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.hive.HiveType.HIVE_BYTE;
import static io.trino.plugin.hive.HiveType.HIVE_DOUBLE;
import static io.trino.plugin.hive.HiveType.HIVE_FLOAT;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.plugin.hive.HiveType.HIVE_LONG;
import static io.trino.plugin.hive.HiveType.HIVE_SHORT;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDecimalToDecimalCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDecimalToDoubleCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDecimalToRealCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDecimalToVarcharCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDoubleToDecimalCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createRealToDecimalCoercer;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.block.ColumnarArray.toColumnarArray;
import static io.trino.spi.block.ColumnarMap.toColumnarMap;
import static io.trino.spi.block.ColumnarRow.toColumnarRow;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class CoercionUtils
{
    private CoercionUtils() {}

    public static Type createTypeFromCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType, HiveTimestampPrecision timestampPrecision)
    {
        return createCoercer(typeManager, fromHiveType, toHiveType, timestampPrecision)
                .map(TypeCoercer::getFromType)
                .orElseGet(() -> fromHiveType.getType(typeManager, timestampPrecision));
    }

    public static Optional<TypeCoercer<? extends Type, ? extends Type>> createCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType, HiveTimestampPrecision timestampPrecision)
    {
        if (fromHiveType.equals(toHiveType)) {
            return Optional.empty();
        }

        Type fromType = fromHiveType.getType(typeManager, timestampPrecision);
        Type toType = toHiveType.getType(typeManager, timestampPrecision);

        if (toType instanceof VarcharType toVarcharType && (fromHiveType.equals(HIVE_BYTE) || fromHiveType.equals(HIVE_SHORT) || fromHiveType.equals(HIVE_INT) || fromHiveType.equals(HIVE_LONG))) {
            return Optional.of(new IntegerNumberToVarcharCoercer<>(fromType, toVarcharType));
        }
        if (fromType instanceof VarcharType fromVarcharType && (toHiveType.equals(HIVE_BYTE) || toHiveType.equals(HIVE_SHORT) || toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG))) {
            return Optional.of(new VarcharToIntegerNumberCoercer<>(fromVarcharType, toType));
        }
        if (fromType instanceof VarcharType varcharType && toType instanceof TimestampType timestampType) {
            if (timestampType.isShort()) {
                return Optional.of(new VarcharToShortTimestampCoercer(varcharType, timestampType));
            }
            return Optional.of(new VarcharToLongTimestampCoercer(varcharType, timestampType));
        }
        if (fromType instanceof VarcharType fromVarcharType && toType instanceof VarcharType toVarcharType) {
            if (narrowerThan(toVarcharType, fromVarcharType)) {
                return Optional.of(new VarcharCoercer(fromVarcharType, toVarcharType));
            }
            return Optional.empty();
        }
        if (fromType instanceof CharType fromCharType && toType instanceof CharType toCharType) {
            if (narrowerThan(toCharType, fromCharType)) {
                return Optional.of(new CharCoercer(fromCharType, toCharType));
            }
            return Optional.empty();
        }
        if (fromHiveType.equals(HIVE_BYTE) && (toHiveType.equals(HIVE_SHORT) || toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG))) {
            return Optional.of(new IntegerNumberUpscaleCoercer<>(fromType, toType));
        }
        if (fromHiveType.equals(HIVE_SHORT) && (toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG))) {
            return Optional.of(new IntegerNumberUpscaleCoercer<>(fromType, toType));
        }
        if (fromHiveType.equals(HIVE_INT) && toHiveType.equals(HIVE_LONG)) {
            return Optional.of(new IntegerNumberUpscaleCoercer<>(fromType, toType));
        }
        if (fromHiveType.equals(HIVE_FLOAT) && toHiveType.equals(HIVE_DOUBLE)) {
            return Optional.of(new FloatToDoubleCoercer());
        }
        if (fromHiveType.equals(HIVE_DOUBLE) && toHiveType.equals(HIVE_FLOAT)) {
            return Optional.of(new DoubleToFloatCoercer());
        }
        if (fromType instanceof DecimalType fromDecimalType && toType instanceof DecimalType toDecimalType) {
            return Optional.of(createDecimalToDecimalCoercer(fromDecimalType, toDecimalType));
        }
        if (fromType instanceof DecimalType fromDecimalType && toType == DOUBLE) {
            return Optional.of(createDecimalToDoubleCoercer(fromDecimalType));
        }
        if (fromType instanceof DecimalType fromDecimalType && toType == REAL) {
            return Optional.of(createDecimalToRealCoercer(fromDecimalType));
        }
        if (fromType instanceof DecimalType fromDecimalType && toType instanceof VarcharType toVarcharType) {
            return Optional.of(createDecimalToVarcharCoercer(fromDecimalType, toVarcharType));
        }
        if (fromType == DOUBLE && toType instanceof DecimalType toDecimalType) {
            return Optional.of(createDoubleToDecimalCoercer(toDecimalType));
        }
        if (fromType == REAL && toType instanceof DecimalType toDecimalType) {
            return Optional.of(createRealToDecimalCoercer(toDecimalType));
        }
        if (fromType instanceof TimestampType && toType instanceof VarcharType varcharType) {
            return Optional.of(new TimestampCoercer.LongTimestampToVarcharCoercer(TIMESTAMP_NANOS, varcharType));
        }
        if ((fromType instanceof ArrayType) && (toType instanceof ArrayType)) {
            return createCoercerForList(
                    typeManager,
                    (ListTypeInfo) fromHiveType.getTypeInfo(),
                    (ListTypeInfo) toHiveType.getTypeInfo(),
                    timestampPrecision);
        }
        if ((fromType instanceof MapType) && (toType instanceof MapType)) {
            return createCoercerForMap(
                    typeManager,
                    (MapTypeInfo) fromHiveType.getTypeInfo(),
                    (MapTypeInfo) toHiveType.getTypeInfo(),
                    timestampPrecision);
        }
        if ((fromType instanceof RowType) && (toType instanceof RowType)) {
            HiveType fromHiveTypeStruct = (fromHiveType.getCategory() == Category.UNION) ? HiveType.toHiveType(fromType) : fromHiveType;
            HiveType toHiveTypeStruct = (toHiveType.getCategory() == Category.UNION) ? HiveType.toHiveType(toType) : toHiveType;

            return createCoercerForStruct(
                    typeManager,
                    (StructTypeInfo) fromHiveTypeStruct.getTypeInfo(),
                    (StructTypeInfo) toHiveTypeStruct.getTypeInfo(),
                    timestampPrecision);
        }

        throw new TrinoException(NOT_SUPPORTED, format("Unsupported coercion from %s to %s", fromHiveType, toHiveType));
    }

    public static boolean narrowerThan(VarcharType first, VarcharType second)
    {
        requireNonNull(first, "first is null");
        requireNonNull(second, "second is null");
        if (first.isUnbounded() || second.isUnbounded()) {
            return !first.isUnbounded();
        }
        return first.getBoundedLength() < second.getBoundedLength();
    }

    public static boolean narrowerThan(CharType first, CharType second)
    {
        requireNonNull(first, "first is null");
        requireNonNull(second, "second is null");
        return first.getLength() < second.getLength();
    }

    private static Optional<TypeCoercer<? extends Type, ? extends Type>> createCoercerForList(
            TypeManager typeManager,
            ListTypeInfo fromListTypeInfo,
            ListTypeInfo toListTypeInfo,
            HiveTimestampPrecision timestampPrecision)
    {
        HiveType fromElementHiveType = HiveType.valueOf(fromListTypeInfo.getListElementTypeInfo().getTypeName());
        HiveType toElementHiveType = HiveType.valueOf(toListTypeInfo.getListElementTypeInfo().getTypeName());

        return createCoercer(typeManager, fromElementHiveType, toElementHiveType, timestampPrecision)
                .map(elementCoercer -> new ListCoercer(new ArrayType(elementCoercer.getFromType()), new ArrayType(elementCoercer.getToType()), elementCoercer));
    }

    private static Optional<TypeCoercer<? extends Type, ? extends Type>> createCoercerForMap(
            TypeManager typeManager,
            MapTypeInfo fromMapTypeInfo,
            MapTypeInfo toMapTypeInfo,
            HiveTimestampPrecision timestampPrecision)
    {
        HiveType fromKeyHiveType = HiveType.valueOf(fromMapTypeInfo.getMapKeyTypeInfo().getTypeName());
        HiveType fromValueHiveType = HiveType.valueOf(fromMapTypeInfo.getMapValueTypeInfo().getTypeName());
        HiveType toKeyHiveType = HiveType.valueOf(toMapTypeInfo.getMapKeyTypeInfo().getTypeName());
        HiveType toValueHiveType = HiveType.valueOf(toMapTypeInfo.getMapValueTypeInfo().getTypeName());
        Optional<TypeCoercer<? extends Type, ? extends Type>> keyCoercer = createCoercer(typeManager, fromKeyHiveType, toKeyHiveType, timestampPrecision);
        Optional<TypeCoercer<? extends Type, ? extends Type>> valueCoercer = createCoercer(typeManager, fromValueHiveType, toValueHiveType, timestampPrecision);
        MapType fromType = new MapType(
                keyCoercer.map(TypeCoercer::getFromType).orElseGet(() -> fromKeyHiveType.getType(typeManager, timestampPrecision)),
                valueCoercer.map(TypeCoercer::getFromType).orElseGet(() -> fromValueHiveType.getType(typeManager, timestampPrecision)),
                typeManager.getTypeOperators());

        MapType toType = new MapType(
                keyCoercer.map(TypeCoercer::getToType).orElseGet(() -> toKeyHiveType.getType(typeManager, timestampPrecision)),
                valueCoercer.map(TypeCoercer::getToType).orElseGet(() -> toValueHiveType.getType(typeManager, timestampPrecision)),
                typeManager.getTypeOperators());

        return Optional.of(new MapCoercer(fromType, toType, keyCoercer, valueCoercer));
    }

    private static Optional<TypeCoercer<? extends Type, ? extends Type>> createCoercerForStruct(
            TypeManager typeManager,
            StructTypeInfo fromStructTypeInfo,
            StructTypeInfo toStructTypeInfo,
            HiveTimestampPrecision timestampPrecision)
    {
        ImmutableList.Builder<Optional<TypeCoercer<? extends Type, ? extends Type>>> coercers = ImmutableList.builder();
        ImmutableList.Builder<Field> fromField = ImmutableList.builder();
        ImmutableList.Builder<Field> toField = ImmutableList.builder();

        List<String> fromStructFieldName = fromStructTypeInfo.getAllStructFieldNames();
        List<String> toStructFieldNames = toStructTypeInfo.getAllStructFieldNames();

        for (int i = 0; i < toStructFieldNames.size(); i++) {
            HiveType toStructFieldType = HiveType.valueOf(toStructTypeInfo.getAllStructFieldTypeInfos().get(i).getTypeName());
            if (i >= fromStructFieldName.size()) {
                toField.add(new Field(
                        Optional.of(toStructFieldNames.get(i)),
                        toStructFieldType.getType(typeManager, timestampPrecision)));
                coercers.add(Optional.empty());
            }
            else {
                HiveType fromStructFieldType = HiveType.valueOf(fromStructTypeInfo.getAllStructFieldTypeInfos().get(i).getTypeName());

                Optional<TypeCoercer<? extends Type, ? extends Type>> coercer = createCoercer(typeManager, fromStructFieldType, toStructFieldType, timestampPrecision);

                fromField.add(new Field(
                        Optional.of(fromStructFieldName.get(i)),
                        coercer.map(TypeCoercer::getFromType).orElseGet(() -> fromStructFieldType.getType(typeManager, timestampPrecision))));
                toField.add(new Field(
                        Optional.of(toStructFieldNames.get(i)),
                        coercer.map(TypeCoercer::getToType).orElseGet(() -> toStructFieldType.getType(typeManager, timestampPrecision))));

                coercers.add(coercer);
            }
        }

        return Optional.of(new StructCoercer(RowType.from(fromField.build()), RowType.from(toField.build()), coercers.build()));
    }

    private static class ListCoercer
            extends TypeCoercer<ArrayType, ArrayType>
    {
        private final TypeCoercer<? extends Type, ? extends Type> elementCoercer;

        public ListCoercer(ArrayType fromType, ArrayType toType, TypeCoercer<? extends Type, ? extends Type> elementCoercer)
        {
            super(fromType, toType);
            this.elementCoercer = requireNonNull(elementCoercer, "elementCoercer is null");
        }

        @Override
        public Block apply(Block block)
        {
            ColumnarArray arrayBlock = toColumnarArray(block);
            Block elementsBlock = elementCoercer.apply(arrayBlock.getElementsBlock());
            boolean[] valueIsNull = new boolean[arrayBlock.getPositionCount()];
            int[] offsets = new int[arrayBlock.getPositionCount() + 1];
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                valueIsNull[i] = arrayBlock.isNull(i);
                offsets[i + 1] = offsets[i] + arrayBlock.getLength(i);
            }
            return ArrayBlock.fromElementBlock(arrayBlock.getPositionCount(), Optional.of(valueIsNull), offsets, elementsBlock);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            throw new UnsupportedOperationException("Not supported");
        }
    }

    private static class MapCoercer
            extends TypeCoercer<MapType, MapType>
    {
        private final Optional<TypeCoercer<? extends Type, ? extends Type>> keyCoercer;
        private final Optional<TypeCoercer<? extends Type, ? extends Type>> valueCoercer;

        public MapCoercer(
                MapType fromType,
                MapType toType,
                Optional<TypeCoercer<? extends Type, ? extends Type>> keyCoercer,
                Optional<TypeCoercer<? extends Type, ? extends Type>> valueCoercer)
        {
            super(fromType, toType);
            this.keyCoercer = requireNonNull(keyCoercer, "keyCoercer is null");
            this.valueCoercer = requireNonNull(valueCoercer, "valueCoercer is null");
        }

        @Override
        public Block apply(Block block)
        {
            ColumnarMap mapBlock = toColumnarMap(block);
            Block keysBlock = keyCoercer.isEmpty() ? mapBlock.getKeysBlock() : keyCoercer.get().apply(mapBlock.getKeysBlock());
            Block valuesBlock = valueCoercer.isEmpty() ? mapBlock.getValuesBlock() : valueCoercer.get().apply(mapBlock.getValuesBlock());
            boolean[] valueIsNull = new boolean[mapBlock.getPositionCount()];
            int[] offsets = new int[mapBlock.getPositionCount() + 1];
            for (int i = 0; i < mapBlock.getPositionCount(); i++) {
                valueIsNull[i] = mapBlock.isNull(i);
                offsets[i + 1] = offsets[i] + mapBlock.getEntryCount(i);
            }
            return toType.createBlockFromKeyValue(Optional.of(valueIsNull), offsets, keysBlock, valuesBlock);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            throw new UnsupportedOperationException("Not supported");
        }
    }

    private static class StructCoercer
            extends TypeCoercer<RowType, RowType>
    {
        private final List<Optional<TypeCoercer<? extends Type, ? extends Type>>> coercers;
        private final Block[] nullBlocks;

        public StructCoercer(RowType fromType, RowType toType, List<Optional<TypeCoercer<? extends Type, ? extends Type>>> coercers)
        {
            super(fromType, toType);
            this.coercers = ImmutableList.copyOf(requireNonNull(coercers, "coercers is null"));
            List<Field> toTypeFields = toType.getFields();
            this.nullBlocks = new Block[toTypeFields.size()];
            for (int i = 0; i < toTypeFields.size(); i++) {
                if (i >= fromType.getFields().size()) {
                    nullBlocks[i] = toTypeFields.get(i).getType().createBlockBuilder(null, 1).appendNull().build();
                }
            }
        }

        @Override
        public Block apply(Block block)
        {
            ColumnarRow rowBlock = toColumnarRow(block);
            Block[] fields = new Block[coercers.size()];
            int[] ids = new int[rowBlock.getField(0).getPositionCount()];
            for (int i = 0; i < coercers.size(); i++) {
                Optional<TypeCoercer<? extends Type, ? extends Type>> coercer = coercers.get(i);
                if (coercer.isPresent()) {
                    fields[i] = coercer.get().apply(rowBlock.getField(i));
                }
                else if (i < rowBlock.getFieldCount()) {
                    fields[i] = rowBlock.getField(i);
                }
                else {
                    fields[i] = DictionaryBlock.create(ids.length, nullBlocks[i], ids);
                }
            }
            boolean[] valueIsNull = null;
            if (rowBlock.mayHaveNull()) {
                valueIsNull = new boolean[rowBlock.getPositionCount()];
                for (int i = 0; i < rowBlock.getPositionCount(); i++) {
                    valueIsNull[i] = rowBlock.isNull(i);
                }
            }
            return RowBlock.fromFieldBlocks(rowBlock.getPositionCount(), Optional.ofNullable(valueIsNull), fields);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            throw new UnsupportedOperationException("Not supported");
        }
    }
}
