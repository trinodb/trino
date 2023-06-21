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
import io.trino.plugin.hive.type.Category;
import io.trino.plugin.hive.type.ListTypeInfo;
import io.trino.plugin.hive.type.MapTypeInfo;
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
import static io.trino.plugin.hive.util.HiveUtil.extractStructFieldTypes;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.block.ColumnarArray.toColumnarArray;
import static io.trino.spi.block.ColumnarMap.toColumnarMap;
import static io.trino.spi.block.ColumnarRow.toColumnarRow;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class CoercionUtils
{
    private CoercionUtils() {}

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
        if (fromType instanceof TimestampType timestampType && toType instanceof VarcharType varcharType) {
            if (timestampType.isShort()) {
                return Optional.of(new TimestampCoercer.ShortTimestampToVarcharCoercer(timestampType, varcharType));
            }
            return Optional.of(new TimestampCoercer.LongTimestampToVarcharCoercer(timestampType, varcharType));
        }
        if ((fromType instanceof ArrayType) && (toType instanceof ArrayType)) {
            return Optional.of(new ListCoercer(typeManager, fromHiveType, toHiveType, timestampPrecision));
        }
        if ((fromType instanceof MapType) && (toType instanceof MapType)) {
            return Optional.of(new MapCoercer(typeManager, fromHiveType, toHiveType, timestampPrecision));
        }
        if ((fromType instanceof RowType) && (toType instanceof RowType)) {
            HiveType fromHiveTypeStruct = (fromHiveType.getCategory() == Category.UNION) ? HiveType.toHiveType(fromType) : fromHiveType;
            HiveType toHiveTypeStruct = (toHiveType.getCategory() == Category.UNION) ? HiveType.toHiveType(toType) : toHiveType;

            return Optional.of(new StructCoercer(typeManager, fromHiveTypeStruct, toHiveTypeStruct, timestampPrecision));
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

    private static class ListCoercer
            extends TypeCoercer<ArrayType, ArrayType>
    {
        private final Optional<TypeCoercer<? extends Type, ? extends Type>> elementCoercer;

        public ListCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType, HiveTimestampPrecision timestampPrecision)
        {
            super((ArrayType) fromHiveType.getType(typeManager, timestampPrecision), (ArrayType) toHiveType.getType(typeManager, timestampPrecision));
            requireNonNull(typeManager, "typeManager is null");
            requireNonNull(fromHiveType, "fromHiveType is null");
            requireNonNull(toHiveType, "toHiveType is null");
            requireNonNull(timestampPrecision, "timestampPrecision is null");
            HiveType fromElementHiveType = HiveType.valueOf(((ListTypeInfo) fromHiveType.getTypeInfo()).getListElementTypeInfo().getTypeName());
            HiveType toElementHiveType = HiveType.valueOf(((ListTypeInfo) toHiveType.getTypeInfo()).getListElementTypeInfo().getTypeName());
            this.elementCoercer = createCoercer(typeManager, fromElementHiveType, toElementHiveType, timestampPrecision);
        }

        @Override
        public Block apply(Block block)
        {
            if (elementCoercer.isEmpty()) {
                return block;
            }
            ColumnarArray arrayBlock = toColumnarArray(block);
            Block elementsBlock = elementCoercer.get().apply(arrayBlock.getElementsBlock());
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

        public MapCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType, HiveTimestampPrecision timestampPrecision)
        {
            super((MapType) fromHiveType.getType(typeManager, timestampPrecision), (MapType) toHiveType.getType(typeManager, timestampPrecision));
            requireNonNull(typeManager, "typeManager is null");
            requireNonNull(fromHiveType, "fromHiveType is null");
            requireNonNull(timestampPrecision, "timestampPrecision is null");
            HiveType fromKeyHiveType = HiveType.valueOf(((MapTypeInfo) fromHiveType.getTypeInfo()).getMapKeyTypeInfo().getTypeName());
            HiveType fromValueHiveType = HiveType.valueOf(((MapTypeInfo) fromHiveType.getTypeInfo()).getMapValueTypeInfo().getTypeName());
            HiveType toKeyHiveType = HiveType.valueOf(((MapTypeInfo) toHiveType.getTypeInfo()).getMapKeyTypeInfo().getTypeName());
            HiveType toValueHiveType = HiveType.valueOf(((MapTypeInfo) toHiveType.getTypeInfo()).getMapValueTypeInfo().getTypeName());
            this.keyCoercer = createCoercer(typeManager, fromKeyHiveType, toKeyHiveType, timestampPrecision);
            this.valueCoercer = createCoercer(typeManager, fromValueHiveType, toValueHiveType, timestampPrecision);
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

        public StructCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType, HiveTimestampPrecision timestampPrecision)
        {
            super((RowType) fromHiveType.getType(typeManager, timestampPrecision), (RowType) toHiveType.getType(typeManager, timestampPrecision));
            requireNonNull(typeManager, "typeManager is null");
            requireNonNull(fromHiveType, "fromHiveType is null");
            requireNonNull(toHiveType, "toHiveType is null");
            requireNonNull(timestampPrecision, "timestampPrecision is null");
            List<HiveType> fromFieldTypes = extractStructFieldTypes(fromHiveType);
            List<HiveType> toFieldTypes = extractStructFieldTypes(toHiveType);
            ImmutableList.Builder<Optional<TypeCoercer<? extends Type, ? extends Type>>> coercers = ImmutableList.builder();
            this.nullBlocks = new Block[toFieldTypes.size()];
            for (int i = 0; i < toFieldTypes.size(); i++) {
                if (i >= fromFieldTypes.size()) {
                    nullBlocks[i] = toFieldTypes.get(i).getType(typeManager).createBlockBuilder(null, 1).appendNull().build();
                    coercers.add(Optional.empty());
                }
                else {
                    coercers.add(createCoercer(typeManager, fromFieldTypes.get(i), toFieldTypes.get(i), timestampPrecision));
                }
            }
            this.coercers = coercers.build();
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
