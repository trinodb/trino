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
import io.trino.plugin.hive.coercions.BooleanCoercer.BooleanToVarcharCoercer;
import io.trino.plugin.hive.coercions.DateCoercer.VarcharToDateCoercer;
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
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hive.HiveType.HIVE_BYTE;
import static io.trino.plugin.hive.HiveType.HIVE_DOUBLE;
import static io.trino.plugin.hive.HiveType.HIVE_FLOAT;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.plugin.hive.HiveType.HIVE_LONG;
import static io.trino.plugin.hive.HiveType.HIVE_SHORT;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDecimalToDecimalCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDecimalToDoubleCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDecimalToInteger;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDecimalToRealCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDecimalToVarcharCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createDoubleToDecimalCoercer;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createRealToDecimalCoercer;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.block.ColumnarArray.toColumnarArray;
import static io.trino.spi.block.ColumnarMap.toColumnarMap;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class CoercionUtils
{
    private CoercionUtils() {}

    public static Type createTypeFromCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType, CoercionContext coercionContext)
    {
        return createCoercer(typeManager, fromHiveType, toHiveType, coercionContext)
                .map(TypeCoercer::getFromType)
                .orElseGet(() -> fromHiveType.getType(typeManager, coercionContext.timestampPrecision()));
    }

    public static Optional<TypeCoercer<? extends Type, ? extends Type>> createCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType, CoercionContext coercionContext)
    {
        if (fromHiveType.equals(toHiveType)) {
            return Optional.empty();
        }

        Type fromType = fromHiveType.getType(typeManager, coercionContext.timestampPrecision());
        Type toType = toHiveType.getType(typeManager, coercionContext.timestampPrecision());

        if (toType instanceof VarcharType toVarcharType && (fromHiveType.equals(HIVE_BYTE) || fromHiveType.equals(HIVE_SHORT) || fromHiveType.equals(HIVE_INT) || fromHiveType.equals(HIVE_LONG))) {
            return Optional.of(new IntegerNumberToVarcharCoercer<>(fromType, toVarcharType));
        }
        if (fromType instanceof VarcharType fromVarcharType && (toHiveType.equals(HIVE_BYTE) || toHiveType.equals(HIVE_SHORT) || toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG))) {
            return Optional.of(new VarcharToIntegerNumberCoercer<>(fromVarcharType, toType));
        }
        if (fromType instanceof VarcharType varcharType && toHiveType.equals(HIVE_DOUBLE)) {
            return Optional.of(new VarcharToDoubleCoercer(varcharType, coercionContext.treatNaNAsNull()));
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
        if (fromType instanceof VarcharType fromVarcharType && toType instanceof DateType toDateType) {
            return Optional.of(new VarcharToDateCoercer(fromVarcharType, toDateType));
        }
        if (fromType instanceof BooleanType && toType instanceof VarcharType toVarcharType) {
            return Optional.of(new BooleanToVarcharCoercer(toVarcharType));
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
            return Optional.of(new IntegerToBigintCoercer());
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
        if (fromType instanceof DecimalType fromDecimalType &&
                (toType instanceof TinyintType ||
                        toType instanceof SmallintType ||
                        toType instanceof IntegerType ||
                        toType instanceof BigintType)) {
            return Optional.of(createDecimalToInteger(fromDecimalType, toType));
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
        if (fromType == DOUBLE && toType instanceof VarcharType toVarcharType) {
            return Optional.of(new DoubleToVarcharCoercer(toVarcharType, coercionContext.treatNaNAsNull()));
        }
        if ((fromType instanceof ArrayType) && (toType instanceof ArrayType)) {
            return createCoercerForList(
                    typeManager,
                    (ListTypeInfo) fromHiveType.getTypeInfo(),
                    (ListTypeInfo) toHiveType.getTypeInfo(),
                    coercionContext);
        }
        if ((fromType instanceof MapType) && (toType instanceof MapType)) {
            return createCoercerForMap(
                    typeManager,
                    (MapTypeInfo) fromHiveType.getTypeInfo(),
                    (MapTypeInfo) toHiveType.getTypeInfo(),
                    coercionContext);
        }
        if ((fromType instanceof RowType) && (toType instanceof RowType)) {
            HiveType fromHiveTypeStruct = (fromHiveType.getCategory() == Category.UNION) ? HiveType.toHiveType(fromType) : fromHiveType;
            HiveType toHiveTypeStruct = (toHiveType.getCategory() == Category.UNION) ? HiveType.toHiveType(toType) : toHiveType;

            return createCoercerForStruct(
                    typeManager,
                    (StructTypeInfo) fromHiveTypeStruct.getTypeInfo(),
                    (StructTypeInfo) toHiveTypeStruct.getTypeInfo(),
                    coercionContext);
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
            CoercionContext coercionContext)
    {
        HiveType fromElementHiveType = HiveType.valueOf(fromListTypeInfo.getListElementTypeInfo().getTypeName());
        HiveType toElementHiveType = HiveType.valueOf(toListTypeInfo.getListElementTypeInfo().getTypeName());

        return createCoercer(typeManager, fromElementHiveType, toElementHiveType, coercionContext)
                .map(elementCoercer -> new ListCoercer(new ArrayType(elementCoercer.getFromType()), new ArrayType(elementCoercer.getToType()), elementCoercer));
    }

    private static Optional<TypeCoercer<? extends Type, ? extends Type>> createCoercerForMap(
            TypeManager typeManager,
            MapTypeInfo fromMapTypeInfo,
            MapTypeInfo toMapTypeInfo,
            CoercionContext coercionContext)
    {
        HiveType fromKeyHiveType = HiveType.valueOf(fromMapTypeInfo.getMapKeyTypeInfo().getTypeName());
        HiveType fromValueHiveType = HiveType.valueOf(fromMapTypeInfo.getMapValueTypeInfo().getTypeName());
        HiveType toKeyHiveType = HiveType.valueOf(toMapTypeInfo.getMapKeyTypeInfo().getTypeName());
        HiveType toValueHiveType = HiveType.valueOf(toMapTypeInfo.getMapValueTypeInfo().getTypeName());
        Optional<TypeCoercer<? extends Type, ? extends Type>> keyCoercer = createCoercer(typeManager, fromKeyHiveType, toKeyHiveType, coercionContext);
        Optional<TypeCoercer<? extends Type, ? extends Type>> valueCoercer = createCoercer(typeManager, fromValueHiveType, toValueHiveType, coercionContext);
        MapType fromType = new MapType(
                keyCoercer.map(TypeCoercer::getFromType).orElseGet(() -> fromKeyHiveType.getType(typeManager, coercionContext.timestampPrecision())),
                valueCoercer.map(TypeCoercer::getFromType).orElseGet(() -> fromValueHiveType.getType(typeManager, coercionContext.timestampPrecision())),
                typeManager.getTypeOperators());

        MapType toType = new MapType(
                keyCoercer.map(TypeCoercer::getToType).orElseGet(() -> toKeyHiveType.getType(typeManager, coercionContext.timestampPrecision())),
                valueCoercer.map(TypeCoercer::getToType).orElseGet(() -> toValueHiveType.getType(typeManager, coercionContext.timestampPrecision())),
                typeManager.getTypeOperators());

        return Optional.of(new MapCoercer(fromType, toType, keyCoercer, valueCoercer));
    }

    private static Optional<TypeCoercer<? extends Type, ? extends Type>> createCoercerForStruct(
            TypeManager typeManager,
            StructTypeInfo fromStructTypeInfo,
            StructTypeInfo toStructTypeInfo,
            CoercionContext coercionContext)
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
                        toStructFieldType.getType(typeManager, coercionContext.timestampPrecision())));
                coercers.add(Optional.empty());
            }
            else {
                HiveType fromStructFieldType = HiveType.valueOf(fromStructTypeInfo.getAllStructFieldTypeInfos().get(i).getTypeName());

                Optional<TypeCoercer<? extends Type, ? extends Type>> coercer = createCoercer(typeManager, fromStructFieldType, toStructFieldType, coercionContext);

                fromField.add(new Field(
                        Optional.of(fromStructFieldName.get(i)),
                        coercer.map(TypeCoercer::getFromType).orElseGet(() -> fromStructFieldType.getType(typeManager, coercionContext.timestampPrecision()))));
                toField.add(new Field(
                        Optional.of(toStructFieldNames.get(i)),
                        coercer.map(TypeCoercer::getToType).orElseGet(() -> toStructFieldType.getType(typeManager, coercionContext.timestampPrecision()))));

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

        public StructCoercer(RowType fromType, RowType toType, List<Optional<TypeCoercer<? extends Type, ? extends Type>>> coercers)
        {
            super(fromType, toType);
            checkArgument(toType.getTypeParameters().size() == coercers.size());
            checkArgument(fromType.getTypeParameters().size() <= coercers.size());
            this.coercers = ImmutableList.copyOf(requireNonNull(coercers, "coercers is null"));
        }

        @Override
        public Block apply(Block block)
        {
            if (block instanceof LazyBlock lazyBlock) {
                // only load the top level block so non-coerced fields are not loaded
                block = lazyBlock.getBlock();
            }

            if (block instanceof RunLengthEncodedBlock runLengthEncodedBlock) {
                RowBlock rowBlock = (RowBlock) runLengthEncodedBlock.getValue();
                RowBlock newRowBlock = RowBlock.fromNotNullSuppressedFieldBlocks(
                        1,
                        rowBlock.isNull(0) ? Optional.of(new boolean[]{true}) : Optional.empty(),
                        coerceFields(rowBlock.getFieldBlocks()));
                return RunLengthEncodedBlock.create(newRowBlock, runLengthEncodedBlock.getPositionCount());
            }
            if (block instanceof DictionaryBlock dictionaryBlock) {
                RowBlock rowBlock = (RowBlock) dictionaryBlock.getDictionary();
                // create a dictionary block for each field, by rewraping the nested fields in a new dictionary
                List<Block> fieldBlocks = rowBlock.getFieldBlocks().stream()
                        .map(dictionaryBlock::createProjection)
                        .toList();
                // coerce the wrapped fields, so only the used dictionary values are coerced
                Block[] newFields = coerceFields(fieldBlocks);
                return RowBlock.fromNotNullSuppressedFieldBlocks(
                        dictionaryBlock.getPositionCount(),
                        getNulls(dictionaryBlock),
                        newFields);
            }
            RowBlock rowBlock = (RowBlock) block;
            return RowBlock.fromNotNullSuppressedFieldBlocks(
                    rowBlock.getPositionCount(),
                    getNulls(rowBlock),
                    coerceFields(rowBlock.getFieldBlocks()));
        }

        private static Optional<boolean[]> getNulls(Block rowBlock)
        {
            if (!rowBlock.mayHaveNull()) {
                return Optional.empty();
            }

            boolean[] valueIsNull = new boolean[rowBlock.getPositionCount()];
            for (int i = 0; i < rowBlock.getPositionCount(); i++) {
                valueIsNull[i] = rowBlock.isNull(i);
            }
            return Optional.of(valueIsNull);
        }

        private Block[] coerceFields(List<Block> fields)
        {
            Block[] newFields = new Block[coercers.size()];
            for (int i = 0; i < coercers.size(); i++) {
                Optional<TypeCoercer<? extends Type, ? extends Type>> coercer = coercers.get(i);
                if (coercer.isPresent()) {
                    newFields[i] = coercer.get().apply(fields.get(i));
                }
                else if (i < fields.size()) {
                    newFields[i] = fields.get(i);
                }
                else {
                    newFields[i] = RunLengthEncodedBlock.create(toType.getTypeParameters().get(i), null, fields.get(0).getPositionCount());
                }
            }
            return newFields;
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            throw new UnsupportedOperationException("Not supported");
        }
    }

    public record CoercionContext(HiveTimestampPrecision timestampPrecision, boolean treatNaNAsNull)
    {
        public CoercionContext
        {
            requireNonNull(timestampPrecision, "timestampPrecision is null");
        }
    }
}
