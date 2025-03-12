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
package io.trino.spi.type;

import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.OperatorMethodHandle;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles.Lookup;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.BLOCK_BUILDER;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.StandardTypes.ROW;
import static io.trino.spi.type.TypeUtils.NULL_HASH_CODE;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.lang.invoke.MethodHandles.collectArguments;
import static java.lang.invoke.MethodHandles.constant;
import static java.lang.invoke.MethodHandles.dropArguments;
import static java.lang.invoke.MethodHandles.insertArguments;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodHandles.permuteArguments;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

/**
 * As defined in ISO/IEC FCD 9075-2 (SQL 2011), section 4.8
 */
public class RowType
        extends AbstractType
{
    private static final InvocationConvention READ_FLAT_CONVENTION = simpleConvention(FAIL_ON_NULL, FLAT);
    private static final InvocationConvention READ_FLAT_TO_BLOCK_CONVENTION = simpleConvention(BLOCK_BUILDER, FLAT);
    private static final InvocationConvention WRITE_FLAT_CONVENTION = simpleConvention(FLAT_RETURN, NEVER_NULL);
    private static final InvocationConvention EQUAL_CONVENTION = simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL);
    private static final InvocationConvention HASH_CODE_CONVENTION = simpleConvention(FAIL_ON_NULL, NEVER_NULL);
    private static final InvocationConvention IDENTICAL_CONVENTION = simpleConvention(FAIL_ON_NULL, BOXED_NULLABLE, BOXED_NULLABLE);
    private static final InvocationConvention INDETERMINATE_CONVENTION = simpleConvention(FAIL_ON_NULL, BOXED_NULLABLE);
    private static final InvocationConvention COMPARISON_CONVENTION = simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL);

    private static final MethodHandle READ_FLAT;
    private static final MethodHandle READ_FLAT_TO_BLOCK;
    private static final MethodHandle WRITE_FLAT;
    private static final MethodHandle EQUAL;
    private static final MethodHandle CHAIN_EQUAL;
    private static final MethodHandle HASH_CODE;
    private static final MethodHandle CHAIN_HASH_CODE;
    private static final MethodHandle IDENTICAL;
    private static final MethodHandle CHAIN_IDENTICAL_START;
    private static final MethodHandle CHAIN_IDENTICAL;
    private static final MethodHandle INDETERMINATE;
    private static final MethodHandle CHAIN_INDETERMINATE;
    private static final MethodHandle COMPARISON;
    private static final MethodHandle CHAIN_COMPARISON;
    private static final int MEGAMORPHIC_FIELD_COUNT = 64;

    // this field is used in double-checked locking
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private volatile TypeOperatorDeclaration typeOperatorDeclaration;

    static {
        try {
            Lookup lookup = lookup();
            READ_FLAT = lookup.findStatic(RowType.class, "megamorphicReadFlat", methodType(SqlRow.class, RowType.class, List.class, byte[].class, int.class, byte[].class, int.class));
            READ_FLAT_TO_BLOCK = lookup.findStatic(RowType.class, "megamorphicReadFlatToBlock", methodType(void.class, RowType.class, List.class, byte[].class, int.class, byte[].class, int.class, BlockBuilder.class));
            WRITE_FLAT = lookup.findStatic(RowType.class, "megamorphicWriteFlat", methodType(void.class, RowType.class, List.class, SqlRow.class, byte[].class, int.class, byte[].class, int.class));
            EQUAL = lookup.findStatic(RowType.class, "megamorphicEqualOperator", methodType(Boolean.class, List.class, SqlRow.class, SqlRow.class));
            CHAIN_EQUAL = lookup.findStatic(RowType.class, "chainEqual", methodType(Boolean.class, Boolean.class, int.class, MethodHandle.class, SqlRow.class, SqlRow.class));
            HASH_CODE = lookup.findStatic(RowType.class, "megamorphicHashCodeOperator", methodType(long.class, List.class, SqlRow.class));
            CHAIN_HASH_CODE = lookup.findStatic(RowType.class, "chainHashCode", methodType(long.class, long.class, int.class, MethodHandle.class, SqlRow.class));
            IDENTICAL = lookup.findStatic(RowType.class, "megamorphicIdenticalOperator", methodType(boolean.class, List.class, SqlRow.class, SqlRow.class));
            CHAIN_IDENTICAL_START = lookup.findStatic(RowType.class, "chainIdenticalStart", methodType(boolean.class, MethodHandle.class, SqlRow.class, SqlRow.class));
            CHAIN_IDENTICAL = lookup.findStatic(RowType.class, "chainIdentical", methodType(boolean.class, boolean.class, int.class, MethodHandle.class, SqlRow.class, SqlRow.class));
            INDETERMINATE = lookup.findStatic(RowType.class, "megamorphicIndeterminateOperator", methodType(boolean.class, List.class, SqlRow.class));
            CHAIN_INDETERMINATE = lookup.findStatic(RowType.class, "chainIndeterminate", methodType(boolean.class, boolean.class, int.class, MethodHandle.class, SqlRow.class));
            COMPARISON = lookup.findStatic(RowType.class, "megamorphicComparisonOperator", methodType(long.class, List.class, SqlRow.class, SqlRow.class));
            CHAIN_COMPARISON = lookup.findStatic(RowType.class, "chainComparison", methodType(long.class, long.class, int.class, MethodHandle.class, SqlRow.class, SqlRow.class));
        }
        catch (NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private final List<Field> fields;
    private final List<Type> fieldTypes;
    private final boolean comparable;
    private final boolean orderable;
    private final int flatFixedSize;
    private final boolean flatVariableWidth;

    private RowType(TypeSignature typeSignature, List<Field> originalFields)
    {
        super(typeSignature, SqlRow.class, RowBlock.class);

        this.fields = List.copyOf(originalFields);
        this.fieldTypes = fields.stream()
                .map(Field::getType)
                .toList();

        this.comparable = fields.stream().allMatch(field -> field.getType().isComparable());
        this.orderable = fields.stream().allMatch(field -> field.getType().isOrderable());

        // flat fixed size is one null byte for each field plus the sum of the field fixed sizes
        int fixedSize = fieldTypes.size();
        for (Type fieldType : fieldTypes) {
            fixedSize += fieldType.getFlatFixedSize();
        }
        flatFixedSize = fixedSize;

        this.flatVariableWidth = fields.stream().anyMatch(field -> field.getType().isFlatVariableWidth());
    }

    public static RowType from(List<Field> fields)
    {
        return new RowType(makeSignature(fields), fields);
    }

    public static RowType anonymous(List<Type> types)
    {
        List<Field> fields = types.stream()
                .map(type -> new Field(Optional.empty(), type))
                .toList();

        return new RowType(makeSignature(fields), fields);
    }

    public static RowType rowType(Field... field)
    {
        return from(Arrays.asList(field));
    }

    public static RowType anonymousRow(Type... types)
    {
        return anonymous(Arrays.asList(types));
    }

    // Only RowParametricType.createType should call this method
    public static RowType createWithTypeSignature(TypeSignature typeSignature, List<Field> fields)
    {
        return new RowType(typeSignature, fields);
    }

    public static Field field(String name, Type type)
    {
        return new Field(Optional.of(name), type);
    }

    public static Field field(Type type)
    {
        return new Field(Optional.empty(), type);
    }

    private static TypeSignature makeSignature(List<Field> fields)
    {
        int size = fields.size();
        if (size == 0) {
            throw new IllegalArgumentException("Row type must have at least 1 field");
        }

        List<TypeSignatureParameter> parameters = fields.stream()
                .map(field -> new NamedTypeSignature(field.getName().map(RowFieldName::new), field.getType().getTypeSignature()))
                .map(TypeSignatureParameter::namedTypeParameter)
                .toList();

        return new TypeSignature(ROW, parameters);
    }

    @Override
    public RowBlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        return new RowBlockBuilder(getTypeParameters(), blockBuilderStatus, expectedEntries);
    }

    @Override
    public RowBlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return new RowBlockBuilder(getTypeParameters(), blockBuilderStatus, expectedEntries);
    }

    @Override
    public String getDisplayName()
    {
        // Convert to standard sql name
        StringBuilder result = new StringBuilder();
        result.append(ROW).append('(');
        for (Field field : fields) {
            String typeDisplayName = field.getType().getDisplayName();
            if (field.getName().isPresent()) {
                // TODO: names are already canonicalized, so they should be printed as delimited identifiers
                result.append(field.getName().get()).append(' ').append(typeDisplayName);
            }
            else {
                result.append(typeDisplayName);
            }
            result.append(", ");
        }
        result.setLength(result.length() - 2);
        result.append(')');
        return result.toString();
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        SqlRow sqlRow = getObject(block, position);
        List<Object> values = new ArrayList<>(sqlRow.getFieldCount());

        int rawIndex = sqlRow.getRawIndex();
        for (int i = 0; i < sqlRow.getFieldCount(); i++) {
            values.add(fields.get(i).getType().getObjectValue(session, sqlRow.getRawFieldBlock(i), rawIndex));
        }

        return Collections.unmodifiableList(values);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            writeObject(blockBuilder, getObject(block, position));
        }
    }

    @Override
    public SqlRow getObject(Block block, int position)
    {
        return read((RowBlock) block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(position));
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        SqlRow sqlRow = (SqlRow) value;
        int rawIndex = sqlRow.getRawIndex();
        ((RowBlockBuilder) blockBuilder).buildEntry(fieldBuilders -> {
            for (int i = 0; i < sqlRow.getFieldCount(); i++) {
                fields.get(i).getType().appendTo(sqlRow.getRawFieldBlock(i), rawIndex, fieldBuilders.get(i));
            }
        });
    }

    @Override
    public int getFlatFixedSize()
    {
        return flatFixedSize;
    }

    @Override
    public boolean isFlatVariableWidth()
    {
        return flatVariableWidth;
    }

    @Override
    public int getFlatVariableWidthSize(Block block, int position)
    {
        if (!flatVariableWidth) {
            return 0;
        }

        SqlRow sqlRow = getObject(block, position);
        int rawIndex = sqlRow.getRawIndex();

        int variableSize = 0;
        for (int i = 0; i < fieldTypes.size(); i++) {
            Type fieldType = fieldTypes.get(i);
            Block fieldBlock = sqlRow.getRawFieldBlock(i);
            if (!fieldBlock.isNull(rawIndex)) {
                variableSize += fieldType.getFlatVariableWidthSize(fieldBlock, rawIndex);
            }
        }
        return variableSize;
    }

    @Override
    public int getFlatVariableWidthLength(byte[] fixedSizeSlice, int fixedSizeOffset)
    {
        int variableSize = 0;
        for (Type fieldType : fieldTypes) {
            if (fixedSizeSlice[fixedSizeOffset] == 0) {
                variableSize += fieldType.getFlatVariableWidthLength(fixedSizeSlice, fixedSizeOffset + 1);
            }
            fixedSizeOffset += fieldType.getFlatFixedSize() + 1;
        }
        return variableSize;
    }

    @Override
    public List<Type> getTypeParameters()
    {
        return fieldTypes;
    }

    public List<Field> getFields()
    {
        return fields;
    }

    public static class Field
    {
        private final Type type;
        private final Optional<String> name;

        public Field(Optional<String> name, Type type)
        {
            this.type = requireNonNull(type, "type is null");
            this.name = requireNonNull(name, "name is null");
        }

        public Type getType()
        {
            return type;
        }

        public Optional<String> getName()
        {
            return name;
        }
    }

    @Override
    public boolean isComparable()
    {
        return comparable;
    }

    @Override
    public boolean isOrderable()
    {
        return orderable;
    }

    @Override
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        if (typeOperatorDeclaration == null) {
            generateTypeOperators(typeOperators);
        }
        return typeOperatorDeclaration;
    }

    private void generateTypeOperators(TypeOperators typeOperators)
    {
        if (typeOperatorDeclaration != null) {
            return;
        }
        typeOperatorDeclaration = TypeOperatorDeclaration.builder(getJavaType())
                .addReadValueOperators(getReadValueOperatorMethodHandles(typeOperators))
                .addEqualOperators(getEqualOperatorMethodHandles(typeOperators, fields))
                .addHashCodeOperators(getHashCodeOperatorMethodHandles(typeOperators, fields))
                .addXxHash64Operators(getXxHash64OperatorMethodHandles(typeOperators, fields))
                .addIdenticalOperators(getIdenticalOperatorInvokers(typeOperators, fields))
                .addIndeterminateOperators(getIndeterminateOperatorInvokers(typeOperators, fields))
                .addComparisonUnorderedLastOperators(getComparisonOperatorInvokers(typeOperators::getComparisonUnorderedLastOperator, fields))
                .addComparisonUnorderedFirstOperators(getComparisonOperatorInvokers(typeOperators::getComparisonUnorderedFirstOperator, fields))
               .build();
    }

    private List<OperatorMethodHandle> getReadValueOperatorMethodHandles(TypeOperators typeOperators)
    {
        List<MethodHandle> fieldReadFlatMethods = fields.stream()
                .map(Field::getType)
                .map(type -> typeOperators.getReadValueOperator(type, simpleConvention(BLOCK_BUILDER, FLAT)))
                .toList();
        MethodHandle readFlat = insertArguments(READ_FLAT, 0, this, fieldReadFlatMethods);
        MethodHandle readFlatToBlock = insertArguments(READ_FLAT_TO_BLOCK, 0, this, fieldReadFlatMethods);

        List<MethodHandle> fieldWriteFlatMethods = fields.stream()
                .map(Field::getType)
                .map(type -> typeOperators.getReadValueOperator(type, simpleConvention(FLAT_RETURN, BLOCK_POSITION)))
                .toList();
        MethodHandle writeFlat = insertArguments(WRITE_FLAT, 0, this, fieldWriteFlatMethods);

        return List.of(
                new OperatorMethodHandle(READ_FLAT_CONVENTION, readFlat),
                new OperatorMethodHandle(READ_FLAT_TO_BLOCK_CONVENTION, readFlatToBlock),
                new OperatorMethodHandle(WRITE_FLAT_CONVENTION, writeFlat));
    }

    private static SqlRow read(RowBlock block, int position)
    {
        return block.getRow(position);
    }

    private static SqlRow megamorphicReadFlat(
            RowType rowType,
            List<MethodHandle> fieldReadFlatMethods,
            byte[] fixedSizeSlice,
            int fixedSizeOffset,
            byte[] variableSizeSlice,
            int variableSizeOffset)
            throws Throwable
    {
        return buildRowValue(rowType, fieldBuilders ->
                readFlatFields(rowType, fieldReadFlatMethods, fixedSizeSlice, fixedSizeOffset, variableSizeSlice, variableSizeOffset, fieldBuilders));
    }

    private static void megamorphicReadFlatToBlock(
            RowType rowType,
            List<MethodHandle> fieldReadFlatMethods,
            byte[] fixedSizeSlice,
            int fixedSizeOffset,
            byte[] variableSizeSlice,
            int variableSizeOffset,
            BlockBuilder blockBuilder)
            throws Throwable
    {
        ((RowBlockBuilder) blockBuilder).buildEntry(fieldBuilders ->
                readFlatFields(rowType, fieldReadFlatMethods, fixedSizeSlice, fixedSizeOffset, variableSizeSlice, variableSizeOffset, fieldBuilders));
    }

    private static void readFlatFields(
            RowType rowType,
            List<MethodHandle> fieldReadFlatMethods,
            byte[] fixedSizeSlice,
            int fixedSizeOffset,
            byte[] variableSizeSlice,
            int variableSizeOffset,
            List<BlockBuilder> fieldBuilders)
            throws Throwable
    {
        List<Type> fieldTypes = rowType.getTypeParameters();
        for (int fieldIndex = 0; fieldIndex < fieldTypes.size(); fieldIndex++) {
            Type fieldType = fieldTypes.get(fieldIndex);
            BlockBuilder fieldBuilder = fieldBuilders.get(fieldIndex);

            boolean isNull = fixedSizeSlice[fixedSizeOffset] != 0;
            if (isNull) {
                fieldBuilder.appendNull();
            }
            else {
                fieldReadFlatMethods.get(fieldIndex).invokeExact(fixedSizeSlice, fixedSizeOffset + 1, variableSizeSlice, variableSizeOffset, fieldBuilder);
                if (fieldType.isFlatVariableWidth()) {
                    variableSizeOffset += fieldType.getFlatVariableWidthLength(fixedSizeSlice, fixedSizeOffset + 1);
                }
            }
            fixedSizeOffset += 1 + fieldType.getFlatFixedSize();
        }
    }

    private static void megamorphicWriteFlat(
            RowType rowType,
            List<MethodHandle> fieldWriteFlatMethods,
            SqlRow row,
            byte[] fixedSizeSlice,
            int fixedSizeOffset,
            byte[] variableSizeSlice,
            int variableSizeOffset)
            throws Throwable
    {
        int rawIndex = row.getRawIndex();
        List<Type> fieldTypes = rowType.getTypeParameters();
        for (int fieldIndex = 0; fieldIndex < fieldTypes.size(); fieldIndex++) {
            Type fieldType = fieldTypes.get(fieldIndex);
            Block fieldBlock = row.getRawFieldBlock(fieldIndex);
            if (fieldBlock.isNull(rawIndex)) {
                fixedSizeSlice[fixedSizeOffset] = 1;
            }
            else {
                fieldWriteFlatMethods.get(fieldIndex).invokeExact((Block) fieldBlock, rawIndex, fixedSizeSlice, fixedSizeOffset + 1, variableSizeSlice, variableSizeOffset);
                if (fieldType.isFlatVariableWidth()) {
                    variableSizeOffset += fieldType.getFlatVariableWidthLength(fixedSizeSlice, fixedSizeOffset + 1);
                }
            }
            fixedSizeOffset += 1 + fieldType.getFlatFixedSize();
        }
    }

    private static List<OperatorMethodHandle> getEqualOperatorMethodHandles(TypeOperators typeOperators, List<Field> fields)
    {
        boolean comparable = fields.stream().allMatch(field -> field.getType().isComparable());
        if (!comparable) {
            return emptyList();
        }

        // for large rows, use a generic loop with a megamorphic call site
        if (fields.size() > MEGAMORPHIC_FIELD_COUNT) {
            List<MethodHandle> equalOperators = new ArrayList<>();
            for (Field field : fields) {
                MethodHandle equalOperator = typeOperators.getEqualOperator(field.getType(), simpleConvention(NULLABLE_RETURN, BLOCK_POSITION_NOT_NULL, BLOCK_POSITION_NOT_NULL));
                equalOperators.add(equalOperator);
            }
            return singletonList(new OperatorMethodHandle(EQUAL_CONVENTION, EQUAL.bindTo(equalOperators)));
        }

        // (SqlRow, SqlRow):Boolean
        MethodHandle equal = dropArguments(constant(Boolean.class, TRUE), 0, SqlRow.class, SqlRow.class);
        for (int fieldId = 0; fieldId < fields.size(); fieldId++) {
            Field field = fields.get(fieldId);
            // (SqlRow, SqlRow, int, MethodHandle, SqlRow, SqlRow):Boolean
            equal = collectArguments(
                    CHAIN_EQUAL,
                    0,
                    equal);

            // field equal
            MethodHandle fieldEqualOperator = typeOperators.getEqualOperator(field.getType(), simpleConvention(NULLABLE_RETURN, BLOCK_POSITION_NOT_NULL, BLOCK_POSITION_NOT_NULL));

            // (SqlRow, SqlRow, SqlRow, SqlRow):Boolean
            equal = insertArguments(equal, 2, fieldId, fieldEqualOperator);

            // (SqlRow, SqlRow):Boolean
            equal = permuteArguments(equal, methodType(Boolean.class, SqlRow.class, SqlRow.class), 0, 1, 0, 1);
        }
        return singletonList(new OperatorMethodHandle(EQUAL_CONVENTION, equal));
    }

    private static Boolean megamorphicEqualOperator(List<MethodHandle> equalOperators, SqlRow leftRow, SqlRow rightRow)
            throws Throwable
    {
        int leftRawIndex = leftRow.getRawIndex();
        int rightRawIndex = rightRow.getRawIndex();
        boolean unknown = false;
        for (int fieldIndex = 0; fieldIndex < equalOperators.size(); fieldIndex++) {
            Block leftFieldBlock = leftRow.getRawFieldBlock(fieldIndex);
            Block rightFieldBlock = rightRow.getRawFieldBlock(fieldIndex);
            if (leftFieldBlock.isNull(leftRawIndex) || rightFieldBlock.isNull(rightRawIndex)) {
                unknown = true;
                continue;
            }
            MethodHandle equalOperator = equalOperators.get(fieldIndex);
            Boolean result = (Boolean) equalOperator.invokeExact(leftFieldBlock, leftRawIndex, rightFieldBlock, rightRawIndex);
            if (result == null) {
                unknown = true;
            }
            else if (!result) {
                return false;
            }
        }

        if (unknown) {
            return null;
        }
        return true;
    }

    private static Boolean chainEqual(Boolean previousFieldsEqual, int currentFieldIndex, MethodHandle currentFieldEqual, SqlRow leftRow, SqlRow rightRow)
            throws Throwable
    {
        if (previousFieldsEqual == FALSE) {
            return FALSE;
        }

        int leftRawIndex = leftRow.getRawIndex();
        int rightRawIndex = rightRow.getRawIndex();
        Block leftFieldBlock = leftRow.getRawFieldBlock(currentFieldIndex);
        Block rightFieldBlock = rightRow.getRawFieldBlock(currentFieldIndex);

        if (leftFieldBlock.isNull(leftRawIndex) || rightFieldBlock.isNull(rightRawIndex)) {
            return null;
        }

        Boolean result = (Boolean) currentFieldEqual.invokeExact(leftFieldBlock, leftRawIndex, rightFieldBlock, rightRawIndex);
        if (result == TRUE) {
            // this field is equal, so the result is either true or unknown depending on the previous fields
            return previousFieldsEqual;
        }
        // this field is either not equal or unknown, which is the result
        return result;
    }

    private static List<OperatorMethodHandle> getHashCodeOperatorMethodHandles(TypeOperators typeOperators, List<Field> fields)
    {
        return getHashCodeOperatorMethodHandles(fields, type -> typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL)));
    }

    private static List<OperatorMethodHandle> getXxHash64OperatorMethodHandles(TypeOperators typeOperators, List<Field> fields)
    {
        return getHashCodeOperatorMethodHandles(fields, type -> typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL)));
    }

    private static List<OperatorMethodHandle> getHashCodeOperatorMethodHandles(List<Field> fields, Function<Type, MethodHandle> getHashOperator)
    {
        boolean comparable = fields.stream().allMatch(field -> field.getType().isComparable());
        if (!comparable) {
            return emptyList();
        }

        // for large rows, use a generic loop with a megamorphic call site
        if (fields.size() > MEGAMORPHIC_FIELD_COUNT) {
            List<MethodHandle> hashCodeOperators = fields.stream()
                    .map(field -> getHashOperator.apply(field.getType()))
                    .toList();
            return singletonList(new OperatorMethodHandle(HASH_CODE_CONVENTION, HASH_CODE.bindTo(hashCodeOperators)));
        }

        // (SqlRow):long
        MethodHandle hashCode = dropArguments(constant(long.class, 1), 0, SqlRow.class);
        for (int fieldId = 0; fieldId < fields.size(); fieldId++) {
            Field field = fields.get(fieldId);
            // (SqlRow, int, MethodHandle, SqlRow):long
            hashCode = collectArguments(
                    CHAIN_HASH_CODE,
                    0,
                    hashCode);

            // field hash code
            MethodHandle fieldHashCodeOperator = getHashOperator.apply(field.getType());

            // (SqlRow, SqlRow):long
            hashCode = insertArguments(hashCode, 1, fieldId, fieldHashCodeOperator);

            // (SqlRow):long
            hashCode = permuteArguments(hashCode, methodType(long.class, SqlRow.class), 0, 0);
        }
        return singletonList(new OperatorMethodHandle(HASH_CODE_CONVENTION, hashCode));
    }

    private static long megamorphicHashCodeOperator(List<MethodHandle> hashCodeOperators, SqlRow row)
            throws Throwable
    {
        int rawIndex = row.getRawIndex();

        long result = 1;
        for (int fieldIndex = 0; fieldIndex < hashCodeOperators.size(); fieldIndex++) {
            Block fieldBlock = row.getRawFieldBlock(fieldIndex);
            long fieldHashCode = NULL_HASH_CODE;
            if (!fieldBlock.isNull(rawIndex)) {
                MethodHandle hashCodeOperator = hashCodeOperators.get(fieldIndex);
                fieldHashCode = (long) hashCodeOperator.invokeExact(fieldBlock, rawIndex);
            }
            result = 31 * result + fieldHashCode;
        }
        return result;
    }

    private static long chainHashCode(long previousFieldHashCode, int currentFieldIndex, MethodHandle currentFieldHashCodeOperator, SqlRow row)
            throws Throwable
    {
        Block fieldBlock = row.getRawFieldBlock(currentFieldIndex);
        int rawIndex = row.getRawIndex();

        long fieldHashCode = NULL_HASH_CODE;
        if (!fieldBlock.isNull(rawIndex)) {
            fieldHashCode = (long) currentFieldHashCodeOperator.invokeExact(fieldBlock, rawIndex);
        }
        return 31 * previousFieldHashCode + fieldHashCode;
    }

    private static List<OperatorMethodHandle> getIdenticalOperatorInvokers(TypeOperators typeOperators, List<Field> fields)
    {
        boolean comparable = fields.stream().allMatch(field -> field.getType().isComparable());
        if (!comparable) {
            return emptyList();
        }

        // for large rows, use a generic loop with a megamorphic call site
        if (fields.size() > MEGAMORPHIC_FIELD_COUNT) {
            List<MethodHandle> identicalOperators = fields.stream()
                    .map(field -> typeOperators.getIdenticalOperator(field.getType(), simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION)))
                    .toList();
            return singletonList(new OperatorMethodHandle(IDENTICAL_CONVENTION, IDENTICAL.bindTo(identicalOperators)));
        }

        // (SqlRow, SqlRow):boolean
        MethodHandle identical = dropArguments(constant(boolean.class, true), 0, SqlRow.class, SqlRow.class);
        for (int fieldId = 0; fieldId < fields.size(); fieldId++) {
            Field field = fields.get(fieldId);
            // (SqlRow, SqlRow, int, MethodHandle, SqlRow, SqlRow):boolean
            identical = collectArguments(
                    CHAIN_IDENTICAL,
                    0,
                    identical);

            // field identical
            MethodHandle fieldIdenticalOperator = typeOperators.getIdenticalOperator(field.getType(), simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION));

            // (SqlRow, SqlRow, SqlRow, SqlRow):boolean
            identical = insertArguments(identical, 2, fieldId, fieldIdenticalOperator);

            // (SqlRow, SqlRow):boolean
            identical = permuteArguments(identical, methodType(boolean.class, SqlRow.class, SqlRow.class), 0, 1, 0, 1);
        }
        identical = CHAIN_IDENTICAL_START.bindTo(identical);

        return singletonList(new OperatorMethodHandle(IDENTICAL_CONVENTION, identical));
    }

    private static boolean megamorphicIdenticalOperator(List<MethodHandle> identicalOperators, SqlRow leftRow, SqlRow rightRow)
            throws Throwable
    {
        boolean leftIsNull = leftRow == null;
        boolean rightIsNull = rightRow == null;
        if (leftIsNull || rightIsNull) {
            return leftIsNull == rightIsNull;
        }

        int leftRawIndex = leftRow.getRawIndex();
        int rightRawIndex = rightRow.getRawIndex();

        for (int fieldIndex = 0; fieldIndex < identicalOperators.size(); fieldIndex++) {
            Block leftFieldBlock = leftRow.getRawFieldBlock(fieldIndex);
            Block rightFieldBlock = rightRow.getRawFieldBlock(fieldIndex);

            MethodHandle equalOperator = identicalOperators.get(fieldIndex);
            boolean result = (boolean) equalOperator.invoke(leftFieldBlock, leftRawIndex, rightFieldBlock, rightRawIndex);
            if (!result) {
                return false;
            }
        }

        return true;
    }

    private static boolean chainIdenticalStart(MethodHandle chain, SqlRow leftRow, SqlRow rightRow)
            throws Throwable
    {
        boolean leftIsNull = leftRow == null;
        boolean rightIsNull = rightRow == null;
        if (leftIsNull || rightIsNull) {
            return leftIsNull == rightIsNull;
        }
        return (boolean) chain.invokeExact(leftRow, rightRow);
    }

    private static boolean chainIdentical(boolean previousFieldsIdentical, int currentFieldIndex, MethodHandle currentFieldIdentical, SqlRow leftRow, SqlRow rightRow)
            throws Throwable
    {
        if (!previousFieldsIdentical) {
            return false;
        }
        return (boolean) currentFieldIdentical.invokeExact(
                leftRow.getRawFieldBlock(currentFieldIndex), leftRow.getRawIndex(),
                rightRow.getRawFieldBlock(currentFieldIndex), rightRow.getRawIndex());
    }

    private static List<OperatorMethodHandle> getIndeterminateOperatorInvokers(TypeOperators typeOperators, List<Field> fields)
    {
        boolean comparable = fields.stream().allMatch(field -> field.getType().isComparable());
        if (!comparable) {
            return emptyList();
        }

        // for large rows, use a generic loop with a megamorphic call site
        if (fields.size() > MEGAMORPHIC_FIELD_COUNT) {
            List<MethodHandle> indeterminateOperators = fields.stream()
                    .map(field -> typeOperators.getIndeterminateOperator(field.getType(), simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL)))
                    .toList();
            return singletonList(new OperatorMethodHandle(INDETERMINATE_CONVENTION, INDETERMINATE.bindTo(indeterminateOperators)));
        }

        // (SqlRow):long
        MethodHandle indeterminate = dropArguments(constant(boolean.class, false), 0, SqlRow.class);
        for (int fieldId = 0; fieldId < fields.size(); fieldId++) {
            Field field = fields.get(fieldId);
            // (SqlRow, int, MethodHandle, SqlRow):boolean
            indeterminate = collectArguments(
                    CHAIN_INDETERMINATE,
                    0,
                    indeterminate);

            // field indeterminate
            MethodHandle fieldIndeterminateOperator = typeOperators.getIndeterminateOperator(field.getType(), simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL));

            // (SqlRow, SqlRow):boolean
            indeterminate = insertArguments(indeterminate, 1, fieldId, fieldIndeterminateOperator);

            // (SqlRow):boolean
            indeterminate = permuteArguments(indeterminate, methodType(boolean.class, SqlRow.class), 0, 0);
        }
        return singletonList(new OperatorMethodHandle(INDETERMINATE_CONVENTION, indeterminate));
    }

    private static boolean megamorphicIndeterminateOperator(List<MethodHandle> indeterminateOperators, SqlRow row)
            throws Throwable
    {
        if (row == null) {
            return true;
        }
        int rawIndex = row.getRawIndex();
        for (int fieldIndex = 0; fieldIndex < indeterminateOperators.size(); fieldIndex++) {
            Block fieldBlock = row.getRawFieldBlock(fieldIndex);
            if (!fieldBlock.isNull(rawIndex)) {
                MethodHandle indeterminateOperator = indeterminateOperators.get(fieldIndex);
                if ((boolean) indeterminateOperator.invokeExact(fieldBlock, rawIndex)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean chainIndeterminate(boolean previousFieldIndeterminate, int currentFieldIndex, MethodHandle currentFieldIndeterminateOperator, SqlRow row)
            throws Throwable
    {
        if (row == null || previousFieldIndeterminate) {
            return true;
        }
        int rawIndex = row.getRawIndex();
        Block fieldBlock = row.getRawFieldBlock(currentFieldIndex);
        if (fieldBlock.isNull(rawIndex)) {
            return true;
        }
        return (boolean) currentFieldIndeterminateOperator.invokeExact(fieldBlock, rawIndex);
    }

    private static List<OperatorMethodHandle> getComparisonOperatorInvokers(BiFunction<Type, InvocationConvention, MethodHandle> comparisonOperatorFactory, List<Field> fields)
    {
        boolean orderable = fields.stream().allMatch(field -> field.getType().isOrderable());
        if (!orderable) {
            return emptyList();
        }

        // for large rows, use a generic loop with a megamorphic call site
        if (fields.size() > MEGAMORPHIC_FIELD_COUNT) {
            List<MethodHandle> comparisonOperators = fields.stream()
                    .map(field -> comparisonOperatorFactory.apply(field.getType(), simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL, BLOCK_POSITION_NOT_NULL)))
                    .toList();
            return singletonList(new OperatorMethodHandle(COMPARISON_CONVENTION, COMPARISON.bindTo(comparisonOperators)));
        }

        // (SqlRow, SqlRow):Boolean
        MethodHandle comparison = dropArguments(constant(long.class, 0), 0, SqlRow.class, SqlRow.class);
        for (int fieldId = 0; fieldId < fields.size(); fieldId++) {
            Field field = fields.get(fieldId);
            // (SqlRow, SqlRow, int, MethodHandle, SqlRow, SqlRow):Boolean
            comparison = collectArguments(
                    CHAIN_COMPARISON,
                    0,
                    comparison);

            // field comparison
            MethodHandle fieldComparisonOperator = comparisonOperatorFactory.apply(field.getType(), simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL, BLOCK_POSITION_NOT_NULL));

            // (SqlRow, SqlRow, SqlRow, SqlRow):Boolean
            comparison = insertArguments(comparison, 2, fieldId, fieldComparisonOperator);

            // (SqlRow, SqlRow):Boolean
            comparison = permuteArguments(comparison, methodType(long.class, SqlRow.class, SqlRow.class), 0, 1, 0, 1);
        }
        return singletonList(new OperatorMethodHandle(COMPARISON_CONVENTION, comparison));
    }

    private static long megamorphicComparisonOperator(List<MethodHandle> comparisonOperators, SqlRow leftRow, SqlRow rightRow)
            throws Throwable
    {
        int leftRawIndex = leftRow.getRawIndex();
        int rightRawIndex = rightRow.getRawIndex();

        for (int fieldIndex = 0; fieldIndex < comparisonOperators.size(); fieldIndex++) {
            Block leftFieldBlock = leftRow.getRawFieldBlock(fieldIndex);
            Block rightFieldBlock = rightRow.getRawFieldBlock(fieldIndex);

            checkElementNotNull(leftFieldBlock.isNull(leftRawIndex));
            checkElementNotNull(rightFieldBlock.isNull(rightRawIndex));

            MethodHandle comparisonOperator = comparisonOperators.get(fieldIndex);
            long result = (long) comparisonOperator.invoke(leftFieldBlock, leftRawIndex, rightFieldBlock, rightRawIndex);
            if (result == 0) {
                return result;
            }
        }
        return 0;
    }

    private static long chainComparison(long previousFieldsResult, int fieldIndex, MethodHandle nextFieldComparison, SqlRow leftRow, SqlRow rightRow)
            throws Throwable
    {
        if (previousFieldsResult != 0) {
            return previousFieldsResult;
        }

        int leftRawIndex = leftRow.getRawIndex();
        int rightRawIndex = rightRow.getRawIndex();
        Block leftFieldBlock = leftRow.getRawFieldBlock(fieldIndex);
        Block rightFieldBlock = rightRow.getRawFieldBlock(fieldIndex);

        checkElementNotNull(leftFieldBlock.isNull(leftRawIndex));
        checkElementNotNull(rightFieldBlock.isNull(rightRawIndex));

        return (long) nextFieldComparison.invokeExact(leftFieldBlock, leftRawIndex, rightFieldBlock, rightRawIndex);
    }

    private static void checkElementNotNull(boolean isNull)
    {
        if (isNull) {
            throw new TrinoException(StandardErrorCode.NOT_SUPPORTED, "ROW comparison not supported for fields with null elements");
        }
    }
}
