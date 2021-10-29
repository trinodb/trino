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
package io.trino.block;

import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.block.ArrayBlock.fromElementBlock;
import static io.trino.spi.block.DictionaryId.randomDictionaryId;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.trino.spi.type.Decimals.encodeUnscaledValue;
import static io.trino.spi.type.Decimals.writeBigDecimal;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.ColorType.COLOR;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.multiplyExact;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

public final class BlockAssertions
{
    private static final int ENTRY_SIZE = 4;
    private static final int MAX_STRING_SIZE = 50;
    private static final int RANDOM_SEED = 633969769;

    private static final Random RANDOM = new Random(RANDOM_SEED);

    private BlockAssertions() {}

    public static Object getOnlyValue(Type type, Block block)
    {
        assertEquals(block.getPositionCount(), 1, "Block positions");
        return type.getObjectValue(SESSION, block, 0);
    }

    public static List<Object> toValues(Type type, Iterable<Block> blocks)
    {
        List<Object> values = new ArrayList<>();
        for (Block block : blocks) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                values.add(type.getObjectValue(SESSION, block, position));
            }
        }
        return unmodifiableList(values);
    }

    public static List<Object> toValues(Type type, Block block)
    {
        List<Object> values = new ArrayList<>();
        for (int position = 0; position < block.getPositionCount(); position++) {
            values.add(type.getObjectValue(SESSION, block, position));
        }
        return unmodifiableList(values);
    }

    public static void assertBlockEquals(Type type, Block actual, Block expected)
    {
        assertEquals(actual.getPositionCount(), expected.getPositionCount());
        for (int position = 0; position < actual.getPositionCount(); position++) {
            assertEquals(type.getObjectValue(SESSION, actual, position), type.getObjectValue(SESSION, expected, position));
        }
    }

    public static DictionaryBlock createRandomDictionaryBlock(Block dictionary, int positionCount)
    {
        checkArgument(dictionary.getPositionCount() > 0, "dictionary position count %s is less than or equal to 0", dictionary.getPositionCount());

        int[] ids = IntStream.range(0, positionCount)
                .map(i -> RANDOM.nextInt(dictionary.getPositionCount()))
                .toArray();
        return new DictionaryBlock(0, positionCount, dictionary, ids, false, randomDictionaryId());
    }

    public static RunLengthEncodedBlock createRandomRleBlock(Block block, int positionCount)
    {
        checkArgument(block.getPositionCount() > 0, format("block positions %d is less than or equal to 0", block.getPositionCount()));
        return new RunLengthEncodedBlock(block.getSingleValueBlock(RANDOM.nextInt(block.getPositionCount())), positionCount);
    }

    public static Block createRandomBlockForType(Type type, int positionCount, float nullRate)
    {
        verifyNullRate(nullRate);

        if (type == BOOLEAN) {
            return createRandomBooleansBlock(positionCount, nullRate);
        }
        if (type == BIGINT) {
            return createRandomLongsBlock(positionCount, nullRate);
        }
        if (type == INTEGER || type == REAL) {
            return createRandomIntsBlock(positionCount, nullRate);
        }
        if (type == SMALLINT) {
            return createRandomSmallintsBlock(positionCount, nullRate);
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            if (decimalType.isShort()) {
                return createRandomLongsBlock(positionCount, nullRate);
            }
            return createRandomLongDecimalsBlock(positionCount, nullRate);
        }
        if (type == VARCHAR) {
            return createRandomStringBlock(positionCount, nullRate, MAX_STRING_SIZE);
        }
        if (type instanceof CharType) {
            return createRandomCharsBlock((CharType) type, positionCount, nullRate);
        }
        if (type == DOUBLE) {
            return createRandomDoublesBlock(positionCount, nullRate);
        }
        if (type == TINYINT) {
            return createRandomTinyintsBlock(positionCount, nullRate);
        }
        if (type == UUID) {
            return createRandomUUIDsBlock(positionCount, nullRate);
        }
        if (type == VARBINARY) {
            return createRandomVarbinariesBlock(positionCount, nullRate);
        }

        return createRandomBlockForNestedType(type, positionCount, nullRate);
    }

    private static Block createRandomBlockForNestedType(Type type, int positionCount, float nullRate)
    {
        // Builds isNull and offsets of size positionCount
        boolean[] isNull = null;
        Set<Integer> nullPositions = null;
        if (nullRate > 0) {
            isNull = new boolean[positionCount];
            nullPositions = chooseNullPositions(positionCount, nullRate);
        }
        int[] offsets = new int[positionCount + 1];

        for (int position = 0; position < positionCount; position++) {
            if (nullRate > 0 && nullPositions.contains(position)) {
                isNull[position] = true;
                offsets[position + 1] = offsets[position];
            }
            else {
                // RowType doesn't need offsets, so we just use 1,
                // for ArrayType and MapType we choose randomly either array length or map size at the current position
                offsets[position + 1] = offsets[position] + (type instanceof RowType ? 1 : RANDOM.nextInt(ENTRY_SIZE) + 1);
            }
        }

        // Builds the nested block of size offsets[positionCount].
        if (type instanceof ArrayType) {
            Block valuesBlock = createRandomBlockForType(((ArrayType) type).getElementType(), offsets[positionCount], nullRate);
            return fromElementBlock(positionCount, Optional.ofNullable(isNull), offsets, valuesBlock);
        }
        if (type instanceof MapType) {
            MapType mapType = (MapType) type;
            Block keyBlock = createRandomBlockForType(mapType.getKeyType(), offsets[positionCount], 0.0f);
            Block valueBlock = createRandomBlockForType(mapType.getValueType(), offsets[positionCount], nullRate);

            return mapType.createBlockFromKeyValue(Optional.ofNullable(isNull), offsets, keyBlock, valueBlock);
        }
        if (type instanceof RowType) {
            List<Type> fieldTypes = type.getTypeParameters();
            Block[] fieldBlocks = new Block[fieldTypes.size()];

            for (int i = 0; i < fieldBlocks.length; i++) {
                fieldBlocks[i] = createRandomBlockForType(fieldTypes.get(i), positionCount, nullRate);
            }

            return RowBlock.fromFieldBlocks(positionCount, Optional.ofNullable(isNull), fieldBlocks);
        }

        throw new IllegalArgumentException(format("type %s is not supported.", type));
    }

    public static Block createRandomBooleansBlock(int positionCount, float nullRate)
    {
        return createBooleansBlock(generateListWithNulls(positionCount, nullRate, RANDOM::nextBoolean));
    }

    public static Block createRandomIntsBlock(int positionCount, float nullRate)
    {
        return createIntsBlock(generateListWithNulls(positionCount, nullRate, RANDOM::nextInt));
    }

    public static Block createRandomLongDecimalsBlock(int positionCount, float nullRate)
    {
        return createLongDecimalsBlock(generateListWithNulls(
                positionCount,
                nullRate,
                () -> String.valueOf(RANDOM.nextLong())));
    }

    public static Block createRandomLongsBlock(int positionCount, int numberOfUniqueValues)
    {
        checkArgument(positionCount >= numberOfUniqueValues, "numberOfUniqueValues must be between 1 and positionCount: %s but was %s", positionCount, numberOfUniqueValues);
        int[] uniqueValues = chooseRandomUnique(positionCount, numberOfUniqueValues).stream()
                .mapToInt(Integer::intValue)
                .toArray();
        return createLongsBlock(IntStream.range(0, positionCount)
                .mapToLong(position -> uniqueValues[RANDOM.nextInt(numberOfUniqueValues)])
                .boxed()
                .collect(toImmutableList()));
    }

    public static Block createRandomLongsBlock(int positionCount, float nullRate)
    {
        return createLongsBlock(generateListWithNulls(positionCount, nullRate, RANDOM::nextLong));
    }

    public static Block createRandomSmallintsBlock(int positionCount, float nullRate)
    {
        return createTypedLongsBlock(
                SMALLINT,
                generateListWithNulls(positionCount, nullRate, () -> (long) (short) RANDOM.nextLong()));
    }

    public static Block createRandomStringBlock(int positionCount, float nullRate, int maxStringLength)
    {
        return createStringsBlock(
                generateListWithNulls(positionCount, nullRate, () -> generateRandomStringWithLength(maxStringLength)));
    }

    private static Block createRandomVarbinariesBlock(int positionCount, float nullRate)
    {
        return createSlicesBlock(VARBINARY, generateListWithNulls(positionCount, nullRate, () -> Slices.wrappedLongArray(RANDOM.nextLong(), RANDOM.nextLong())));
    }

    private static Block createRandomUUIDsBlock(int positionCount, float nullRate)
    {
        return createSlicesBlock(UUID, generateListWithNulls(positionCount, nullRate, () -> Slices.wrappedLongArray(RANDOM.nextLong(), RANDOM.nextLong())));
    }

    private static Block createRandomTinyintsBlock(int positionCount, float nullRate)
    {
        return createTypedLongsBlock(TINYINT, generateListWithNulls(positionCount, nullRate, () -> (long) (byte) RANDOM.nextLong()));
    }

    public static Block createRandomDoublesBlock(int positionCount, float nullRate)
    {
        return createDoublesBlock(generateListWithNulls(positionCount, nullRate, RANDOM::nextDouble));
    }

    public static Block createRandomCharsBlock(CharType charType, int positionCount, float nullRate)
    {
        return createCharsBlock(charType, generateListWithNulls(positionCount, nullRate, () -> generateRandomStringWithLength(charType.getLength())));
    }

    public static <T> List<T> generateListWithNulls(int positionCount, float nullRate, Supplier<T> valueSupplier)
    {
        List<T> result = new ArrayList<>(positionCount);

        Set<Integer> nullPositions = chooseNullPositions(positionCount, nullRate);
        for (int i = 0; i < positionCount; i++) {
            result.add(nullPositions.contains(i) ? null : valueSupplier.get());
        }
        return unmodifiableList(result);
    }

    public static Block createStringsBlock(String... values)
    {
        requireNonNull(values, "values is null");

        return createStringsBlock(Arrays.asList(values));
    }

    public static Block createStringsBlock(Iterable<String> values)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, 100);

        for (String value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                VARCHAR.writeString(builder, value);
            }
        }

        return builder.build();
    }

    public static Block createSlicesBlock(Slice... values)
    {
        requireNonNull(values, "values is null");
        return createSlicesBlock(Arrays.asList(values));
    }

    public static Block createSlicesBlock(Iterable<Slice> values)
    {
        return createSlicesBlock(VARBINARY, values);
    }

    public static Block createSlicesBlock(Type type, Iterable<Slice> values)
    {
        return createBlock(type, type::writeSlice, values);
    }

    public static Block createStringSequenceBlock(int start, int end)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, 100);

        for (int i = start; i < end; i++) {
            VARCHAR.writeString(builder, String.valueOf(i));
        }

        return builder.build();
    }

    public static Block createStringDictionaryBlock(int start, int length)
    {
        checkArgument(length > 5, "block must have more than 5 entries");

        int dictionarySize = length / 5;
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, dictionarySize);
        for (int i = start; i < start + dictionarySize; i++) {
            VARCHAR.writeString(builder, String.valueOf(i));
        }
        int[] ids = new int[length];
        for (int i = 0; i < length; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(builder.build(), ids);
    }

    public static Block createStringArraysBlock(Iterable<? extends Iterable<String>> values)
    {
        ArrayType arrayType = new ArrayType(VARCHAR);
        BlockBuilder builder = arrayType.createBlockBuilder(null, 100);

        for (Iterable<String> value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                arrayType.writeObject(builder, createStringsBlock(value));
            }
        }

        return builder.build();
    }

    public static Block createBooleansBlock(Boolean... values)
    {
        requireNonNull(values, "values is null");

        return createBooleansBlock(Arrays.asList(values));
    }

    public static Block createBooleansBlock(Boolean value, int count)
    {
        return createBooleansBlock(Collections.nCopies(count, value));
    }

    public static Block createBooleansBlock(Iterable<Boolean> values)
    {
        BlockBuilder builder = BOOLEAN.createBlockBuilder(null, 100);

        for (Boolean value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                BOOLEAN.writeBoolean(builder, value);
            }
        }

        return builder.build();
    }

    public static Block createShortDecimalsBlock(String... values)
    {
        requireNonNull(values, "values is null");

        return createShortDecimalsBlock(Arrays.asList(values));
    }

    public static Block createShortDecimalsBlock(Iterable<String> values)
    {
        DecimalType shortDecimalType = DecimalType.createDecimalType(1);
        BlockBuilder builder = shortDecimalType.createBlockBuilder(null, 100);

        for (String value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                shortDecimalType.writeLong(builder, new BigDecimal(value).unscaledValue().longValue());
            }
        }

        return builder.build();
    }

    public static Block createLongDecimalsBlock(String... values)
    {
        requireNonNull(values, "values is null");

        return createLongDecimalsBlock(Arrays.asList(values));
    }

    public static Block createLongDecimalsBlock(Iterable<String> values)
    {
        DecimalType longDecimalType = DecimalType.createDecimalType(MAX_SHORT_PRECISION + 1);
        BlockBuilder builder = longDecimalType.createBlockBuilder(null, 100);

        for (String value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                writeBigDecimal(longDecimalType, builder, new BigDecimal(value));
            }
        }

        return builder.build();
    }

    public static Block createCharsBlock(CharType charType, List<String> values)
    {
        return createBlock(charType, charType::writeString, values);
    }

    public static Block createIntsBlock(Integer... values)
    {
        requireNonNull(values, "values is null");

        return createIntsBlock(Arrays.asList(values));
    }

    public static Block createIntsBlock(Iterable<Integer> values)
    {
        return createBlock(INTEGER, (ValueWriter<Integer>) INTEGER::writeLong, values);
    }

    public static Block createRowBlock(List<Type> fieldTypes, Object[]... rows)
    {
        BlockBuilder rowBlockBuilder = new RowBlockBuilder(fieldTypes, null, 1);
        for (Object[] row : rows) {
            if (row == null) {
                rowBlockBuilder.appendNull();
                continue;
            }
            BlockBuilder singleRowBlockWriter = rowBlockBuilder.beginBlockEntry();
            for (Object fieldValue : row) {
                if (fieldValue == null) {
                    singleRowBlockWriter.appendNull();
                    continue;
                }

                if (fieldValue instanceof String) {
                    VARCHAR.writeSlice(singleRowBlockWriter, utf8Slice((String) fieldValue));
                }
                else if (fieldValue instanceof Slice) {
                    VARBINARY.writeSlice(singleRowBlockWriter, (Slice) fieldValue);
                }
                else if (fieldValue instanceof Double) {
                    DOUBLE.writeDouble(singleRowBlockWriter, (Double) fieldValue);
                }
                else if (fieldValue instanceof Long) {
                    BIGINT.writeLong(singleRowBlockWriter, (Long) fieldValue);
                }
                else if (fieldValue instanceof Boolean) {
                    BOOLEAN.writeBoolean(singleRowBlockWriter, (Boolean) fieldValue);
                }
                else if (fieldValue instanceof Block) {
                    singleRowBlockWriter.appendStructure((Block) fieldValue);
                }
                else if (fieldValue instanceof Integer) {
                    INTEGER.writeLong(singleRowBlockWriter, (Integer) fieldValue);
                }
                else {
                    throw new IllegalArgumentException();
                }
            }
            rowBlockBuilder.closeEntry();
        }

        return rowBlockBuilder.build();
    }

    public static Block createEmptyLongsBlock()
    {
        return BIGINT.createFixedSizeBlockBuilder(0).build();
    }

    // This method makes it easy to create blocks without having to add an L to every value
    public static Block createLongsBlock(int... values)
    {
        BlockBuilder builder = BIGINT.createBlockBuilder(null, 100);

        for (int value : values) {
            BIGINT.writeLong(builder, value);
        }

        return builder.build();
    }

    public static Block createLongsBlock(Long... values)
    {
        requireNonNull(values, "values is null");

        return createLongsBlock(Arrays.asList(values));
    }

    public static Block createLongsBlock(Iterable<Long> values)
    {
        return createTypedLongsBlock(BIGINT, values);
    }

    public static Block createTypedLongsBlock(Type type, Iterable<Long> values)
    {
        return createBlock(type, type::writeLong, values);
    }

    public static Block createLongSequenceBlock(int start, int end)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(end - start);

        for (int i = start; i < end; i++) {
            BIGINT.writeLong(builder, i);
        }

        return builder.build();
    }

    public static Block createLongDictionaryBlock(int start, int length)
    {
        checkArgument(length > 5, "block must have more than 5 entries");
        return createLongDictionaryBlock(start, length, length / 5);
    }

    public static Block createLongDictionaryBlock(int start, int length, int dictionarySize)
    {
        checkArgument(dictionarySize > 0, "dictionarySize must be greater than 0");

        BlockBuilder builder = BIGINT.createBlockBuilder(null, dictionarySize);
        for (int i = start; i < start + dictionarySize; i++) {
            BIGINT.writeLong(builder, i);
        }
        int[] ids = new int[length];
        for (int i = 0; i < length; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(builder.build(), ids);
    }

    public static Block createLongRepeatBlock(int value, int length)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(length);
        for (int i = 0; i < length; i++) {
            BIGINT.writeLong(builder, value);
        }
        return builder.build();
    }

    public static Block createDoubleRepeatBlock(double value, int length)
    {
        BlockBuilder builder = DOUBLE.createFixedSizeBlockBuilder(length);
        for (int i = 0; i < length; i++) {
            DOUBLE.writeDouble(builder, value);
        }
        return builder.build();
    }

    public static Block createTimestampsWithTimeZoneMillisBlock(Long... values)
    {
        BlockBuilder builder = TIMESTAMP_TZ_MILLIS.createFixedSizeBlockBuilder(values.length);
        for (long value : values) {
            TIMESTAMP_TZ_MILLIS.writeLong(builder, value);
        }
        return builder.build();
    }

    public static Block createBooleanSequenceBlock(int start, int end)
    {
        BlockBuilder builder = BOOLEAN.createFixedSizeBlockBuilder(end - start);

        for (int i = start; i < end; i++) {
            BOOLEAN.writeBoolean(builder, i % 2 == 0);
        }

        return builder.build();
    }

    public static Block createBlockOfReals(Float... values)
    {
        requireNonNull(values, "values is null");

        return createBlockOfReals(Arrays.asList(values));
    }

    public static Block createBlockOfReals(Iterable<Float> values)
    {
        BlockBuilder builder = REAL.createBlockBuilder(null, 100);
        for (Float value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                REAL.writeLong(builder, floatToRawIntBits(value));
            }
        }
        return builder.build();
    }

    public static Block createSequenceBlockOfReal(int start, int end)
    {
        BlockBuilder builder = REAL.createFixedSizeBlockBuilder(end - start);

        for (int i = start; i < end; i++) {
            REAL.writeLong(builder, floatToRawIntBits(i));
        }

        return builder.build();
    }

    public static Block createDoublesBlock(Double... values)
    {
        requireNonNull(values, "values is null");

        return createDoublesBlock(Arrays.asList(values));
    }

    public static Block createDoublesBlock(Iterable<Double> values)
    {
        return createBlock(DOUBLE, DOUBLE::writeDouble, values);
    }

    public static Block createDoubleSequenceBlock(int start, int end)
    {
        BlockBuilder builder = DOUBLE.createFixedSizeBlockBuilder(end - start);

        for (int i = start; i < end; i++) {
            DOUBLE.writeDouble(builder, i);
        }

        return builder.build();
    }

    public static Block createArrayBigintBlock(Iterable<? extends Iterable<Long>> values)
    {
        ArrayType arrayType = new ArrayType(BIGINT);
        BlockBuilder builder = arrayType.createBlockBuilder(null, 100);

        for (Iterable<Long> value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                arrayType.writeObject(builder, createLongsBlock(value));
            }
        }

        return builder.build();
    }

    public static Block createDateSequenceBlock(int start, int end)
    {
        BlockBuilder builder = DATE.createFixedSizeBlockBuilder(end - start);

        for (int i = start; i < end; i++) {
            DATE.writeLong(builder, i);
        }

        return builder.build();
    }

    public static Block createTimestampSequenceBlock(int start, int end)
    {
        BlockBuilder builder = TIMESTAMP_MILLIS.createFixedSizeBlockBuilder(end - start);

        for (int i = start; i < end; i++) {
            TIMESTAMP_MILLIS.writeLong(builder, multiplyExact(i, MICROSECONDS_PER_MILLISECOND));
        }

        return builder.build();
    }

    public static Block createShortDecimalSequenceBlock(int start, int end, DecimalType type)
    {
        BlockBuilder builder = type.createFixedSizeBlockBuilder(end - start);
        long base = BigInteger.TEN.pow(type.getScale()).longValue();

        for (int i = start; i < end; ++i) {
            type.writeLong(builder, base * i);
        }

        return builder.build();
    }

    public static Block createLongDecimalSequenceBlock(int start, int end, DecimalType type)
    {
        BlockBuilder builder = type.createFixedSizeBlockBuilder(end - start);
        BigInteger base = BigInteger.TEN.pow(type.getScale());

        for (int i = start; i < end; ++i) {
            type.writeSlice(builder, encodeUnscaledValue(BigInteger.valueOf(i).multiply(base)));
        }

        return builder.build();
    }

    public static Block createColorRepeatBlock(int value, int length)
    {
        BlockBuilder builder = COLOR.createFixedSizeBlockBuilder(length);
        for (int i = 0; i < length; i++) {
            COLOR.writeLong(builder, value);
        }
        return builder.build();
    }

    public static Block createColorSequenceBlock(int start, int end)
    {
        BlockBuilder builder = COLOR.createBlockBuilder(null, end - start);
        for (int i = start; i < end; ++i) {
            COLOR.writeLong(builder, i);
        }
        return builder.build();
    }

    public static RunLengthEncodedBlock createRLEBlock(double value, int positionCount)
    {
        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(null, 1);
        DOUBLE.writeDouble(blockBuilder, value);
        return new RunLengthEncodedBlock(blockBuilder.build(), positionCount);
    }

    public static RunLengthEncodedBlock createRLEBlock(long value, int positionCount)
    {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, 1);
        BIGINT.writeLong(blockBuilder, value);
        return new RunLengthEncodedBlock(blockBuilder.build(), positionCount);
    }

    private static <T> Block createBlock(Type type, ValueWriter<T> valueWriter, Iterable<T> values)
    {
        BlockBuilder builder = type.createBlockBuilder(null, 100);

        for (T value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                valueWriter.write(builder, value);
            }
        }

        return builder.build();
    }

    private interface ValueWriter<T>
    {
        void write(BlockBuilder builder, T value);
    }

    private static Set<Integer> chooseNullPositions(int positionCount, float nullRate)
    {
        int nullCount = (int) (positionCount * nullRate);
        if (nullCount == 0) {
            verify(nullRate == 0, "position count %s too small to have at least one null with rate %s", (Object) positionCount, nullRate);
            return ImmutableSet.of();
        }
        return chooseRandomUnique(positionCount, nullCount);
    }

    private static Set<Integer> chooseRandomUnique(int bound, int count)
    {
        if (count < bound / 10) {
            // it's an order of bound/count faster to use this method for small enough count/bound ratio
            Set<Integer> values = new HashSet<>(count);
            while (values.size() < count) {
                values.add(RANDOM.nextInt(bound));
            }
            return ImmutableSet.copyOf(values);
        }

        List<Integer> allNumbers = IntStream.range(0, bound).boxed().collect(toList());
        Collections.shuffle(allNumbers, RANDOM);
        return allNumbers.stream().limit(count).collect(toImmutableSet());
    }

    private static String generateRandomStringWithLength(int length)
    {
        String symbols = "abcdefghijklmnopqrstuvwxyz";
        char[] chars = new char[length];
        for (int i = 0; i < length; i++) {
            chars[i] = symbols.charAt(RANDOM.nextInt(symbols.length()));
        }
        return new String(chars);
    }

    private static void verifyNullRate(float nullRate)
    {
        verify(nullRate >= 0 && nullRate <= 1, "nullRate %s is not valid", nullRate);
    }
}
