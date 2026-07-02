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
package io.trino.orc.reader;

import io.trino.orc.OrcColumn;
import io.trino.orc.OrcCorruptionException;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.type.Type;

import java.util.Arrays;
import java.util.function.Predicate;

import static io.trino.spi.block.Bitmap.countTransitions;
import static io.trino.spi.block.Bitmap.getBits;
import static io.trino.spi.block.Bitmap.isSet;
import static java.lang.Long.bitCount;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;
import static java.util.Objects.requireNonNull;

final class ReaderUtils
{
    private static final int RANDOM_TRANSITION_THRESHOLD = 12;

    private ReaderUtils() {}

    public static void verifyStreamType(OrcColumn column, Type actual, Predicate<Type> validTypes)
            throws OrcCorruptionException
    {
        if (!validTypes.test(actual)) {
            throw invalidStreamType(column, actual);
        }
    }

    public static OrcCorruptionException invalidStreamType(OrcColumn column, Type type)
            throws OrcCorruptionException
    {
        throw new OrcCorruptionException(
                column.getOrcDataSourceId(),
                "Cannot read SQL type '%s' from ORC stream '%s' of type %s with attributes %s",
                type,
                column.getPath(),
                column.getColumnType().getOrcTypeKind(),
                column.getAttributes());
    }

    public static int minNonNullValueSize(int nonNullCount)
    {
        return max(nonNullCount + 1, 1025);
    }

    public static byte[] unpackByteNulls(byte[] values, long[] valueIsValid, int positionCount)
    {
        byte[] result = new byte[positionCount];

        int inputPosition = 0;
        int outputPosition = 0;
        while (outputPosition < positionCount) {
            int bitsInWord = min(Long.SIZE, positionCount - outputPosition);
            long validBits = getBits(valueIsValid, 0, outputPosition, bitsInWord);
            if (validBits == 0) {
                Arrays.fill(result, outputPosition, outputPosition + bitsInWord, values[inputPosition]);
                outputPosition += bitsInWord;
                continue;
            }
            if (bitCount(validBits) == bitsInWord) {
                arraycopy(values, inputPosition, result, outputPosition, bitsInWord);
                inputPosition += bitsInWord;
                outputPosition += bitsInWord;
                continue;
            }
            if (countTransitions(validBits, bitsInWord) >= RANDOM_TRANSITION_THRESHOLD) {
                int endOutputPosition = outputPosition + bitsInWord;
                while (outputPosition < endOutputPosition) {
                    result[outputPosition] = values[inputPosition];
                    inputPosition += (int) (validBits & 1);
                    outputPosition++;
                    validBits >>>= 1;
                }
                continue;
            }

            int offsetInWord = 0;
            while (offsetInWord < bitsInWord) {
                int nullCount = min(Long.numberOfTrailingZeros(validBits), bitsInWord - offsetInWord);
                Arrays.fill(result, outputPosition, outputPosition + nullCount, values[inputPosition]);
                outputPosition += nullCount;
                offsetInWord += nullCount;
                validBits >>>= nullCount;
                if (offsetInWord == bitsInWord) {
                    break;
                }

                int validCount = min(Long.numberOfTrailingZeros(~validBits), bitsInWord - offsetInWord);
                arraycopy(values, inputPosition, result, outputPosition, validCount);
                inputPosition += validCount;
                outputPosition += validCount;
                offsetInWord += validCount;
                validBits >>>= validCount;
            }
        }
        return result;
    }

    public static short[] unpackShortNulls(short[] values, long[] valueIsValid, int positionCount)
    {
        short[] result = new short[positionCount];

        int inputPosition = 0;
        int outputPosition = 0;
        while (outputPosition < positionCount) {
            int bitsInWord = min(Long.SIZE, positionCount - outputPosition);
            long validBits = getBits(valueIsValid, 0, outputPosition, bitsInWord);
            if (validBits == 0) {
                Arrays.fill(result, outputPosition, outputPosition + bitsInWord, values[inputPosition]);
                outputPosition += bitsInWord;
                continue;
            }
            if (bitCount(validBits) == bitsInWord) {
                arraycopy(values, inputPosition, result, outputPosition, bitsInWord);
                inputPosition += bitsInWord;
                outputPosition += bitsInWord;
                continue;
            }
            if (countTransitions(validBits, bitsInWord) >= RANDOM_TRANSITION_THRESHOLD) {
                int endOutputPosition = outputPosition + bitsInWord;
                while (outputPosition < endOutputPosition) {
                    result[outputPosition] = values[inputPosition];
                    inputPosition += (int) (validBits & 1);
                    outputPosition++;
                    validBits >>>= 1;
                }
                continue;
            }

            int offsetInWord = 0;
            while (offsetInWord < bitsInWord) {
                int nullCount = min(Long.numberOfTrailingZeros(validBits), bitsInWord - offsetInWord);
                Arrays.fill(result, outputPosition, outputPosition + nullCount, values[inputPosition]);
                outputPosition += nullCount;
                offsetInWord += nullCount;
                validBits >>>= nullCount;
                if (offsetInWord == bitsInWord) {
                    break;
                }

                int validCount = min(Long.numberOfTrailingZeros(~validBits), bitsInWord - offsetInWord);
                arraycopy(values, inputPosition, result, outputPosition, validCount);
                inputPosition += validCount;
                outputPosition += validCount;
                offsetInWord += validCount;
                validBits >>>= validCount;
            }
        }
        return result;
    }

    public static int[] unpackIntNulls(int[] values, long[] valueIsValid, int positionCount)
    {
        int[] result = new int[positionCount];

        int inputPosition = 0;
        int outputPosition = 0;
        while (outputPosition < positionCount) {
            int bitsInWord = min(Long.SIZE, positionCount - outputPosition);
            long validBits = getBits(valueIsValid, 0, outputPosition, bitsInWord);
            if (validBits == 0) {
                Arrays.fill(result, outputPosition, outputPosition + bitsInWord, values[inputPosition]);
                outputPosition += bitsInWord;
                continue;
            }
            if (bitCount(validBits) == bitsInWord) {
                arraycopy(values, inputPosition, result, outputPosition, bitsInWord);
                inputPosition += bitsInWord;
                outputPosition += bitsInWord;
                continue;
            }
            if (countTransitions(validBits, bitsInWord) >= RANDOM_TRANSITION_THRESHOLD) {
                int endOutputPosition = outputPosition + bitsInWord;
                while (outputPosition < endOutputPosition) {
                    result[outputPosition] = values[inputPosition];
                    inputPosition += (int) (validBits & 1);
                    outputPosition++;
                    validBits >>>= 1;
                }
                continue;
            }

            int offsetInWord = 0;
            while (offsetInWord < bitsInWord) {
                int nullCount = min(Long.numberOfTrailingZeros(validBits), bitsInWord - offsetInWord);
                Arrays.fill(result, outputPosition, outputPosition + nullCount, values[inputPosition]);
                outputPosition += nullCount;
                offsetInWord += nullCount;
                validBits >>>= nullCount;
                if (offsetInWord == bitsInWord) {
                    break;
                }

                int validCount = min(Long.numberOfTrailingZeros(~validBits), bitsInWord - offsetInWord);
                arraycopy(values, inputPosition, result, outputPosition, validCount);
                inputPosition += validCount;
                outputPosition += validCount;
                offsetInWord += validCount;
                validBits >>>= validCount;
            }
        }
        return result;
    }

    public static long[] unpackLongNulls(long[] values, long[] valueIsValid, int positionCount)
    {
        long[] result = new long[positionCount];

        int inputPosition = 0;
        int outputPosition = 0;
        while (outputPosition < positionCount) {
            int bitsInWord = min(Long.SIZE, positionCount - outputPosition);
            long validBits = getBits(valueIsValid, 0, outputPosition, bitsInWord);
            if (validBits == 0) {
                Arrays.fill(result, outputPosition, outputPosition + bitsInWord, values[inputPosition]);
                outputPosition += bitsInWord;
                continue;
            }
            if (bitCount(validBits) == bitsInWord) {
                arraycopy(values, inputPosition, result, outputPosition, bitsInWord);
                inputPosition += bitsInWord;
                outputPosition += bitsInWord;
                continue;
            }
            if (countTransitions(validBits, bitsInWord) >= RANDOM_TRANSITION_THRESHOLD) {
                int endOutputPosition = outputPosition + bitsInWord;
                while (outputPosition < endOutputPosition) {
                    result[outputPosition] = values[inputPosition];
                    inputPosition += (int) (validBits & 1);
                    outputPosition++;
                    validBits >>>= 1;
                }
                continue;
            }

            int offsetInWord = 0;
            while (offsetInWord < bitsInWord) {
                int nullCount = min(Long.numberOfTrailingZeros(validBits), bitsInWord - offsetInWord);
                Arrays.fill(result, outputPosition, outputPosition + nullCount, values[inputPosition]);
                outputPosition += nullCount;
                offsetInWord += nullCount;
                validBits >>>= nullCount;
                if (offsetInWord == bitsInWord) {
                    break;
                }

                int validCount = min(Long.numberOfTrailingZeros(~validBits), bitsInWord - offsetInWord);
                arraycopy(values, inputPosition, result, outputPosition, validCount);
                inputPosition += validCount;
                outputPosition += validCount;
                offsetInWord += validCount;
                validBits >>>= validCount;
            }
        }
        return result;
    }

    public static long[] unpackInt128Nulls(long[] values, long[] valueIsValid, int positionCount)
    {
        long[] result = new long[positionCount * 2];

        int inputPosition = 0;
        int outputPosition = 0;
        while (outputPosition < positionCount) {
            int bitsInWord = min(Long.SIZE, positionCount - outputPosition);
            long validBits = getBits(valueIsValid, 0, outputPosition, bitsInWord);
            if (validBits == 0) {
                fillInt128Nulls(result, outputPosition, outputPosition + bitsInWord, values, inputPosition);
                outputPosition += bitsInWord;
                continue;
            }
            if (bitCount(validBits) == bitsInWord) {
                arraycopy(values, inputPosition, result, outputPosition * 2, bitsInWord * 2);
                inputPosition += bitsInWord * 2;
                outputPosition += bitsInWord;
                continue;
            }
            if (countTransitions(validBits, bitsInWord) >= RANDOM_TRANSITION_THRESHOLD) {
                int endOutputPosition = outputPosition + bitsInWord;
                while (outputPosition < endOutputPosition) {
                    int outputIndex = outputPosition * 2;
                    result[outputIndex] = values[inputPosition];
                    result[outputIndex + 1] = values[inputPosition + 1];
                    inputPosition += (int) (validBits & 1) * 2;
                    outputPosition++;
                    validBits >>>= 1;
                }
                continue;
            }

            int offsetInWord = 0;
            while (offsetInWord < bitsInWord) {
                int nullCount = min(Long.numberOfTrailingZeros(validBits), bitsInWord - offsetInWord);
                fillInt128Nulls(result, outputPosition, outputPosition + nullCount, values, inputPosition);
                outputPosition += nullCount;
                offsetInWord += nullCount;
                validBits >>>= nullCount;
                if (offsetInWord == bitsInWord) {
                    break;
                }

                int validCount = min(Long.numberOfTrailingZeros(~validBits), bitsInWord - offsetInWord);
                arraycopy(values, inputPosition, result, outputPosition * 2, validCount * 2);
                inputPosition += validCount * 2;
                outputPosition += validCount;
                offsetInWord += validCount;
                validBits >>>= validCount;
            }
        }
        return result;
    }

    public static void unpackLengthNulls(int[] values, long[] valueIsValid, int positionCount, int nonNullCount)
    {
        int nullSuppressedPosition = nonNullCount - 1;
        int outputPosition = positionCount;
        while (outputPosition > 0) {
            int bitsInWord = min(Long.SIZE, outputPosition);
            int wordStart = outputPosition - bitsInWord;
            long validBits = getBits(valueIsValid, 0, wordStart, bitsInWord);
            if (validBits == 0) {
                Arrays.fill(values, wordStart, outputPosition, 0);
                outputPosition = wordStart;
                continue;
            }
            if (bitCount(validBits) == bitsInWord) {
                int sourcePosition = nullSuppressedPosition - bitsInWord + 1;
                arraycopy(values, sourcePosition, values, wordStart, bitsInWord);
                nullSuppressedPosition -= bitsInWord;
                outputPosition = wordStart;
                continue;
            }

            for (int position = bitsInWord - 1; position >= 0; position--) {
                if ((validBits & (1L << position)) != 0) {
                    values[wordStart + position] = values[nullSuppressedPosition];
                    nullSuppressedPosition--;
                }
                else {
                    values[wordStart + position] = 0;
                }
            }
            outputPosition = wordStart;
        }
    }

    private static void fillInt128Nulls(long[] result, int fromPosition, int toPosition, long[] values, int valuePosition)
    {
        long low = values[valuePosition];
        long high = values[valuePosition + 1];
        for (int position = fromPosition; position < toPosition; position++) {
            int outputIndex = position * 2;
            result[outputIndex] = low;
            result[outputIndex + 1] = high;
        }
    }

    public static void convertLengthVectorToOffsetVector(int[] vector)
    {
        int currentLength = vector[0];
        vector[0] = 0;
        for (int i = 1; i < vector.length; i++) {
            int nextLength = vector[i];
            vector[i] = vector[i - 1] + currentLength;
            currentLength = nextLength;
        }
    }

    static Block toNotNullSupressedBlock(int positionCount, long[] rowIsValid, Block fieldBlock)
    {
        requireNonNull(rowIsValid, "rowIsValid is null");
        requireNonNull(fieldBlock, "fieldBlock is null");

        // find an existing position in the block that is null
        int nullIndex = -1;
        if (fieldBlock.mayHaveNull()) {
            for (int position = 0; position < fieldBlock.getPositionCount(); position++) {
                if (fieldBlock.isNull(position)) {
                    nullIndex = position;
                    break;
                }
            }
        }
        // if there are no null positions, append a null to the end of the block
        if (nullIndex == -1) {
            nullIndex = fieldBlock.getPositionCount();
            fieldBlock = fieldBlock.copyWithAppendedNull();
        }

        // create a dictionary that maps null positions to the null index
        int[] dictionaryIds = new int[positionCount];
        int nullSuppressedPosition = 0;
        for (int position = 0; position < positionCount; position++) {
            if (isSet(rowIsValid, 0, position)) {
                dictionaryIds[position] = nullSuppressedPosition;
                nullSuppressedPosition++;
            }
            else {
                dictionaryIds[position] = nullIndex;
            }
        }
        return DictionaryBlock.create(positionCount, fieldBlock, dictionaryIds);
    }
}
