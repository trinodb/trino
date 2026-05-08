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
package io.trino.operator.join;

import io.trino.spi.block.Bitmap;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;

import java.util.Arrays;
import java.util.Optional;

public final class NullablePositions
{
    private NullablePositions() {}

    /**
     * Returns the indices of non-null positions across {@code nullableBlocks}, or an empty {@link Optional} when
     * every position is non-null. The empty result lets callers that don't need an explicit position list skip the
     * identity-array allocation that would otherwise occur on the all-non-null fast path.
     */
    public static Optional<int[]> getNonNullPositions(Block[] nullableBlocks, int nullableBlocksCount, int positionCount)
    {
        if (nullableBlocksCount == 0) {
            return Optional.empty();
        }
        if (nullableBlocksCount == 1) {
            // Special case for a single nullable block to avoid the need for explicit `boolean[] isNull`
            int[] outputPositions = new int[positionCount];
            int outputPositionsCount = getNonNullPositions(nullableBlocks[0], positionCount, outputPositions);
            if (outputPositionsCount == positionCount) {
                return Optional.empty();
            }
            return Optional.of(Arrays.copyOf(outputPositions, outputPositionsCount));
        }

        boolean[] isNull = new boolean[positionCount];
        for (int i = 0; i < nullableBlocksCount - 1; i++) {
            Block nullableBlock = nullableBlocks[i];
            getNonNullPositions(nullableBlock, isNull);
        }

        int[] outputPositions = new int[positionCount];
        // For the last nullable block, we need to fill outputPositions and count non-null positions
        int outputPositionsCount = getNonNullPositionsLast(nullableBlocks[nullableBlocksCount - 1], isNull, outputPositions);
        if (outputPositionsCount == positionCount) {
            return Optional.empty();
        }
        return Optional.of(Arrays.copyOf(outputPositions, outputPositionsCount));
    }

    private static int getNonNullPositions(Block nullableBlock, int positionCount, int[] outputPositions)
    {
        switch (nullableBlock) {
            case RunLengthEncodedBlock rleBlock -> {
                if (rleBlock.isNull(0)) {
                    return 0; // all positions are null
                }
            }
            case DictionaryBlock dictionaryBlock -> {
                ValueBlock dictionary = dictionaryBlock.getDictionary();
                Optional<Bitmap> dictionaryValidity = dictionary.getValidityBitmap();
                if (dictionaryValidity.isPresent()) {
                    int outputPositionCount = 0;
                    long[] dictionaryIsValid = dictionaryValidity.get().getRawWords();
                    int validityOffset = dictionaryValidity.get().getRawBitOffset();
                    for (int position = 0; position < positionCount; position++) {
                        boolean isNull = !Bitmap.isSet(dictionaryIsValid, validityOffset, dictionaryBlock.getId(position));
                        outputPositions[outputPositionCount] = position;
                        outputPositionCount += isNull ? 0 : 1;
                    }
                    return outputPositionCount;
                }
            }
            case ValueBlock valueBlock -> {
                Optional<Bitmap> validity = valueBlock.getValidityBitmap();
                if (validity.isPresent()) {
                    int outputPositionCount = 0;
                    long[] valueIsValid = validity.get().getRawWords();
                    int validityOffset = validity.get().getRawBitOffset();
                    for (int position = 0; position < positionCount; position++) {
                        outputPositions[outputPositionCount] = position;
                        outputPositionCount += !Bitmap.isSet(valueIsValid, validityOffset, position) ? 0 : 1;
                    }
                    return outputPositionCount;
                }
            }
        }
        for (int position = 0; position < positionCount; position++) {
            outputPositions[position] = position;
        }
        return positionCount;
    }

    private static void getNonNullPositions(Block nullableBlock, boolean[] isNull)
    {
        switch (nullableBlock) {
            case RunLengthEncodedBlock rleBlock -> {
                if (rleBlock.isNull(0)) {
                    Arrays.fill(isNull, true);
                }
            }
            case DictionaryBlock dictionaryBlock -> {
                ValueBlock dictionary = dictionaryBlock.getDictionary();
                Optional<Bitmap> dictionaryValidity = dictionary.getValidityBitmap();
                if (dictionaryValidity.isPresent()) {
                    long[] dictionaryIsValid = dictionaryValidity.get().getRawWords();
                    int validityOffset = dictionaryValidity.get().getRawBitOffset();
                    for (int position = 0; position < isNull.length; position++) {
                        isNull[position] |= !Bitmap.isSet(dictionaryIsValid, validityOffset, dictionaryBlock.getId(position));
                    }
                }
            }
            case ValueBlock valueBlock -> {
                Optional<Bitmap> validity = valueBlock.getValidityBitmap();
                if (validity.isPresent()) {
                    long[] valueIsValid = validity.get().getRawWords();
                    int validityOffset = validity.get().getRawBitOffset();
                    for (int position = 0; position < isNull.length; position++) {
                        isNull[position] |= !Bitmap.isSet(valueIsValid, validityOffset, position);
                    }
                }
            }
        }
    }

    private static int getNonNullPositionsLast(Block nullableBlock, boolean[] isNull, int[] outputPositions)
    {
        int outputPositionCount = 0;
        switch (nullableBlock) {
            case RunLengthEncodedBlock rleBlock -> {
                if (rleBlock.isNull(0)) {
                    return 0;
                }
            }
            case DictionaryBlock dictionaryBlock -> {
                ValueBlock dictionary = dictionaryBlock.getDictionary();
                Optional<Bitmap> dictionaryValidity = dictionary.getValidityBitmap();
                if (dictionaryValidity.isPresent()) {
                    long[] dictionaryIsValid = dictionaryValidity.get().getRawWords();
                    int validityOffset = dictionaryValidity.get().getRawBitOffset();
                    for (int position = 0; position < isNull.length; position++) {
                        isNull[position] |= !Bitmap.isSet(dictionaryIsValid, validityOffset, dictionaryBlock.getId(position));
                        outputPositions[outputPositionCount] = position;
                        outputPositionCount += isNull[position] ? 0 : 1;
                    }
                    return outputPositionCount;
                }
            }
            case ValueBlock valueBlock -> {
                Optional<Bitmap> validity = valueBlock.getValidityBitmap();
                if (validity.isPresent()) {
                    long[] valueIsValid = validity.get().getRawWords();
                    int validityOffset = validity.get().getRawBitOffset();
                    for (int position = 0; position < isNull.length; position++) {
                        isNull[position] |= !Bitmap.isSet(valueIsValid, validityOffset, position);
                        outputPositions[outputPositionCount] = position;
                        outputPositionCount += isNull[position] ? 0 : 1;
                    }
                    return outputPositionCount;
                }
            }
        }
        for (int position = 0; position < isNull.length; position++) {
            outputPositions[outputPositionCount] = position;
            outputPositionCount += isNull[position] ? 0 : 1;
        }
        return outputPositionCount;
    }
}
