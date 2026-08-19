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

import static io.trino.spi.block.Bitmap.allocateWords;
import static io.trino.spi.block.Bitmap.getBits;
import static io.trino.spi.block.Bitmap.hasUnsetBit;
import static io.trino.spi.block.Bitmap.isSet;
import static io.trino.sql.gen.columnar.ColumnarFilterUtils.filterSetBitsRange;
import static java.lang.Math.min;

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
            // Special case for a single nullable block to avoid the need for an explicit validity accumulator
            int[] outputPositions = new int[positionCount];
            int outputPositionsCount = getNonNullPositions(nullableBlocks[0], positionCount, outputPositions);
            if (outputPositionsCount == positionCount) {
                return Optional.empty();
            }
            return Optional.of(Arrays.copyOf(outputPositions, outputPositionsCount));
        }

        long[] valueIsValid = allocateWords(positionCount, true);
        for (int i = 0; i < nullableBlocksCount; i++) {
            retainNonNullPositions(nullableBlocks[i], valueIsValid, positionCount);
        }

        int[] outputPositions = new int[positionCount];
        int outputPositionsCount = filterSetBitsRange(valueIsValid, 0, 0, positionCount, outputPositions);
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
                if (hasNulls(dictionary)) {
                    int outputPositionCount = 0;
                    if (positionCount >= dictionary.getPositionCount()) {
                        // The page dominates the dictionary, so the decode amortizes and the
                        // gather is an id lookup plus a byte load
                        byte[] dictionaryIsValid = decodeValidity(dictionary);
                        for (int position = 0; position < positionCount; position++) {
                            outputPositions[outputPositionCount] = position;
                            outputPositionCount += dictionaryIsValid[dictionaryBlock.getId(position)];
                        }
                    }
                    else {
                        // The dictionary dominates the page, so read the bits directly instead of
                        // paying a decode proportional to the dictionary size
                        Bitmap validityBitmap = dictionary.getValidityBitmap().orElseThrow();
                        long[] words = validityBitmap.getRawWords();
                        int offset = validityBitmap.getRawBitOffset();
                        for (int position = 0; position < positionCount; position++) {
                            int bit = offset + dictionaryBlock.getId(position);
                            outputPositions[outputPositionCount] = position;
                            // Branchless: the validity bit is extracted as 0 or 1 and added directly
                            outputPositionCount += (int) ((words[bit >>> 6] >>> (bit & 63)) & 1);
                        }
                    }
                    return outputPositionCount;
                }
            }
            case ValueBlock valueBlock -> {
                Optional<Bitmap> validity = valueBlock.getValidityBitmap();
                if (validity.isPresent()) {
                    Bitmap bitmap = validity.get();
                    return filterSetBitsRange(bitmap.getRawWords(), bitmap.getRawBitOffset(), 0, positionCount, outputPositions);
                }
            }
        }
        for (int position = 0; position < positionCount; position++) {
            outputPositions[position] = position;
        }
        return positionCount;
    }

    /**
     * Clears the accumulator bits of positions that are null in {@code nullableBlock}, a word at a time.
     */
    private static void retainNonNullPositions(Block nullableBlock, long[] valueIsValid, int positionCount)
    {
        switch (nullableBlock) {
            case RunLengthEncodedBlock rleBlock -> {
                if (rleBlock.isNull(0)) {
                    Arrays.fill(valueIsValid, 0);
                }
            }
            case DictionaryBlock dictionaryBlock -> {
                ValueBlock dictionary = dictionaryBlock.getDictionary();
                if (hasNulls(dictionary)) {
                    if (positionCount >= dictionary.getPositionCount()) {
                        // The page dominates the dictionary, so the decode amortizes and the
                        // gather is an id lookup plus a byte load
                        byte[] dictionaryIsValid = decodeValidity(dictionary);
                        // Gather a whole word before writing, so the accumulator is written once per 64 positions
                        for (int position = 0, word = 0; position < positionCount; position += Long.SIZE, word++) {
                            int bitsInWord = min(Long.SIZE, positionCount - position);
                            long bits = 0;
                            for (int bit = 0; bit < bitsInWord; bit++) {
                                // Branchless: the decoded byte is 0 or 1, so it can be shifted straight into place
                                bits |= ((long) dictionaryIsValid[dictionaryBlock.getId(position + bit)]) << bit;
                            }
                            valueIsValid[word] &= bits;
                        }
                    }
                    else {
                        // The dictionary dominates the page, so read the bits directly instead of
                        // paying a decode proportional to the dictionary size
                        long[] words = dictionary.getValidityBitmap().orElseThrow().getRawWords();
                        int offset = dictionary.getValidityBitmap().orElseThrow().getRawBitOffset();
                        for (int position = 0, word = 0; position < positionCount; position += Long.SIZE, word++) {
                            int bitsInWord = min(Long.SIZE, positionCount - position);
                            long bits = 0;
                            for (int bit = 0; bit < bitsInWord; bit++) {
                                int validityBit = offset + dictionaryBlock.getId(position + bit);
                                // Branchless: the validity bit is extracted as 0 or 1 and shifted into place
                                bits |= ((words[validityBit >>> 6] >>> (validityBit & 63)) & 1L) << bit;
                            }
                            valueIsValid[word] &= bits;
                        }
                    }
                }
            }
            case ValueBlock valueBlock -> {
                Optional<Bitmap> validity = valueBlock.getValidityBitmap();
                if (validity.isPresent()) {
                    Bitmap bitmap = validity.get();
                    long[] blockIsValid = bitmap.getRawWords();
                    int rawBitOffset = bitmap.getRawBitOffset();
                    for (int position = 0, word = 0; position < positionCount; position += Long.SIZE, word++) {
                        valueIsValid[word] &= getBits(blockIsValid, rawBitOffset, position, min(Long.SIZE, positionCount - position));
                    }
                }
            }
        }
    }

    /**
     * Decodes validity into one byte per position holding 0 or 1, so gathers through dictionary ids stay
     * branchless: the byte can be shifted or added directly instead of driving a conditional.
     */
    private static byte[] decodeValidity(ValueBlock block)
    {
        Bitmap bitmap = block.getValidityBitmap().orElseThrow();
        long[] rawWords = bitmap.getRawWords();
        int rawBitOffset = bitmap.getRawBitOffset();
        byte[] valueIsValid = new byte[block.getPositionCount()];
        for (int position = 0; position < valueIsValid.length; position++) {
            valueIsValid[position] = (byte) (isSet(rawWords, rawBitOffset, position) ? 1 : 0);
        }
        return valueIsValid;
    }

    private static boolean hasNulls(ValueBlock block)
    {
        Optional<Bitmap> validity = block.getValidityBitmap();
        return validity.isPresent() && hasUnsetBit(validity.get().getRawWords(), validity.get().getRawBitOffset(), block.getPositionCount());
    }
}
