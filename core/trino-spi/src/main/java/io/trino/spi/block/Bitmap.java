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
package io.trino.spi.block;

import jakarta.annotation.Nullable;

import java.util.Arrays;

import static io.trino.spi.block.BlockUtil.checkValidPosition;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/// A view over a bit-packed boolean vector stored as raw `long` words.
///
/// Logical position `position` is stored at raw bit index `rawBitOffset + position`. The raw word index is
/// `rawBitIndex / 64`, and the bit index within that word is `rawBitIndex % 64`. Bit index zero is the
/// least-significant bit of the word, so `rawWords[0] & 1` contains raw bit zero.
///
/// A set bit means `true`; an unset bit means `false`. When a bitmap is used for block validity, a set bit means the
/// value is present and non-null. Validity uses this polarity instead of a null bitmap so the bits align with the Java
/// Vector API mask convention, where a set lane is selected for processing.
///
/// Bits outside `bitCount` are not part of this view and are unspecified. The raw words are underlying storage and may
/// be shared with a block; callers must not mutate them unless they own the array. Sliced blocks may expose a non-zero
/// raw bit offset, so callers that use the raw words directly must also account for `rawBitOffset`.
public final class Bitmap
{
    private static final int ADDRESS_BITS_PER_WORD = 6;
    private static final int BITS_PER_WORD = 1 << ADDRESS_BITS_PER_WORD;
    private static final int BIT_INDEX_MASK = BITS_PER_WORD - 1;

    private final long[] rawWords;
    private final int rawBitOffset;
    private final int bitCount;

    public Bitmap(long[] rawWords, int rawBitOffset, int bitCount)
    {
        this.rawWords = requireNonNull(rawWords, "rawWords is null");
        checkBitRange(rawWords, rawBitOffset, bitCount);
        this.rawBitOffset = rawBitOffset;
        this.bitCount = bitCount;
    }

    /// Returns the raw bitmap words. See [Bitmap] for the bit order and offset rules.
    public long[] getRawWords()
    {
        return rawWords;
    }

    /// Returns the raw bit offset of logical position zero within [getRawWords()].
    public int getRawBitOffset()
    {
        return rawBitOffset;
    }

    /// Returns the number of logical bits in this bitmap view.
    public int getBitCount()
    {
        return bitCount;
    }

    /// Returns whether the bit at the specified logical position is set.
    public boolean isSet(int position)
    {
        checkValidPosition(position, bitCount);
        return isSet(rawWords, rawBitOffset, position);
    }

    /// Returns the number of raw words required to store `bitCount` bits.
    public static int wordsForBits(int bitCount)
    {
        if (bitCount < 0) {
            throw new IllegalArgumentException("bitCount is negative");
        }
        return (bitCount + BIT_INDEX_MASK) >>> ADDRESS_BITS_PER_WORD;
    }

    /// Allocates enough raw words to store `bitCount` logical bits.
    ///
    /// If `initialValue` is true, the first `bitCount` bits are set and any trailing bits in the last word remain
    /// unset. Trailing bits are outside the logical bitmap, but leaving them unset makes append-null operations more
    /// likely to reuse the existing bitmap without clearing an extra bit. If `initialValue` is false, all bits in the
    /// returned array are unset.
    public static long[] allocateWords(int bitCount, boolean initialValue)
    {
        long[] rawWords = new long[wordsForBits(bitCount)];
        if (initialValue) {
            setBits(rawWords, 0, 0, bitCount);
        }
        return rawWords;
    }

    /// Returns whether the bit at `rawBitOffset + position` is set. See [Bitmap] for the raw encoding.
    public static boolean isSet(long[] rawWords, int rawBitOffset, int position)
    {
        int bitIndex = rawBitOffset + position;
        return (rawWords[bitIndex >>> ADDRESS_BITS_PER_WORD] & (1L << (bitIndex & BIT_INDEX_MASK))) != 0;
    }

    /// Sets the bit at `rawBitOffset + position`. See [Bitmap] for the raw encoding.
    public static void set(long[] rawWords, int rawBitOffset, int position)
    {
        int bitIndex = rawBitOffset + position;
        rawWords[bitIndex >>> ADDRESS_BITS_PER_WORD] |= 1L << (bitIndex & BIT_INDEX_MASK);
    }

    /// Clears the bit at `rawBitOffset + position`. See [Bitmap] for the raw encoding.
    public static void clear(long[] rawWords, int rawBitOffset, int position)
    {
        int bitIndex = rawBitOffset + position;
        rawWords[bitIndex >>> ADDRESS_BITS_PER_WORD] &= ~(1L << (bitIndex & BIT_INDEX_MASK));
    }

    /// Clears every bit in the logical range beginning at `rawBitOffset + position`.
    public static void clearBits(long[] rawWords, int rawBitOffset, int position, int bitCount)
    {
        if (bitCount == 0) {
            return;
        }

        int bitIndex = rawBitOffset + position;
        int wordIndex = bitIndex >>> ADDRESS_BITS_PER_WORD;
        int shift = bitIndex & BIT_INDEX_MASK;

        if (shift == 0) {
            int fullWords = bitCount >>> ADDRESS_BITS_PER_WORD;
            Arrays.fill(rawWords, wordIndex, wordIndex + fullWords, 0);
            int remainingBits = bitCount & BIT_INDEX_MASK;
            if (remainingBits != 0) {
                rawWords[wordIndex + fullWords] &= ~lowBitsMask(remainingBits);
            }
            return;
        }

        int headBits = Math.min(bitCount, Long.SIZE - shift);
        rawWords[wordIndex] &= ~(lowBitsMask(headBits) << shift);
        bitCount -= headBits;
        wordIndex++;

        int fullWords = bitCount >>> ADDRESS_BITS_PER_WORD;
        Arrays.fill(rawWords, wordIndex, wordIndex + fullWords, 0);
        wordIndex += fullWords;

        int remainingBits = bitCount & BIT_INDEX_MASK;
        if (remainingBits != 0) {
            rawWords[wordIndex] &= ~lowBitsMask(remainingBits);
        }
    }

    /// Sets every bit in the logical range beginning at `rawBitOffset + position`.
    public static void setBits(long[] rawWords, int rawBitOffset, int position, int bitCount)
    {
        if (bitCount == 0) {
            return;
        }

        int bitIndex = rawBitOffset + position;
        int wordIndex = bitIndex >>> ADDRESS_BITS_PER_WORD;
        int shift = bitIndex & BIT_INDEX_MASK;

        if (shift == 0) {
            int fullWords = bitCount >>> ADDRESS_BITS_PER_WORD;
            Arrays.fill(rawWords, wordIndex, wordIndex + fullWords, -1L);
            int remainingBits = bitCount & BIT_INDEX_MASK;
            if (remainingBits != 0) {
                rawWords[wordIndex + fullWords] |= lowBitsMask(remainingBits);
            }
            return;
        }

        int headBits = Math.min(bitCount, Long.SIZE - shift);
        rawWords[wordIndex] |= lowBitsMask(headBits) << shift;
        bitCount -= headBits;
        wordIndex++;

        int fullWords = bitCount >>> ADDRESS_BITS_PER_WORD;
        Arrays.fill(rawWords, wordIndex, wordIndex + fullWords, -1L);
        wordIndex += fullWords;

        int remainingBits = bitCount & BIT_INDEX_MASK;
        if (remainingBits != 0) {
            rawWords[wordIndex] |= lowBitsMask(remainingBits);
        }
    }

    /// ORs the low `bitCount` packed bits from `bits` into the logical range beginning at `rawBitOffset + position`.
    ///
    /// The least-significant bit of `bits` corresponds to `position`. Existing set bits in the destination remain set.
    public static void orPackedBits(long[] rawWords, int rawBitOffset, int position, long bits, int bitCount)
    {
        if (bitCount == 0) {
            return;
        }
        bits &= lowBitsMask(bitCount);

        int bitIndex = rawBitOffset + position;
        int wordIndex = bitIndex >>> ADDRESS_BITS_PER_WORD;
        int shift = bitIndex & BIT_INDEX_MASK;
        rawWords[wordIndex] |= bits << shift;
        if (shift != 0 && shift + bitCount > Long.SIZE) {
            rawWords[wordIndex + 1] |= bits >>> (Long.SIZE - shift);
        }
    }

    /// Copies a logical bit range from one bitmap to another, replacing the destination bits.
    public static void copyBits(long[] sourceRawWords, int sourceRawBitOffset, long[] destinationRawWords, int destinationRawBitOffset, int bitCount)
    {
        for (int position = 0; position < bitCount; position += Long.SIZE) {
            int remaining = Math.min(Long.SIZE, bitCount - position);
            long bits = getBits(sourceRawWords, sourceRawBitOffset, position, remaining);
            clearBits(destinationRawWords, destinationRawBitOffset, position, remaining);
            orPackedBits(destinationRawWords, destinationRawBitOffset, position, bits, remaining);
        }
    }

    /// Copies selected source positions into a contiguous destination range, replacing the destination bits.
    public static void copyBits(long[] sourceRawWords, int sourceRawBitOffset, int[] sourcePositions, int sourcePositionsOffset, long[] destinationRawWords, int destinationRawBitOffset, int bitCount)
    {
        clearBits(destinationRawWords, destinationRawBitOffset, 0, bitCount);
        for (int i = 0; i < bitCount; i++) {
            if (isSet(sourceRawWords, sourceRawBitOffset, sourcePositions[sourcePositionsOffset + i])) {
                set(destinationRawWords, destinationRawBitOffset, i);
            }
        }
    }

    /// Returns whether any bit in the logical range is unset. A null bitmap is treated as fully set.
    public static boolean hasUnsetBit(@Nullable long[] rawWords, int rawBitOffset, int bitCount)
    {
        if (rawWords == null) {
            return false;
        }
        checkBitRange(rawWords, rawBitOffset, bitCount);
        for (int position = 0; position < bitCount; position += Long.SIZE) {
            int remaining = Math.min(Long.SIZE, bitCount - position);
            long word = getAlignedWord(rawWords, rawBitOffset, position) & lowBitsMask(remaining);
            if (word != lowBitsMask(remaining)) {
                return true;
            }
        }
        return false;
    }

    /// Returns whether any bit in the logical range is set. A null bitmap is treated as fully set.
    public static boolean hasSetBit(@Nullable long[] rawWords, int rawBitOffset, int bitCount)
    {
        if (rawWords == null) {
            return bitCount > 0;
        }
        checkBitRange(rawWords, rawBitOffset, bitCount);
        for (int position = 0; position < bitCount; position += Long.SIZE) {
            int remaining = Math.min(Long.SIZE, bitCount - position);
            long word = getAlignedWord(rawWords, rawBitOffset, position) & lowBitsMask(remaining);
            if (word != 0) {
                return true;
            }
        }
        return false;
    }

    /// Returns whether any selected source position is unset.
    public static boolean hasUnsetBit(long[] rawWords, int rawBitOffset, int[] positions, int positionsOffset, int positionCount)
    {
        for (int i = 0; i < positionCount; i++) {
            if (!isSet(rawWords, rawBitOffset, positions[positionsOffset + i])) {
                return true;
            }
        }
        return false;
    }

    /// Returns whether any selected source position is set.
    public static boolean hasSetBit(long[] rawWords, int rawBitOffset, int[] positions, int positionsOffset, int positionCount)
    {
        for (int i = 0; i < positionCount; i++) {
            if (isSet(rawWords, rawBitOffset, positions[positionsOffset + i])) {
                return true;
            }
        }
        return false;
    }

    /// Returns up to 64 bits beginning at `rawBitOffset + position`, with the first bit in the low bit of the result.
    public static long getBits(long[] rawWords, int rawBitOffset, int position, int bitCount)
    {
        return getAlignedWord(rawWords, rawBitOffset, position) & lowBitsMask(bitCount);
    }

    /// Returns the number of adjacent bit transitions in the low `bitCount` bits of `bits`.
    ///
    /// A transition is counted when bit `i` and bit `i + 1` differ. Bits at positions greater than or equal to
    /// `bitCount` are ignored.
    public static int countTransitions(long bits, int bitCount)
    {
        if (bitCount < 0 || bitCount > Long.SIZE) {
            throw new IllegalArgumentException("bitCount must be between 0 and 64: " + bitCount);
        }
        if (bitCount <= 1) {
            return 0;
        }
        return Long.bitCount((bits ^ (bits >>> 1)) & lowBitsMask(bitCount - 1));
    }

    /// Returns a word where bit 0 corresponds to logical `position`, bit 1 to `position + 1`, and so on.
    static long getAlignedWord(long[] rawWords, int rawBitOffset, int position)
    {
        int bitIndex = rawBitOffset + position;
        int wordIndex = bitIndex >>> ADDRESS_BITS_PER_WORD;
        int shift = bitIndex & BIT_INDEX_MASK;

        long low = rawWords[wordIndex] >>> shift;
        if (shift == 0) {
            return low;
        }

        long high = 0;
        if (wordIndex + 1 < rawWords.length) {
            high = rawWords[wordIndex + 1] << (Long.SIZE - shift);
        }
        return low | high;
    }

    /// Returns a mask with the low `bits` bits set.
    static long lowBitsMask(int bits)
    {
        if (bits < 0 || bits > Long.SIZE) {
            throw new IllegalArgumentException("bits must be between 0 and 64: " + bits);
        }
        if (bits == Long.SIZE) {
            return -1L;
        }
        return (1L << bits) - 1;
    }

    /// Verifies that the logical range fits within the raw bitmap storage.
    static void checkBitRange(@Nullable long[] rawWords, int rawBitOffset, int bitCount)
    {
        if (rawWords == null) {
            return;
        }
        if (rawBitOffset < 0 || bitCount < 0) {
            throw new IndexOutOfBoundsException(format("Invalid rawBitOffset %s and bitCount %s", rawBitOffset, bitCount));
        }
        long rawBitCount = (long) rawWords.length * Long.SIZE;
        if ((long) rawBitOffset + bitCount > rawBitCount) {
            throw new IllegalArgumentException("bitmap length is less than bitCount");
        }
    }

    /// Returns a compact offset-zero bitmap for the logical range, or null if the range is fully set.
    ///
    /// If the input is already offset-zero with the exact word count, the original array is returned. Bits outside
    /// `bitCount` are not part of the bitmap and do not need to be cleared.
    static long[] compactBitmap(@Nullable long[] rawWords, int rawBitOffset, int bitCount)
    {
        if (rawWords == null) {
            return null;
        }
        checkBitRange(rawWords, rawBitOffset, bitCount);
        if (!hasUnsetBit(rawWords, rawBitOffset, bitCount)) {
            return null;
        }

        int wordCount = wordsForBits(bitCount);
        if (rawBitOffset == 0 && rawWords.length == wordCount) {
            return rawWords;
        }

        long[] compacted = new long[wordCount];
        for (int wordIndex = 0; wordIndex < wordCount; wordIndex++) {
            int position = wordIndex << ADDRESS_BITS_PER_WORD;
            int remaining = Math.min(Long.SIZE, bitCount - position);
            compacted[wordIndex] = getAlignedWord(rawWords, rawBitOffset, position) & lowBitsMask(remaining);
        }
        return compacted;
    }

    /// Copies the logical range and appends one unset bit, preserving `rawBitOffset` in the returned bitmap.
    static long[] copyBitmapAndAppendUnset(@Nullable long[] rawWords, int rawBitOffset, int bitCount)
    {
        checkBitRange(rawWords, rawBitOffset, bitCount);
        int desiredBitCount = rawBitOffset + bitCount + 1;
        long[] newBitmap = new long[wordsForBits(desiredBitCount)];
        if (rawWords == null) {
            setBits(newBitmap, 0, 0, rawBitOffset + bitCount);
        }
        else {
            for (int position = 0; position < bitCount; position += Long.SIZE) {
                int remaining = Math.min(Long.SIZE, bitCount - position);
                long word = getAlignedWord(rawWords, rawBitOffset, position) & lowBitsMask(remaining);
                orPackedBits(newBitmap, rawBitOffset, position, word, remaining);
            }
        }
        return newBitmap;
    }

    /// Ensures that the raw bitmap storage can hold `bitCapacity` bits, preserving existing words.
    static long[] ensureCapacity(@Nullable long[] rawWords, int bitCapacity)
    {
        int requiredWords = wordsForBits(bitCapacity);
        if (rawWords == null) {
            return new long[requiredWords];
        }
        if (rawWords.length < requiredWords) {
            return Arrays.copyOf(rawWords, requiredWords);
        }
        return rawWords;
    }
}
