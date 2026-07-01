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

import org.junit.jupiter.api.Test;

import static io.trino.spi.block.Bitmap.allocateWords;
import static io.trino.spi.block.Bitmap.clear;
import static io.trino.spi.block.Bitmap.clearBits;
import static io.trino.spi.block.Bitmap.compactBitmap;
import static io.trino.spi.block.Bitmap.copyBits;
import static io.trino.spi.block.Bitmap.countTransitions;
import static io.trino.spi.block.Bitmap.getAlignedWord;
import static io.trino.spi.block.Bitmap.getBits;
import static io.trino.spi.block.Bitmap.hasSetBit;
import static io.trino.spi.block.Bitmap.hasUnsetBit;
import static io.trino.spi.block.Bitmap.isSet;
import static io.trino.spi.block.Bitmap.lowBitsMask;
import static io.trino.spi.block.Bitmap.orPackedBits;
import static io.trino.spi.block.Bitmap.set;
import static io.trino.spi.block.Bitmap.setBits;
import static io.trino.spi.block.Bitmap.wordsForBits;
import static org.assertj.core.api.Assertions.assertThat;

final class TestBitmap
{
    @Test
    void testNullBitmapMeansAllSet()
    {
        assertThat(hasUnsetBit(null, 123, 456)).isFalse();
        assertThat(compactBitmap(null, 123, 456)).isNull();
    }

    @Test
    void testSetClearAndReadBoundaryBits()
    {
        long[] validity = new long[2];

        set(validity, 0, 0);
        set(validity, 0, 1);
        set(validity, 0, 63);
        set(validity, 0, 64);
        set(validity, 0, 65);

        assertThat(isSet(validity, 0, 0)).isTrue();
        assertThat(isSet(validity, 0, 1)).isTrue();
        assertThat(isSet(validity, 0, 2)).isFalse();
        assertThat(isSet(validity, 0, 63)).isTrue();
        assertThat(isSet(validity, 0, 64)).isTrue();
        assertThat(isSet(validity, 0, 65)).isTrue();

        clear(validity, 0, 64);

        assertThat(isSet(validity, 0, 64)).isFalse();
        assertThat(isSet(validity, 0, 65)).isTrue();
    }

    @Test
    void testAllocateWords()
    {
        assertThat(allocateWords(0, true)).isEmpty();
        assertThat(allocateWords(65, false)).containsExactly(0L, 0L);
        assertThat(allocateWords(65, true)).containsExactly(-1L, 1L);
    }

    @Test
    void testSetBitsAcrossWordBoundary()
    {
        long[] validity = new long[3];

        setBits(validity, 0, 61, 70);

        assertThat(isSet(validity, 0, 60)).isFalse();
        assertThat(isSet(validity, 0, 61)).isTrue();
        assertThat(isSet(validity, 0, 63)).isTrue();
        assertThat(isSet(validity, 0, 64)).isTrue();
        assertThat(isSet(validity, 0, 127)).isTrue();
        assertThat(isSet(validity, 0, 130)).isTrue();
        assertThat(isSet(validity, 0, 131)).isFalse();
    }

    @Test
    void testClearBitsAcrossWordBoundary()
    {
        long[] validity = {-1L, -1L, -1L};

        clearBits(validity, 0, 61, 70);

        assertThat(isSet(validity, 0, 60)).isTrue();
        assertThat(isSet(validity, 0, 61)).isFalse();
        assertThat(isSet(validity, 0, 63)).isFalse();
        assertThat(isSet(validity, 0, 64)).isFalse();
        assertThat(isSet(validity, 0, 127)).isFalse();
        assertThat(isSet(validity, 0, 130)).isFalse();
        assertThat(isSet(validity, 0, 131)).isTrue();
    }

    @Test
    void testOrPackedBitsAcrossWordBoundary()
    {
        long[] validity = new long[2];

        orPackedBits(validity, 3, 60, (1L << 10) | 0b101101L, 6);

        assertThat(isSet(validity, 3, 60)).isTrue();
        assertThat(isSet(validity, 3, 61)).isFalse();
        assertThat(isSet(validity, 3, 62)).isTrue();
        assertThat(isSet(validity, 3, 63)).isTrue();
        assertThat(isSet(validity, 3, 64)).isFalse();
        assertThat(isSet(validity, 3, 65)).isTrue();
        assertThat(isSet(validity, 3, 66)).isFalse();
    }

    @Test
    void testCopyBitsAcrossWordBoundary()
    {
        long[] source = new long[3];
        setBits(source, 0, 61, 70);
        clear(source, 0, 63);
        clear(source, 0, 127);
        long[] destination = {-1L, -1L, -1L};

        copyBits(source, 61, destination, 7, 70);

        assertThat(isSet(destination, 0, 6)).isTrue();
        assertThat(isSet(destination, 0, 7)).isTrue();
        assertThat(isSet(destination, 0, 9)).isFalse();
        assertThat(isSet(destination, 0, 73)).isFalse();
        assertThat(isSet(destination, 0, 76)).isTrue();
        assertThat(isSet(destination, 0, 77)).isTrue();
    }

    @Test
    void testCopySelectedBits()
    {
        long[] source = new long[2];
        set(source, 3, 2);
        set(source, 3, 5);
        set(source, 3, 9);
        long[] destination = {-1L};

        copyBits(source, 3, new int[] {5, 6, 2, 9}, 0, destination, 4, 4);

        assertThat(isSet(destination, 0, 3)).isTrue();
        assertThat(isSet(destination, 0, 4)).isTrue();
        assertThat(isSet(destination, 0, 5)).isFalse();
        assertThat(isSet(destination, 0, 6)).isTrue();
        assertThat(isSet(destination, 0, 7)).isTrue();
        assertThat(isSet(destination, 0, 8)).isTrue();
    }

    @Test
    void testHasSetBit()
    {
        long[] validity = new long[2];
        set(validity, 0, 65);

        assertThat(hasSetBit(null, 0, 10)).isTrue();
        assertThat(hasSetBit(validity, 0, 65)).isFalse();
        assertThat(hasSetBit(validity, 0, 66)).isTrue();
        assertThat(hasSetBit(validity, 0, new int[] {1, 2, 65}, 0, 2)).isFalse();
        assertThat(hasSetBit(validity, 0, new int[] {1, 2, 65}, 0, 3)).isTrue();
        assertThat(hasUnsetBit(validity, 0, new int[] {65}, 0, 1)).isFalse();
        assertThat(hasUnsetBit(validity, 0, new int[] {64, 65}, 0, 2)).isTrue();
    }

    @Test
    void testAlignedWord()
    {
        long[] validity = {0xFEDC_BA98_7654_3210L, 0x0123_4567_89AB_CDEFL};

        assertThat(getAlignedWord(validity, 0, 0)).isEqualTo(0xFEDC_BA98_7654_3210L);
        assertThat(getAlignedWord(validity, 0, 64)).isEqualTo(0x0123_4567_89AB_CDEFL);
    }

    @Test
    void testUnalignedWord()
    {
        long low = 0xFEDC_BA98_7654_3210L;
        long high = 0x0123_4567_89AB_CDEFL;
        long[] validity = {low, high};

        assertThat(getAlignedWord(validity, 0, 4)).isEqualTo((low >>> 4) | (high << 60));
        assertThat(getAlignedWord(validity, 3, 61)).isEqualTo(high);
        assertThat(getAlignedWord(validity, 5, 60)).isEqualTo(high >>> 1);
    }

    @Test
    void testGetBits()
    {
        long[] validity = {0xFEDC_BA98_7654_3210L, 0x0123_4567_89AB_CDEFL};

        assertThat(getBits(validity, 0, 4, 12)).isEqualTo(0x321L);
        assertThat(getBits(validity, 3, 61, 12)).isEqualTo(0xDEFL);
        assertThat(getBits(validity, 0, 0, 64)).isEqualTo(0xFEDC_BA98_7654_3210L);
    }

    @Test
    void testCountTransitions()
    {
        assertThat(countTransitions(0, 0)).isZero();
        assertThat(countTransitions(0, 1)).isZero();
        assertThat(countTransitions(-1L, 64)).isZero();
        assertThat(countTransitions(0b0000_1111, 8)).isEqualTo(1);
        assertThat(countTransitions(0b0101_0101, 8)).isEqualTo(7);
        assertThat(countTransitions(0b10_0101_0101, 8)).isEqualTo(7);
    }

    @Test
    void testLowBitsMask()
    {
        assertThat(lowBitsMask(0)).isZero();
        assertThat(lowBitsMask(1)).isEqualTo(0b1L);
        assertThat(lowBitsMask(63)).isEqualTo(0x7FFF_FFFF_FFFF_FFFFL);
        assertThat(lowBitsMask(64)).isEqualTo(-1L);
    }

    @Test
    void testWordsForPositions()
    {
        assertThat(wordsForBits(0)).isZero();
        assertThat(wordsForBits(1)).isEqualTo(1);
        assertThat(wordsForBits(64)).isEqualTo(1);
        assertThat(wordsForBits(65)).isEqualTo(2);
    }

    @Test
    void testCompactBitmapKeepsCompactBitmapWithUndefinedTail()
    {
        long[] validity = {-1L, -1L};
        clear(validity, 0, 5);

        assertThat(compactBitmap(validity, 0, 65)).isSameAs(validity);
    }

    @Test
    void testCompactBitmapRealignsAndClearsTail()
    {
        long[] validity = {-1L, -1L};
        clear(validity, 0, 5);
        clear(validity, 0, 63);
        clear(validity, 0, 64);
        clear(validity, 0, 70);

        long[] compacted = compactBitmap(validity, 3, 65);

        assertThat(compacted).hasSize(2);
        assertThat(isSet(compacted, 0, 0)).isTrue();
        assertThat(isSet(compacted, 0, 2)).isFalse();
        assertThat(isSet(compacted, 0, 60)).isFalse();
        assertThat(isSet(compacted, 0, 61)).isFalse();
        assertThat(isSet(compacted, 0, 64)).isTrue();
        assertThat(compacted[1] & ~lowBitsMask(1)).isZero();
    }
}
