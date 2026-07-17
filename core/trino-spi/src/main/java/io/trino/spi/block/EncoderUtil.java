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

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import jakarta.annotation.Nullable;

import static io.trino.spi.block.Bitmap.getAlignedWord;
import static io.trino.spi.block.Bitmap.lowBitsMask;
import static io.trino.spi.block.Bitmap.wordsForBits;

final class EncoderUtil
{
    private EncoderUtil() {}

    public static void encodeValidityAsLongs(SliceOutput sliceOutput, @Nullable long[] validity, int offset, int positionCount)
    {
        sliceOutput.writeBoolean(validity != null);
        if (validity == null) {
            return;
        }

        int wordCount = wordsForBits(positionCount);
        for (int wordIndex = 0; wordIndex < wordCount; wordIndex++) {
            int position = wordIndex << 6;
            long word = getAlignedWord(validity, offset, position);
            int remaining = positionCount - position;
            if (remaining < Long.SIZE) {
                word &= lowBitsMask(remaining);
            }
            sliceOutput.writeLong(word);
        }
    }

    @Nullable
    public static long[] decodeValidityAsLongs(SliceInput sliceInput, int positionCount)
    {
        if (!sliceInput.readBoolean()) {
            return null;
        }

        long[] validity = new long[wordsForBits(positionCount)];
        sliceInput.readLongs(validity);
        return validity;
    }
}
