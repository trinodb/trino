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
package io.trino.type.collation;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.spi.type.CollationComparison;

import static io.trino.spi.type.Chars.compareChars;
import static io.trino.spi.type.Chars.trimTrailingSpaces;
import static io.trino.type.collation.PadSpaceKeys.appendLevel;

/// The comparison of `ucs_basic PAD SPACE`: code point order, with the shorter string compared as
/// though it were extended with spaces to the length of the longer. For valid UTF-8 this is the
/// order of the bytes with the shorter extended with `0x20` bytes, the order of `char` values, so
/// `'a'` followed by a tab sorts before `'a'`. Strings are equal exactly when they are equal
/// without their trailing spaces.
enum BinaryPadSpaceComparison
        implements CollationComparison
{
    BINARY_PAD_SPACE;

    private static final int SPACE = ' ';

    @Override
    public int compare(Slice left, Slice right)
    {
        return compareChars(left, right);
    }

    @Override
    public boolean equal(Slice left, Slice right)
    {
        return trimTrailingSpaces(left).equals(trimTrailingSpaces(right));
    }

    @Override
    public Slice collationKey(Slice value)
    {
        int[] bytes = new int[value.length()];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = value.getUnsignedByte(i);
        }
        DynamicSliceOutput output = new DynamicSliceOutput(2 * bytes.length + 2);
        appendLevel(output, bytes, bytes.length, SPACE, 1);
        return output.slice();
    }
}
