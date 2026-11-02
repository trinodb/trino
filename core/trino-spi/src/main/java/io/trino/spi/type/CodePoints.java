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

import io.airlift.slice.InvalidUtf8Exception;
import io.airlift.slice.Slice;

import java.util.Optional;

import static io.airlift.slice.SliceUtf8.toCodePoints;
import static java.lang.Character.MAX_CODE_POINT;
import static java.lang.Character.MAX_SURROGATE;
import static java.lang.Character.MIN_CODE_POINT;
import static java.lang.Character.MIN_SURROGATE;

/**
 * The code points of a UTF-8 encoded value, and navigation over the code points such a value can be
 * built from. The surrogate range is skipped, because it has no UTF-8 encoding.
 */
final class CodePoints
{
    private CodePoints() {}

    /**
     * @return the code points of the value, or empty when the value is not valid UTF-8
     */
    static Optional<int[]> tryCodePoints(Slice utf8)
    {
        try {
            return Optional.of(toCodePoints(utf8));
        }
        catch (InvalidUtf8Exception _) {
            return Optional.empty();
        }
    }

    static int previousCodePoint(int codePoint)
    {
        if (codePoint <= MIN_CODE_POINT) {
            throw new IllegalArgumentException("No code point precedes " + codePoint);
        }
        if (codePoint - 1 == MAX_SURROGATE) {
            return MIN_SURROGATE - 1;
        }
        return codePoint - 1;
    }

    static int nextCodePoint(int codePoint)
    {
        if (codePoint >= MAX_CODE_POINT) {
            throw new IllegalArgumentException("No code point follows " + codePoint);
        }
        if (codePoint + 1 == MIN_SURROGATE) {
            return MAX_SURROGATE + 1;
        }
        return codePoint + 1;
    }
}
