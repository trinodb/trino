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
package io.trino.operator.window.matcher;

import com.google.common.annotations.VisibleForTesting;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ArrayView
{
    public static final ArrayView EMPTY = new ArrayView(new int[] {}, 0);

    private final int[] array;
    private final int length;

    public ArrayView(int[] array, int length)
    {
        this.array = requireNonNull(array, "array is null");
        checkArgument(length >= 0, "used slots count is negative");
        checkArgument(length <= array.length, "used slots count exceeds array size");
        this.length = length;
    }

    public int get(int index)
    {
        checkArgument(index >= 0 && index < length, "array index out of bounds");
        return array[index];
    }

    public int length()
    {
        return length;
    }

    @VisibleForTesting
    int[] toArray()
    {
        return Arrays.copyOf(array, length);
    }
}
