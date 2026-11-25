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

import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;

/// Reads variable-width values from a block of either of the two classes that hold them.
///
/// A [VariableWidthBlockBuilder] builds [StridedVariableWidthBlock]s when [#STRIDED] is set. Blocks that are made
/// in other ways, for example by the readers of file formats, stay [VariableWidthBlock]s, so both classes then
/// reach the code that reads the values of a type.
public final class VariableWidthValues
{
    /// Whether builders of variable-width values build [StridedVariableWidthBlock]s. Experimental: set with the
    /// system property `trino.experimental.strided-variable-width-block` when the JVM starts. The value is a
    /// constant, so the code for the other class costs nothing when this is not set.
    public static final boolean STRIDED = Boolean.getBoolean("trino.experimental.strided-variable-width-block");

    private VariableWidthValues() {}

    public static Slice getSlice(ValueBlock block, int position)
    {
        if (STRIDED && block instanceof StridedVariableWidthBlock strided) {
            return strided.getSlice(position);
        }
        return ((VariableWidthBlock) block).getSlice(position);
    }

    public static int getSliceLength(ValueBlock block, int position)
    {
        if (STRIDED && block instanceof StridedVariableWidthBlock strided) {
            return strided.getSliceLength(position);
        }
        return ((VariableWidthBlock) block).getSliceLength(position);
    }

    /// Returns the slice that the values of the block are read from in place, at the offsets that [#getRawSliceOffset] returns.
    public static Slice getRawSlice(ValueBlock block)
    {
        if (STRIDED && block instanceof StridedVariableWidthBlock strided) {
            return strided.getRawSlice();
        }
        return ((VariableWidthBlock) block).getRawSlice();
    }

    public static int getRawSliceOffset(ValueBlock block, int position)
    {
        if (STRIDED && block instanceof StridedVariableWidthBlock strided) {
            return strided.getRawSliceOffset(position);
        }
        return ((VariableWidthBlock) block).getRawSliceOffset(position);
    }

    public static boolean valueEquals(ValueBlock leftBlock, int leftPosition, ValueBlock rightBlock, int rightPosition)
    {
        if (STRIDED && leftBlock instanceof StridedVariableWidthBlock left && rightBlock instanceof StridedVariableWidthBlock right) {
            return left.valueEquals(leftPosition, right, rightPosition);
        }
        return getRawSlice(leftBlock).equals(
                getRawSliceOffset(leftBlock, leftPosition),
                getSliceLength(leftBlock, leftPosition),
                getRawSlice(rightBlock),
                getRawSliceOffset(rightBlock, rightPosition),
                getSliceLength(rightBlock, rightPosition));
    }

    public static boolean valueEquals(ValueBlock block, int position, byte[] otherBytes, int otherOffset, int otherLength)
    {
        if (STRIDED && block instanceof StridedVariableWidthBlock strided) {
            return strided.valueEquals(position, otherBytes, otherOffset, otherLength);
        }
        return getRawSlice(block).equals(
                getRawSliceOffset(block, position),
                getSliceLength(block, position),
                otherBytes,
                otherOffset,
                otherLength);
    }

    public static long valueHash(ValueBlock block, int position)
    {
        if (STRIDED && block instanceof StridedVariableWidthBlock strided) {
            return strided.valueHash(position);
        }
        return XxHash64.hash(getRawSlice(block), getRawSliceOffset(block, position), getSliceLength(block, position));
    }

    public static void copyValue(ValueBlock block, int position, byte[] target, int targetOffset, int length)
    {
        if (STRIDED && block instanceof StridedVariableWidthBlock strided) {
            strided.copyValue(position, target, targetOffset);
            return;
        }
        getRawSlice(block).getBytes(getRawSliceOffset(block, position), target, targetOffset, length);
    }
}
