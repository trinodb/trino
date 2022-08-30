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

import io.trino.spi.Page;
import io.trino.spi.type.UuidType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.format;

public final class PageDescriptionUtils
{
    private static final int MAX_DISPLAYED_ELEMENTS = 8;
    private static final int MAX_DISPLAYED_STRING_SIZE = 20;

    private PageDescriptionUtils() {}

    public static String describePage(Page page)
    {
        if (page == null) {
            return "null";
        }
        StringBuilder builder = new StringBuilder();
        builder.append("Page[positions=").append(page.getPositionCount()).append(" ");
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            if (channel > 0) {
                builder.append(", ");
            }
            builder.append(channel).append(":");
            addBlockDescription(builder, page.getBlock(channel));
        }
        builder.append("]");
        return builder.toString();
    }

    public static String describeBlocks(Block[] blocks)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("Blocks[");
        for (int channel = 0; channel < blocks.length; channel++) {
            if (channel > 0) {
                builder.append(", ");
            }
            builder.append(channel).append(":");
            addBlockDescription(builder, blocks[channel]);
        }
        builder.append("]");
        return builder.toString();
    }

    public static String describePositions(int[] positions)
    {
        int positionCount = positions.length;
        int limit = Math.min(MAX_DISPLAYED_ELEMENTS, positionCount);
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        for (int position = 0; position < limit; position++) {
            if (position > 0) {
                builder.append(",");
            }
            builder.append(positions[position]);
        }
        builder.append("]");
        return builder.toString();
    }

    public static String describeBlock(Block block)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("Block[");
        addBlockDescription(builder, block);
        builder.append("]");
        return builder.toString();
    }

    public static String describeBuckets(Block block, int maxBucket)
    {
        Block bucketBlock = ColumnarRow.toColumnarRow(block).getField(0);
        Set<Integer> buckets = new HashSet<>();
        for (int position = 0; position < bucketBlock.getPositionCount(); position++) {
            buckets.add(bucketBlock.getInt(position, 0));
        }
        List<Integer> sortedBuckets = buckets.stream().sorted().collect(Collectors.toList());
        List<Integer> missingBuckets = new ArrayList<>();
        for (int bucket = 0; bucket < maxBucket; bucket++) {
            if (!buckets.contains(bucket)) {
                missingBuckets.add(bucket);
            }
        }
        return format("Buckets: %s, missingBuckets %s", sortedBuckets, missingBuckets);
    }

    private interface BlockElementFetcher
    {
        String fetchElement(int position);
    }

    private static void addBlockDescription(StringBuilder builder, Block block)
    {
        try {
            if (block == null) {
                builder.append("null");
            }
            else if (block instanceof ByteArrayBlock) {
                addElements(builder, "Byte", block, position -> String.valueOf(block.getByte(position, 0)));
            }
            else if (block instanceof IntArrayBlock) {
                addElements(builder, "Int", block, position -> String.valueOf(block.getInt(position, 0)));
            }
            else if (block instanceof LongArrayBlock) {
                addElements(builder, "Long", block, position -> String.valueOf(block.getLong(position, 0)));
            }
            else if (block instanceof Int128ArrayBlock) {
                addElements(builder, "UUID", block, position -> UuidType.UUID.getObjectValue(null, block, position).toString());
            }
            else if (block instanceof VariableWidthBlock) {
                VariableWidthBlock varBlock = (VariableWidthBlock) block;
                addElements(builder, "VarWidth", block, position ->
                        format("\"%s\"", limitStringSize(varBlock.getSlice(position, 0, varBlock.getSliceLength(position)).toStringUtf8())));
            }
            else if (block instanceof LazyBlock) {
                addBlockDescription(builder, ((LazyBlock) block).getBlock());
            }
            else if (block instanceof RunLengthEncodedBlock) {
                RunLengthEncodedBlock rleBlock = (RunLengthEncodedBlock) block;
                builder.append("RLE[").append(block.getPositionCount()).append("@");
                addBlockDescription(builder, rleBlock.getValue());
                builder.append("]");
            }
            else if (block instanceof RowBlock) {
                builder.append("Row[");
                int limit = Math.min(MAX_DISPLAYED_ELEMENTS, block.getPositionCount());
                maybeAddIsNullDescription(builder, block, block.getPositionCount(), limit);
                int childIndex = 0;
                for (Block child : block.getChildren()) {
                    if (childIndex > 0) {
                        builder.append(", ");
                    }
                    builder.append(childIndex++).append(":");
                    addBlockDescription(builder, child);
                }
                builder.append("]");
            }
            else if (block instanceof DictionaryBlock) {
                DictionaryBlock dictionaryBlock = (DictionaryBlock) block;
                int positionCount = dictionaryBlock.getPositionCount();
                builder.append("Dict[ids=[");
                int limit = Math.min(positionCount, 10);
                for (int position = 0; position < limit; position++) {
                    if (position > 0) {
                        builder.append(", ");
                    }
                    builder.append(dictionaryBlock.getId(position));
                }
                if (limit != positionCount) {
                    builder.append("...");
                }
                builder.append("], ");

                Block dictionary = dictionaryBlock.getDictionary();
                addBlockDescription(builder, dictionary);
                builder.append("]");
            }
            else {
                builder.append("SomeOtherBlock[").append(block.getClass().getSimpleName()).append("]");
            }
        }
        catch (Exception e) {
            builder.append("Exception ").append(e.getClass()).append(" with message ").append(e.getMessage());
        }
    }

    private static String limitStringSize(String s)
    {
        if (s.length() >= MAX_DISPLAYED_STRING_SIZE) {
            return s.substring(0, MAX_DISPLAYED_STRING_SIZE) + "...";
        }
        return s;
    }

    private static void addElements(StringBuilder builder, String description, Block block, BlockElementFetcher elementFetcher)
    {
        builder.append(description).append("[");

        int positionCount = block.getPositionCount();
        int limit = Math.min(MAX_DISPLAYED_ELEMENTS, positionCount);
        maybeAddIsNullDescription(builder, block, positionCount, limit);

        for (int position = 0; position < limit; position++) {
            if (position > 0) {
                builder.append(", ");
            }
            builder.append(elementFetcher.fetchElement(position));
        }
        if (limit < positionCount) {
            builder.append("...");
        }
        builder.append("]");
    }

    private static void maybeAddIsNullDescription(StringBuilder builder, Block block, int positionCount, int limit)
    {
        if (hasNulls(block)) {
            builder.append("isNull[");
            for (int position = 0; position < limit; position++) {
                if (position > 0) {
                    builder.append(",");
                }
                builder.append(block.isNull(position) ? "t" : "f");
            }
            if (limit < positionCount) {
                builder.append("...");
            }
            builder.append("], ");
        }
    }

    private static boolean hasNulls(Block block)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                return true;
            }
        }
        return false;
    }
}
