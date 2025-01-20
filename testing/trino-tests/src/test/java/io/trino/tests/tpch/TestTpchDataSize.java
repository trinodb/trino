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
package io.trino.tests.tpch;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.plugin.tpch.DecimalTypeMapping;
import io.trino.plugin.tpch.TpchTables;
import io.trino.spi.Page;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.vstream.IntStreamVByte;
import io.trino.spi.block.vstream.LongStreamVByte;

import java.util.List;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;

public class TestTpchDataSize
{
    private TestTpchDataSize() {}

    public static void main(String[] args)
    {
        List<Page> pages = ImmutableList.copyOf(TpchTables.getTablePages("lineitem", 3, DecimalTypeMapping.DECIMAL));

        int totalSize = 0;
        int encodedTotalSize = 0;

        for (Page page : pages) {
            for (int i = 0; i < page.getChannelCount(); i++) {
                if (page.getBlock(i) instanceof LongArrayBlock longArrayBlock) {
                    totalSize += longArrayBlock.getRawValues().length * SIZE_OF_INT;
                    //System.out.println("block[" + i  + "] size is "  + longArrayBlock.getRawValues().length * SIZE_OF_INT);
                    Slice buffer = Slices.allocate(LongStreamVByte.maxEncodedSize(longArrayBlock.getRawValues().length));

                    //System.out.println("Buffer size is " + buffer.length() + " for positions " + longArrayBlock.getRawValues().length);

                    int size = LongStreamVByte.writeLongs(buffer.getOutput(), longArrayBlock.getRawValues());
                    //System.out.println("block[" + i  + "] encoded size is " + size + " " + (size * 100.0f / (longArrayBlock.getRawValues().length * SIZE_OF_INT)) + "%");
                    encodedTotalSize += size;
                }

                if (page.getBlock(i) instanceof IntArrayBlock intArrayBlock) {
                    totalSize += intArrayBlock.getRawValues().length * SIZE_OF_INT;
                    //System.out.println("block[" + i  + "] size is "  + intArrayBlock.getRawValues().length * SIZE_OF_INT);
                    Slice buffer = Slices.allocate(LongStreamVByte.maxEncodedSize(intArrayBlock.getRawValues().length));
                    int size = IntStreamVByte.writeInts(buffer.getOutput(), intArrayBlock.getRawValues());
                    //System.out.println("block[" + i  + "] encoded size is " + size + " " + (size * 100.0f / (intArrayBlock.getRawValues().length * SIZE_OF_INT)) + "%");
                    encodedTotalSize += size;
                }
            }
        }

        System.out.println("Total size is " + DataSize.succinctBytes(totalSize));
        System.out.println("Total encoded size is " + DataSize.succinctBytes(encodedTotalSize));
        System.out.println("Total encoded size is " + (encodedTotalSize * 100.0f / totalSize) + "%");
    }
}
