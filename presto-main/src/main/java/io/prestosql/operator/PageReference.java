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
package io.prestosql.operator;

import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import org.openjdk.jol.info.ClassLayout;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class PageReference
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(PageReference.class).instanceSize();

    private Page page;
    private Row[] reference;

    private int usedPositionCount;

    public PageReference(Page page)
    {
        this.page = requireNonNull(page, "page is null");
        this.reference = new Row[page.getPositionCount()];
    }

    public void reference(Row row)
    {
        int position = row.getPosition();
        reference[position] = row;
        usedPositionCount++;
    }

    public void dereference(int position)
    {
        checkArgument(reference[position] != null && usedPositionCount > 0);
        reference[position] = null;
        usedPositionCount--;
    }

    public int getUsedPositionCount()
    {
        return usedPositionCount;
    }

    public void compact()
    {
        checkState(usedPositionCount > 0);

        if (usedPositionCount == page.getPositionCount()) {
            return;
        }

        // re-assign reference
        Row[] newReference = new Row[usedPositionCount];
        int[] positions = new int[usedPositionCount];
        int index = 0;
        for (int i = 0; i < page.getPositionCount(); i++) {
            if (reference[i] != null) {
                newReference[index] = reference[i];
                positions[index] = i;
                index++;
            }
        }
        verify(index == usedPositionCount);

        // compact page
        Block[] blocks = new Block[page.getChannelCount()];
        for (int i = 0; i < page.getChannelCount(); i++) {
            Block block = page.getBlock(i);
            blocks[i] = block.copyPositions(positions, 0, usedPositionCount);
        }

        // update all the elements in the heaps that reference the current page
        for (int i = 0; i < usedPositionCount; i++) {
            // this does not change the elements in the heap;
            // it only updates the value of the elements; while keeping the same order
            newReference[i].reset(i);
        }
        page = new Page(usedPositionCount, blocks);
        reference = newReference;
    }

    public Page getPage()
    {
        return page;
    }

    public long getEstimatedSizeInBytes()
    {
        return page.getRetainedSizeInBytes() + sizeOf(reference) + INSTANCE_SIZE;
    }
}
