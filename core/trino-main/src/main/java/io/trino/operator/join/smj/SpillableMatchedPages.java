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
package io.trino.operator.join.smj;

import com.google.common.collect.Iterators;
import io.trino.spi.Page;
import io.trino.spiller.Spiller;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

public class SpillableMatchedPages
{
    private final int numRowsInMemoryBufferThreshold;
    private final Supplier<Spiller> spillerSupplier;
    private List<Page> buffer = new LinkedList<>();
    private Long positionCount = 0L;

    private Page cur;
    private int positionOffset;
    private int length;
    private Spiller spiller;
    private Row firstMatchRow;

    private Iterator<Row> iterator;

    private long memorySize;

    public SpillableMatchedPages(int numRowsInMemoryBufferThreshold, Supplier<Spiller> spillerSupplier)
    {
        this.numRowsInMemoryBufferThreshold = numRowsInMemoryBufferThreshold;
        this.spillerSupplier = spillerSupplier;
        this.iterator = null;
        this.memorySize = 0;
    }

    public void insertRow(Row row)
    {
        if (firstMatchRow == null) {
            firstMatchRow = row.clone();
        }

        if (this.cur == null) {
            this.cur = row.getPage();
            this.positionOffset = row.getPosition();
            this.length = 1;
        }
        else if (row.getPage() == this.cur && row.getPosition() == positionOffset + length) {
            this.length++;
        }
        else {
            addPage();
            insertRow(row);
            return;
        }
        positionCount++;
        if (positionCount >= numRowsInMemoryBufferThreshold && spiller == null) {
            spillToDisk();
        }
    }

    public void finishInsert()
    {
        if (cur != null) {
            addPage();
        }
        if (spiller != null) {
            spiller.endSpill();
        }
    }

    private void spillToDisk()
    {
        spiller = spillerSupplier.get();
        spiller.beginSpill();
        for (Page page : buffer) {
            spiller.spillOnePage(page);
        }
        buffer.clear();
        memorySize = 0;
    }

    private void addPage()
    {
        Page page = cur.getRegion(positionOffset, length);
        if (spiller == null) {
            buffer.add(cur.getRegion(positionOffset, length));
            memorySize += page.getSizeInBytes();
        }
        else {
            spiller.spillOnePage(page);
        }
        cur = null;
    }

    public Iterator<Row> getOrCreateIterator()
    {
        if (iterator == null) {
            Iterator<Page> pageIterator = null;
            if (spiller == null) {
                pageIterator = buffer.iterator();
            }
            else {
                List<Iterator<Page>> iteratorList = spiller.getSpills();
                pageIterator = iteratorList.get(0);
                for (int i = 1; i < iteratorList.size(); i++) {
                    pageIterator = Iterators.concat(pageIterator, iteratorList.get(i));
                }
            }

            final Iterator<Page> finalPageIterator = pageIterator;
            iterator = new Iterator<>()
            {
                private int pos;
                private Page page;
                private Row row;

                {
                    this.pos = 0;
                    this.page = finalPageIterator.next();
                    this.row = new Row();
                }

                @Override
                public boolean hasNext()
                {
                    return pos >= 0;
                }

                @Override
                public Row next()
                {
                    row.set(page, pos);
                    if (pos < page.getPositionCount() - 1) {
                        pos++;
                    }
                    else {
                        if (finalPageIterator.hasNext()) {
                            page = finalPageIterator.next();
                            pos = 0;
                        }
                        else {
                            pos = -1;
                        }
                    }
                    return row;
                }
            };
        }
        return iterator;
    }

    public void resetIterator()
    {
        iterator = null;
    }

    public void clear()
    {
        this.firstMatchRow = null;
        this.buffer.clear();
        if (this.spiller != null) {
            this.spiller.close();
            this.spiller = null;
        }
        this.cur = null;
        this.positionOffset = 0;
        this.length = 0;
        this.positionCount = 0L;
        this.memorySize = 0;
    }

    public boolean isEmpty()
    {
        return positionCount == 0;
    }

    public List<Page> getBuffer()
    {
        return buffer;
    }

    public Row getFirstMatchRow()
    {
        return firstMatchRow;
    }

    public long getMemorySize()
    {
        return memorySize;
    }
}
