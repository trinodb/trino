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
package io.trino.plugin.varada.juffer;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;

import static com.google.common.base.Preconditions.checkArgument;

public class BlockPosHolder
{
    private final Block block;
    private final Type type;
    private final int startPos; // inclusive
    private final int endPos; // exclusive

    private int pos;

    public BlockPosHolder(Block block, Type type, int startPos, int maxRecordsToAdd)
    {
        checkArgument(startPos >= 0, "startPos must be greater or equal to 0");
        checkArgument(maxRecordsToAdd > 0, "maxRecordsToAdd must be greater than 0");

        this.block = block;
        this.type = type;
        this.startPos = startPos;
        this.endPos = startPos + maxRecordsToAdd;
        this.pos = startPos;
    }

    public void advance()
    {
        pos++;
    }

    public void seek(int recordNumber)
    {
        pos = startPos + recordNumber;
    }

    public boolean inRange()
    {
        return pos < endPos;
    }

    public int getPos()
    {
        return pos - startPos;
    }

    /*
    total row to write from this block
     */
    public int getNumEntries()
    {
        return endPos - startPos;
    }

    public boolean isNull()
    {
        return block.isNull(pos);
    }

    public long getLong()
    {
        return type.getLong(block, pos);
    }

    public int getInt()
    {
        return (int) type.getLong(block, pos);
    }

    public short getShort()
    {
        return (short) type.getLong(block, pos);
    }

    public byte getByte()
    {
        return (byte) type.getLong(block, pos);
    }

    public byte getBoolean()
    {
        return type.getBoolean(block, pos) ? (byte) 1 : (byte) 0;
    }

    public double getDouble()
    {
        return type.getDouble(block, pos);
    }

    public Slice getSlice()
    {
        return type.getSlice(block, pos);
    }

    public Object getObject()
    {
        return type.getObject(block, pos);
    }

    public boolean mayHaveNull()
    {
        return block.mayHaveNull();
    }

    public Type getType()
    {
        return type;
    }
}
