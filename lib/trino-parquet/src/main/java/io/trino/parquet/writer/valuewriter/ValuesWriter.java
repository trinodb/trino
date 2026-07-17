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
package io.trino.parquet.writer.valuewriter;

import io.airlift.slice.Slice;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.io.api.Binary;

/**
 * Based on org.apache.parquet.column.values.ValuesWriter, adapted so binary values
 * are written as a Slice, avoiding a per-value Binary on the write path.
 */
public abstract class ValuesWriter
        implements AutoCloseable
{
    /**
     * @return the size of the currently buffered data in bytes, used to decide when to move to the next page
     */
    public abstract long getBufferedSize();

    /**
     * @return the bytes buffered so far to write to the current page
     */
    public abstract BytesInput getBytes();

    /**
     * @return the encoding used to encode the bytes, called after getBytes() and before reset()
     */
    public abstract Encoding getEncoding();

    /**
     * resets the current buffer and starts writing the next page, called after getBytes()
     */
    public abstract void reset();

    /**
     * closes the writer, releasing any output stream and resources
     */
    @Override
    public void close() {}

    /**
     * @return the dictionary page or null if not dictionary based; closes the dictionary
     */
    public DictionaryPage toDictPageAndClose()
    {
        return null;
    }

    /**
     * resets the dictionary when a new block starts
     */
    public void resetDictionary() {}

    /**
     * @return the allocated size of the buffer, at least as large as getBufferedSize()
     */
    public abstract long getAllocatedSize();

    public void writeByte(int value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    public void writeBoolean(boolean value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    public void writeBytes(Slice value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    public void writeBytes(Binary value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    public void writeInteger(int value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    public void writeLong(long value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    public void writeDouble(double value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    public void writeFloat(float value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    public abstract String memUsageString(String prefix);
}
