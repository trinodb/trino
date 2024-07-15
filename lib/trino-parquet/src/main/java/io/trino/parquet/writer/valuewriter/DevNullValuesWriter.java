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

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;

/**
 * This is a special writer that doesn't write anything. The idea being that
 * some columns will always be the same value, and this will capture that. An
 * example is the set of repetition levels for a schema with no repeated fields.
 */
public class DevNullValuesWriter
        implements ColumnDescriptorValuesWriter
{
    @Override
    public long getBufferedSize()
    {
        return 0;
    }

    @Override
    public void reset() {}

    @Override
    public void writeInteger(int v) {}

    @Override
    public void writeRepeatInteger(int value, int valueRepetitions) {}

    @Override
    public BytesInput getBytes()
    {
        return BytesInput.empty();
    }

    @Override
    public long getAllocatedSize()
    {
        return 0;
    }

    @Override
    @SuppressWarnings("deprecation")
    public Encoding getEncoding()
    {
        return Encoding.BIT_PACKED;
    }
}
