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
package io.trino.plugin.redshift;

import io.airlift.slice.Slice;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.AbstractParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;

import java.io.IOException;

public class TrinoParquetDataSource
        extends AbstractParquetDataSource
{
    private final TrinoInput input;

    public TrinoParquetDataSource(TrinoInputFile file, ParquetReaderOptions options)
            throws IOException
    {
        super(new ParquetDataSourceId(file.location().toString()), file.length(), options);
        this.input = file.newInput();
    }

    @Override
    public void close()
            throws IOException
    {
        input.close();
    }

    @Override
    protected Slice readTailInternal(int length)
            throws IOException
    {
        return input.readTail(length);
    }

    @Override
    protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        input.readFully(position, buffer, bufferOffset, bufferLength);
    }
}
