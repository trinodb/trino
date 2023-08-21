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
package io.trino.parquet.reader;

import io.trino.parquet.AbstractParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FileParquetDataSource
        extends AbstractParquetDataSource
{
    private final RandomAccessFile input;

    public FileParquetDataSource(File path, ParquetReaderOptions options)
            throws FileNotFoundException
    {
        super(new ParquetDataSourceId(path.getPath()), path.length(), options);
        this.input = new RandomAccessFile(path, "r");
    }

    @Override
    public void close()
            throws IOException
    {
        super.close();
        input.close();
    }

    @Override
    protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        input.seek(position);
        input.readFully(buffer, bufferOffset, bufferLength);
    }
}
