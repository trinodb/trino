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

import io.airlift.slice.Slice;
import io.trino.parquet.AbstractParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;

import java.io.IOException;
import java.util.UUID;

import static java.lang.Math.toIntExact;

public class TestingParquetDataSource
        extends AbstractParquetDataSource
{
    private final Slice input;

    public TestingParquetDataSource(Slice slice, ParquetReaderOptions options)
            throws IOException
    {
        super(new ParquetDataSourceId(UUID.randomUUID().toString()), slice.length(), options);
        this.input = slice;
    }

    @Override
    protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        input.getBytes(toIntExact(position), buffer, bufferOffset, bufferLength);
    }
}
