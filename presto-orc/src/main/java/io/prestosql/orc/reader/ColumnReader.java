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
package io.prestosql.orc.reader;

import io.prestosql.orc.metadata.ColumnEncoding;
import io.prestosql.orc.metadata.ColumnMetadata;
import io.prestosql.orc.stream.InputStreamSources;
import io.prestosql.spi.block.Block;

import java.io.IOException;
import java.time.ZoneId;

public interface ColumnReader
{
    Block readBlock()
            throws IOException;

    void prepareNextRead(int batchSize);

    void startStripe(ZoneId timeZone, InputStreamSources dictionaryStreamSources, ColumnMetadata<ColumnEncoding> encoding)
            throws IOException;

    void startRowGroup(InputStreamSources dataStreamSources)
            throws IOException;

    void close();

    long getRetainedSizeInBytes();
}
