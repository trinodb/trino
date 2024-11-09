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
package io.trino.plugin.hive.util;

import com.google.common.collect.AbstractIterator;
import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcPredicate;
import io.trino.orc.OrcReader;
import io.trino.orc.OrcReaderOptions;
import io.trino.orc.OrcRecordReader;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_DATA_ERROR;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeZone.UTC;

public class TempFileReader
        extends AbstractIterator<Page>
{
    private final OrcRecordReader reader;

    public TempFileReader(List<Type> types, OrcDataSource dataSource)
    {
        requireNonNull(types, "types is null");

        try {
            OrcReader orcReader = OrcReader.createOrcReader(dataSource, new OrcReaderOptions())
                    .orElseThrow(() -> new TrinoException(HIVE_WRITER_DATA_ERROR, "Temporary data file is empty"));
            reader = orcReader.createRecordReader(
                    orcReader.getRootColumn().getNestedColumns(),
                    types,
                    OrcPredicate.TRUE,
                    UTC,
                    newSimpleAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE,
                    TempFileReader::handleException);
        }
        catch (IOException e) {
            throw handleException(e);
        }
    }

    @Override
    protected Page computeNext()
    {
        try {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedIOException();
            }

            SourcePage page = reader.nextPage();
            if (page == null) {
                return endOfData();
            }

            // eagerly load the page
            return page.getPage().getLoadedPage();
        }
        catch (IOException e) {
            throw handleException(e);
        }
    }

    private static TrinoException handleException(Exception e)
    {
        return new TrinoException(HIVE_WRITER_DATA_ERROR, "Failed to read temporary data", e);
    }
}
