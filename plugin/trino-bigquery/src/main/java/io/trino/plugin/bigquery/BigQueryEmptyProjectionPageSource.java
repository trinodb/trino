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
package io.trino.plugin.bigquery;

import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;

public class BigQueryEmptyProjectionPageSource
        implements ConnectorPageSource
{
    // This is used whenever a query doesn't reference any data columns.
    // We need to limit the number of rows per page in case there are projections
    // in the query that can cause page sizes to explode. For example: SELECT rand() FROM some_table
    // TODO (https://github.com/trinodb/trino/issues/16824) allow connector to return pages of arbitrary row count and handle this gracefully in engine
    private static final int MAX_RLE_PAGE_SIZE = DEFAULT_MAX_PAGE_SIZE_IN_BYTES / SIZE_OF_LONG;

    private final long numberOfRows;
    private long outputRows;

    public BigQueryEmptyProjectionPageSource(long numberOfRows)
    {
        this.numberOfRows = numberOfRows;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return outputRows == numberOfRows;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        int positionCount = toIntExact(min(MAX_RLE_PAGE_SIZE, numberOfRows - outputRows));
        outputRows += positionCount;
        return SourcePage.create(positionCount);
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        // nothing to do
    }
}
