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
package io.trino.plugin.pinot.client;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.pinot.PinotException;
import io.trino.plugin.pinot.PinotSplit;
import org.apache.pinot.common.datatable.DataTable;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static io.trino.plugin.pinot.PinotErrorCode.PINOT_EXCEPTION;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public interface PinotDataFetcher
{
    default void checkExceptions(DataTable dataTable, PinotSplit split, String query)
    {
        List<String> exceptions = ImmutableList.copyOf(dataTable.getExceptions().values());
        if (!exceptions.isEmpty()) {
            throw new PinotException(PINOT_EXCEPTION, Optional.of(query), format("Encountered %d pinot exceptions for split %s: %s", exceptions.size(), split, exceptions));
        }
    }

    long getReadTimeNanos();

    long getMemoryUsageBytes();

    boolean endOfData();

    boolean isDataFetched();

    void fetchData();

    PinotDataTableWithSize getNextDataTable();

    class RowCountChecker
    {
        private final AtomicLong currentRowCount = new AtomicLong();
        private final int limit;
        private final String query;

        public RowCountChecker(int limit, String query)
        {
            this.limit = limit;
            this.query = requireNonNull(query, "query is null");
        }

        public void checkTooManyRows(DataTable dataTable)
        {
            if (currentRowCount.addAndGet(dataTable.getNumberOfRows()) > limit) {
                throw new PinotException(PINOT_EXCEPTION, Optional.of(query), format("Segment query returned '%s' rows per split, maximum allowed is '%s' rows.", currentRowCount.get(), limit));
            }
        }
    }

    interface Factory
    {
        PinotDataFetcher create(String query, PinotSplit split);

        int getRowLimit();
    }
}
