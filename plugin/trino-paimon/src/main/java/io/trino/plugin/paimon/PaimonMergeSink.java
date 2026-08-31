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
package io.trino.plugin.paimon;

import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.MergePage;
import org.apache.paimon.types.RowKind;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PaimonMergeSink
        implements ConnectorMergeSink
{
    private final PaimonPageSink pageSink;
    private final int dataColumnCount;
    private final int[] dataColumnIndexes;

    public PaimonMergeSink(ConnectorPageSink pageSink, int dataColumnCount)
    {
        requireNonNull(pageSink, "pageSink is null");
        checkArgument(pageSink instanceof PaimonPageSink,
                "PaimonMergeSink requires PaimonPageSink, got: %s",
                pageSink.getClass().getName());
        checkArgument(dataColumnCount >= 0, "dataColumnCount must be non-negative: %s", dataColumnCount);
        this.pageSink = (PaimonPageSink) pageSink;
        this.dataColumnCount = dataColumnCount;
        this.dataColumnIndexes = IntStream.range(0, dataColumnCount).toArray();
    }

    @Override
    public void storeMergedRows(Page page)
    {
        requireNonNull(page, "page is null");
        try {
            validateInputPage(page);
            if (page.getPositionCount() == 0) {
                return;
            }

            MergePage mergePage = MergePage.createDeleteAndInsertPages(page, dataColumnCount);
            mergePage.getDeletionsPage()
                    .map(this::withoutRowIdColumn)
                    .ifPresent(delete -> pageSink.writePage(delete, RowKind.DELETE));
            mergePage.getInsertionsPage()
                    .ifPresent(insert -> pageSink.writePage(insert, RowKind.INSERT));
        }
        catch (Exception e) {
            throw PaimonPageSink.wrapWriteException(e);
        }
    }

    private void validateInputPage(Page page)
    {
        int inputChannelCount = page.getChannelCount();
        if (inputChannelCount != dataColumnCount + 3) {
            throw new IllegalArgumentException("inputPage channelCount (%s) must equal dataColumns size (%s) + 3"
                    .formatted(inputChannelCount, dataColumnCount));
        }
    }

    private Page withoutRowIdColumn(Page deletePage)
    {
        return deletePage.getColumns(dataColumnIndexes);
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return pageSink.finish();
    }

    @Override
    public void abort()
    {
        pageSink.abort();
    }
}
