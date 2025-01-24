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
import io.trino.spi.connector.ConnectorPageSink;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.types.RowKind;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * TrinoPageSink.
 */
public class PaimonPageSink
        implements ConnectorPageSink
{
    private final BatchTableWrite writer;

    public PaimonPageSink(BatchTableWrite writer)
    {
        this.writer = writer;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        try {
            writePage(page, RowKind.INSERT);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return NOT_BLOCKED;
    }

    public void writePage(Page page, RowKind rowKind)
    {
        try {
            for (int i = 0; i < page.getPositionCount(); i++) {
                writer.write(new PaimonRow(page.getSingleValuePage(i), rowKind));
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        Collection<Slice> commitTasks = new ArrayList<>();
        try {
            List<CommitMessage> commitMessages = writer.prepareCommit();
            CommitMessageSerializer serializer = new CommitMessageSerializer();
            for (CommitMessage commitMessage : commitMessages) {
                commitTasks.add(wrappedBuffer(serializer.serialize(commitMessage)));
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            try {
                writer.close();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return completedFuture(commitTasks);
    }

    @Override
    public void abort()
    {
        try {
            writer.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
