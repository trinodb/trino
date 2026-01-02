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
package io.trino.plugin.iceberg.functions.tablechanges;

import io.trino.plugin.iceberg.IcebergPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.function.table.TableFunctionSplitProcessor;

import java.io.IOException;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class MergeOnReadTableChangesFunctionProcessor
        implements TableFunctionSplitProcessor
{
    private final TableFunctionSplitProcessor delegate;

    public MergeOnReadTableChangesFunctionProcessor(
            ConnectorSession session,
            TableChangesFunctionHandle functionHandle,
            TableChangesSplit tableChangesSplit,
            IcebergPageSourceProvider icebergPageSourceProvider)
    {
        requireNonNull(session, "session is null");
        requireNonNull(functionHandle, "functionHandle is null");
        requireNonNull(tableChangesSplit, "tableChangesSplit is null");
        requireNonNull(icebergPageSourceProvider, "icebergPageSourceProvider is null");

        TableChangesInternalSplit split = (TableChangesInternalSplit) getOnlyElement(tableChangesSplit.splits());
        this.delegate = new InternalTableChangesFunctionProcessor(session, functionHandle, split, icebergPageSourceProvider);
    }

    @Override
    public TableFunctionProcessorState process()
    {
        return delegate.process();
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }
}
