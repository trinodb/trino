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
package io.trino.plugin.paimon.functions.tablechanges;

import com.google.inject.Inject;
import io.trino.plugin.base.classloader.ClassLoaderSafeTableFunctionSplitProcessor;
import io.trino.plugin.paimon.PaimonPageSourceProvider;
import io.trino.plugin.paimon.PaimonSplit;
import io.trino.plugin.paimon.PaimonTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionSplitProcessor;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TableChangesFunctionProcessorProvider
        implements TableFunctionProcessorProvider
{
    private final PaimonPageSourceProvider pageSourceProvider;

    @Inject
    public TableChangesFunctionProcessorProvider(PaimonPageSourceProvider pageSourceProvider)
    {
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
    }

    @Override
    public TableFunctionSplitProcessor getSplitProcessor(
            ConnectorSession session,
            ConnectorTableFunctionHandle handle,
            Optional<ConnectorTableCredentials> tableCredentials,
            ConnectorSplit split)
    {
        requireNonNull(session, "session is null");
        requireNonNull(handle, "handle is null");
        requireNonNull(split, "split is null");
        if (!(handle instanceof PaimonTableHandle tableHandle)) {
            throw new IllegalArgumentException("handle must be PaimonTableHandle, got: " + handle.getClass().getName());
        }
        if (!(split instanceof PaimonSplit paimonSplit)) {
            throw new IllegalArgumentException("split must be PaimonSplit, got: " + split.getClass().getName());
        }
        return new ClassLoaderSafeTableFunctionSplitProcessor(new TableChangesFunctionProcessor(
                session,
                tableHandle,
                paimonSplit,
                pageSourceProvider), getClass().getClassLoader());
    }
}
