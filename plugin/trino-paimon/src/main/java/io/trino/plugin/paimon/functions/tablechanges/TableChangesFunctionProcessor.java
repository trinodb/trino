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

import io.trino.plugin.paimon.PaimonColumnHandle;
import io.trino.plugin.paimon.PaimonErrorCode;
import io.trino.plugin.paimon.PaimonPageSourceProvider;
import io.trino.plugin.paimon.PaimonSplit;
import io.trino.plugin.paimon.PaimonTableHandle;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.function.table.TableFunctionSplitProcessor;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.spi.function.table.TableFunctionProcessorState.Blocked.blocked;
import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static io.trino.spi.function.table.TableFunctionProcessorState.Processed.produced;
import static java.util.Objects.requireNonNull;

public class TableChangesFunctionProcessor
        implements TableFunctionSplitProcessor
{
    private static final String CLOSE_PAGE_SOURCE_ERROR = "Failed to close Paimon table_changes page source";

    private final ConnectorPageSource pageSource;
    private boolean closed;

    public TableChangesFunctionProcessor(
            ConnectorSession session,
            PaimonTableHandle handle,
            PaimonSplit split,
            PaimonPageSourceProvider pageSourceProvider)
    {
        requireNonNull(session, "session is null");
        requireNonNull(split, "split is null");
        requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        List<?> rawProjectedColumns = requireNonNull(
                requireNonNull(handle, "handle is null").getProjectedColumns(),
                "projectedColumns is null")
                .orElseThrow(() -> new IllegalStateException(
                        "Paimon table_changes requires explicit projected columns"));
        List<ColumnHandle> projectedColumns = rawProjectedColumns
                .stream()
                .map(column -> {
                    if (!(requireNonNull(column, "projectedColumns contains null column") instanceof PaimonColumnHandle paimonColumnHandle)) {
                        throw new IllegalStateException("Paimon table_changes requires PaimonColumnHandle, got: "
                                + column.getClass().getName());
                    }
                    return (ColumnHandle) paimonColumnHandle;
                })
                .toList();
        this.pageSource = pageSourceProvider.createPageSource(
                null,
                session,
                split,
                handle,
                Optional.<ConnectorTableCredentials>empty(),
                projectedColumns,
                DynamicFilter.EMPTY,
                MemoryContext.NO_LIMIT);
    }

    @Override
    public TableFunctionProcessorState process()
    {
        boolean closing = false;
        try {
            if (pageSource.isFinished()) {
                closing = true;
                closeIfNecessary();
                return FINISHED;
            }
            SourcePage sourcePage = pageSource.getNextSourcePage();
            Page dataPage = sourcePage == null ? null : sourcePage.getPage();
            if (dataPage == null) {
                if (pageSource.isFinished()) {
                    closing = true;
                    closeIfNecessary();
                    return FINISHED;
                }
                return blocked(pageSource.isBlocked().thenRun(() -> {}));
            }
            return produced(dataPage);
        }
        catch (RuntimeException e) {
            if (!closing) {
                closeAllSuppress(e, pageSource);
            }
            throw e;
        }
    }

    private void closeIfNecessary()
    {
        if (closed) {
            return;
        }
        try {
            pageSource.close();
            closed = true;
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (IOException e) {
            throw new TrinoException(
                    PaimonErrorCode.PAIMON_CANNOT_OPEN_SPLIT,
                    CLOSE_PAGE_SOURCE_ERROR,
                    e);
        }
        catch (RuntimeException e) {
            throw new TrinoException(
                    PaimonErrorCode.PAIMON_CANNOT_OPEN_SPLIT,
                    CLOSE_PAGE_SOURCE_ERROR,
                    e);
        }
    }
}
