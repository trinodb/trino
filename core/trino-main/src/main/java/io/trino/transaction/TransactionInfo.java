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
package io.trino.transaction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.transaction.IsolationLevel;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class TransactionInfo
{
    private final TransactionId transactionId;
    private final IsolationLevel isolationLevel;
    private final boolean readOnly;
    private final boolean autoCommitContext;
    private final DateTime createTime;
    private final Duration idleTime;
    private final List<String> catalogNames;
    private final Optional<String> writtenCatalogName;
    private final Set<CatalogHandle> activeCatalogs;

    public TransactionInfo(
            TransactionId transactionId,
            IsolationLevel isolationLevel,
            boolean readOnly,
            boolean autoCommitContext,
            DateTime createTime,
            Duration idleTime,
            List<String> catalogNames,
            Optional<String> writtenCatalogName,
            Set<CatalogHandle> activeCatalogs)
    {
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
        this.isolationLevel = requireNonNull(isolationLevel, "isolationLevel is null");
        this.readOnly = readOnly;
        this.autoCommitContext = autoCommitContext;
        this.createTime = requireNonNull(createTime, "createTime is null");
        this.idleTime = requireNonNull(idleTime, "idleTime is null");
        this.catalogNames = ImmutableList.copyOf(requireNonNull(catalogNames, "catalogNames is null"));
        this.writtenCatalogName = requireNonNull(writtenCatalogName, "writtenCatalogName is null");
        this.activeCatalogs = ImmutableSet.copyOf(requireNonNull(activeCatalogs, "activeCatalogs is null"));
    }

    public TransactionId getTransactionId()
    {
        return transactionId;
    }

    public IsolationLevel getIsolationLevel()
    {
        return isolationLevel;
    }

    public boolean isReadOnly()
    {
        return readOnly;
    }

    public boolean isAutoCommitContext()
    {
        return autoCommitContext;
    }

    public DateTime getCreateTime()
    {
        return createTime;
    }

    public Duration getIdleTime()
    {
        return idleTime;
    }

    public List<String> getCatalogNames()
    {
        return catalogNames;
    }

    public Optional<String> getWrittenCatalogName()
    {
        return writtenCatalogName;
    }

    public Set<CatalogHandle> getActiveCatalogs()
    {
        return activeCatalogs;
    }
}
