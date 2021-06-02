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
package io.trino.metadata;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public abstract class RedirectionAwareTableHandle
{
    private final Optional<TableHandle> tableHandle;

    protected RedirectionAwareTableHandle(Optional<TableHandle> tableHandle)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
    }

    public static RedirectionAwareTableHandle withRedirectionTo(QualifiedObjectName redirectedTableName, TableHandle tableHandle)
    {
        return new TableHandleWithRedirection(redirectedTableName, tableHandle);
    }

    public static RedirectionAwareTableHandle noRedirection(Optional<TableHandle> tableHandle)
    {
        return new TableHandleWithoutRedirection(tableHandle);
    }

    public Optional<TableHandle> getTableHandle()
    {
        return tableHandle;
    }

    /**
     * @return the target table name after redirection. Optional.empty() if the table is not redirected.
     */
    public abstract Optional<QualifiedObjectName> getRedirectedTableName();

    private static class TableHandleWithoutRedirection
            extends RedirectionAwareTableHandle
    {
        protected TableHandleWithoutRedirection(Optional<TableHandle> tableHandle)
        {
            super(tableHandle);
        }

        @Override
        public Optional<QualifiedObjectName> getRedirectedTableName()
        {
            return Optional.empty();
        }
    }

    private static class TableHandleWithRedirection
            extends RedirectionAwareTableHandle
    {
        private final QualifiedObjectName redirectedTableName;

        public TableHandleWithRedirection(QualifiedObjectName redirectedTableName, TableHandle tableHandle)
        {
            // Table handle must exist if there is redirection
            super(Optional.of(tableHandle));
            this.redirectedTableName = requireNonNull(redirectedTableName, "redirectedTableName is null");
        }

        @Override
        public Optional<QualifiedObjectName> getRedirectedTableName()
        {
            return Optional.of(redirectedTableName);
        }
    }
}
