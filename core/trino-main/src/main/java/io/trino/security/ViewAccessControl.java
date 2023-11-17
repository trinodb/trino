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
package io.trino.security;

import io.trino.metadata.QualifiedObjectName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class ViewAccessControl
        extends ForwardingAccessControl
{
    private final AccessControl delegate;

    public ViewAccessControl(AccessControl delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    protected AccessControl delegate()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void checkCanSelectFromColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> columnNames)
    {
        // This is intentional and matches the SQL standard for view security.
        // In SQL, views are special in that they execute with permissions of the owner.
        // This means that the owner of the view is effectively granting permissions to the user running the query,
        // and thus must have the equivalent of the SQL standard "GRANT ... WITH GRANT OPTION".
        wrapAccessDeniedException(() -> delegate.checkCanCreateViewWithSelectFromColumns(context, tableName, columnNames));
    }

    @Override
    public Map<SchemaTableName, Set<String>> filterColumns(SecurityContext context, String catalogName, Map<SchemaTableName, Set<String>> tableColumns)
    {
        return delegate.filterColumns(context, catalogName, tableColumns);
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> columnNames)
    {
        wrapAccessDeniedException(() -> delegate.checkCanCreateViewWithSelectFromColumns(context, tableName, columnNames));
    }

    @Override
    public boolean canExecuteFunction(SecurityContext context, QualifiedObjectName functionName)
    {
        return delegate.canCreateViewWithExecuteFunction(context, functionName);
    }

    @Override
    public boolean canCreateViewWithExecuteFunction(SecurityContext context, QualifiedObjectName functionName)
    {
        return delegate.canCreateViewWithExecuteFunction(context, functionName);
    }

    @Override
    public List<ViewExpression> getRowFilters(SecurityContext context, QualifiedObjectName tableName)
    {
        return delegate.getRowFilters(context, tableName);
    }

    @Override
    public Optional<ViewExpression> getColumnMask(SecurityContext context, QualifiedObjectName tableName, String columnName, Type type)
    {
        return delegate.getColumnMask(context, tableName, columnName, type);
    }

    private static void wrapAccessDeniedException(Runnable runnable)
    {
        try {
            runnable.run();
        }
        catch (AccessDeniedException e) {
            String prefix = AccessDeniedException.PREFIX;
            verify(e.getMessage().startsWith(prefix));
            String msg = e.getMessage().substring(prefix.length());
            throw new AccessDeniedException("View owner does not have sufficient privileges: " + msg, e);
        }
    }
}
