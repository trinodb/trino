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
package io.prestosql.security;

import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.ViewExpression;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class ViewAccessControl
        extends DenyAllAccessControl
{
    private final AccessControl delegate;
    private final Identity invoker;

    public ViewAccessControl(AccessControl delegate, Identity invoker)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.invoker = requireNonNull(invoker, "invoker is null");
    }

    @Override
    public void checkCanSelectFromColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> columnNames)
    {
        // This is intentional and matches the SQL standard for view security.
        // In SQL, views are special in that they execute with permissions of the owner.
        // This means that the owner of the view is effectively granting permissions to the user running the query,
        // and thus must have the equivalent of the SQL standard "GRANT ... WITH GRANT OPTION".
        delegate.checkCanCreateViewWithSelectFromColumns(context, tableName, columnNames);
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> columnNames)
    {
        delegate.checkCanCreateViewWithSelectFromColumns(context, tableName, columnNames);
    }

    @Override
    public void checkCanExecuteFunction(SecurityContext context, String functionName)
    {
        delegate.checkCanGrantExecuteFunctionPrivilege(context, functionName, invoker, false);
    }

    @Override
    public void checkCanGrantExecuteFunctionPrivilege(SecurityContext context, String functionName, Identity grantee, boolean grantOption)
    {
        delegate.checkCanGrantExecuteFunctionPrivilege(context, functionName, grantee, grantOption);
    }

    @Override
    public List<ViewExpression> getRowFilters(SecurityContext context, QualifiedObjectName tableName)
    {
        return delegate.getRowFilters(context, tableName);
    }

    @Override
    public List<ViewExpression> getColumnMasks(SecurityContext context, QualifiedObjectName tableName, String columnName, Type type)
    {
        return delegate.getColumnMasks(context, tableName, columnName, type);
    }
}
