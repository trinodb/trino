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
package io.prestosql.spi.connector.classloader;

import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorSecurityContext;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class ClassLoaderSafeConnectorAccessControl
        implements ConnectorAccessControl
{
    private final ConnectorAccessControl delegate;
    private final ClassLoader classLoader;

    public ClassLoaderSafeConnectorAccessControl(ConnectorAccessControl delegate, ClassLoader classLoader)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public void checkCanCreateSchema(ConnectorSecurityContext context, String schemaName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanCreateSchema(context, schemaName);
        }
    }

    @Override
    public void checkCanDropSchema(ConnectorSecurityContext context, String schemaName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanDropSchema(context, schemaName);
        }
    }

    @Override
    public void checkCanRenameSchema(ConnectorSecurityContext context, String schemaName, String newSchemaName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanRenameSchema(context, schemaName, newSchemaName);
        }
    }

    @Override
    public void checkCanShowSchemas(ConnectorSecurityContext context)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanShowSchemas(context);
        }
    }

    @Override
    public Set<String> filterSchemas(ConnectorSecurityContext context, Set<String> schemaNames)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.filterSchemas(context, schemaNames);
        }
    }

    @Override
    public void checkCanCreateTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanCreateTable(context, tableName);
        }
    }

    @Override
    public void checkCanDropTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanDropTable(context, tableName);
        }
    }

    @Override
    public void checkCanRenameTable(ConnectorSecurityContext context, SchemaTableName tableName, SchemaTableName newTableName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanRenameTable(context, tableName, newTableName);
        }
    }

    @Override
    public void checkCanSetTableComment(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanSetTableComment(context, tableName);
        }
    }

    @Override
    public void checkCanShowTablesMetadata(ConnectorSecurityContext context, String schemaName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanShowTablesMetadata(context, schemaName);
        }
    }

    @Override
    public Set<SchemaTableName> filterTables(ConnectorSecurityContext context, Set<SchemaTableName> tableNames)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.filterTables(context, tableNames);
        }
    }

    @Override
    public void checkCanShowColumnsMetadata(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanShowColumnsMetadata(context, tableName);
        }
    }

    @Override
    public List<ColumnMetadata> filterColumns(ConnectorSecurityContext context, SchemaTableName tableName, List<ColumnMetadata> columns)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            return delegate.filterColumns(context, tableName, columns);
        }
    }

    @Override
    public void checkCanAddColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanAddColumn(context, tableName);
        }
    }

    @Override
    public void checkCanDropColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanDropColumn(context, tableName);
        }
    }

    @Override
    public void checkCanRenameColumn(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanRenameColumn(context, tableName);
        }
    }

    @Override
    public void checkCanSelectFromColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> columnNames)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanSelectFromColumns(context, tableName, columnNames);
        }
    }

    @Override
    public void checkCanInsertIntoTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanInsertIntoTable(context, tableName);
        }
    }

    @Override
    public void checkCanDeleteFromTable(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanDeleteFromTable(context, tableName);
        }
    }

    @Override
    public void checkCanCreateView(ConnectorSecurityContext context, SchemaTableName viewName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanCreateView(context, viewName);
        }
    }

    @Override
    public void checkCanDropView(ConnectorSecurityContext context, SchemaTableName viewName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanDropView(context, viewName);
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(ConnectorSecurityContext context, SchemaTableName tableName, Set<String> columnNames)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanCreateViewWithSelectFromColumns(context, tableName, columnNames);
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(ConnectorSecurityContext context, String propertyName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanSetCatalogSessionProperty(context, propertyName);
        }
    }

    @Override
    public void checkCanGrantTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, PrestoPrincipal grantee, boolean withGrantOption)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanGrantTablePrivilege(context, privilege, tableName, grantee, withGrantOption);
        }
    }

    @Override
    public void checkCanRevokeTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, PrestoPrincipal revokee, boolean grantOptionFor)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanRevokeTablePrivilege(context, privilege, tableName, revokee, grantOptionFor);
        }
    }

    @Override
    public void checkCanCreateRole(ConnectorSecurityContext context, String role, Optional<PrestoPrincipal> grantor)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanCreateRole(context, role, grantor);
        }
    }

    @Override
    public void checkCanDropRole(ConnectorSecurityContext context, String role)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanDropRole(context, role);
        }
    }

    @Override
    public void checkCanGrantRoles(ConnectorSecurityContext context, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor, String catalogName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanGrantRoles(context, roles, grantees, withAdminOption, grantor, catalogName);
        }
    }

    @Override
    public void checkCanRevokeRoles(ConnectorSecurityContext context, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor, String catalogName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanRevokeRoles(context, roles, grantees, adminOptionFor, grantor, catalogName);
        }
    }

    @Override
    public void checkCanSetRole(ConnectorSecurityContext context, String role, String catalogName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanSetRole(context, role, catalogName);
        }
    }

    @Override
    public void checkCanShowRoles(ConnectorSecurityContext context, String catalogName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanShowRoles(context, catalogName);
        }
    }

    @Override
    public void checkCanShowCurrentRoles(ConnectorSecurityContext context, String catalogName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanShowCurrentRoles(context, catalogName);
        }
    }

    @Override
    public void checkCanShowRoleGrants(ConnectorSecurityContext context, String catalogName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            delegate.checkCanShowRoleGrants(context, catalogName);
        }
    }
}
