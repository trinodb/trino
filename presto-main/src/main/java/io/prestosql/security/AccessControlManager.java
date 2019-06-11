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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.prestosql.FullConnectorSecurityContext;
import io.prestosql.connector.CatalogName;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.plugin.base.security.AllowAllSystemAccessControl;
import io.prestosql.plugin.base.security.FileBasedSystemAccessControl;
import io.prestosql.plugin.base.security.ReadOnlySystemAccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorSecurityContext;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.spi.security.SystemAccessControlFactory;
import io.prestosql.spi.security.SystemSecurityContext;
import io.prestosql.transaction.TransactionId;
import io.prestosql.transaction.TransactionManager;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.io.File;
import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.prestosql.spi.StandardErrorCode.SERVER_STARTING_UP;
import static io.prestosql.util.PropertiesUtil.loadProperties;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AccessControlManager
        implements AccessControl
{
    private static final Logger log = Logger.get(AccessControlManager.class);
    private static final File ACCESS_CONTROL_CONFIGURATION = new File("etc/access-control.properties");
    private static final String ACCESS_CONTROL_PROPERTY_NAME = "access-control.name";

    private final TransactionManager transactionManager;
    private final Map<String, SystemAccessControlFactory> systemAccessControlFactories = new ConcurrentHashMap<>();
    private final Map<CatalogName, CatalogAccessControlEntry> connectorAccessControl = new ConcurrentHashMap<>();

    private final AtomicReference<SystemAccessControl> systemAccessControl = new AtomicReference<>(new InitializingSystemAccessControl());
    private final AtomicBoolean systemAccessControlLoading = new AtomicBoolean();

    private final CounterStat authenticationSuccess = new CounterStat();
    private final CounterStat authenticationFail = new CounterStat();
    private final CounterStat authorizationSuccess = new CounterStat();
    private final CounterStat authorizationFail = new CounterStat();

    @Inject
    public AccessControlManager(TransactionManager transactionManager)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        addSystemAccessControlFactory(new AllowAllSystemAccessControl.Factory());
        addSystemAccessControlFactory(new ReadOnlySystemAccessControl.Factory());
        addSystemAccessControlFactory(new FileBasedSystemAccessControl.Factory());
    }

    public void addSystemAccessControlFactory(SystemAccessControlFactory accessControlFactory)
    {
        requireNonNull(accessControlFactory, "accessControlFactory is null");

        if (systemAccessControlFactories.putIfAbsent(accessControlFactory.getName(), accessControlFactory) != null) {
            throw new IllegalArgumentException(format("Access control '%s' is already registered", accessControlFactory.getName()));
        }
    }

    public void addCatalogAccessControl(CatalogName catalogName, ConnectorAccessControl accessControl)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(accessControl, "accessControl is null");
        checkState(connectorAccessControl.putIfAbsent(catalogName, new CatalogAccessControlEntry(catalogName, accessControl)) == null,
                "Access control for connector '%s' is already registered", catalogName);
    }

    public void removeCatalogAccessControl(CatalogName catalogName)
    {
        connectorAccessControl.remove(catalogName);
    }

    public void loadSystemAccessControl()
            throws Exception
    {
        if (ACCESS_CONTROL_CONFIGURATION.exists()) {
            Map<String, String> properties = new HashMap<>(loadProperties(ACCESS_CONTROL_CONFIGURATION));

            String accessControlName = properties.remove(ACCESS_CONTROL_PROPERTY_NAME);
            checkArgument(!isNullOrEmpty(accessControlName),
                    "Access control configuration %s does not contain %s", ACCESS_CONTROL_CONFIGURATION.getAbsoluteFile(), ACCESS_CONTROL_PROPERTY_NAME);

            setSystemAccessControl(accessControlName, properties);
        }
        else {
            setSystemAccessControl(AllowAllSystemAccessControl.NAME, ImmutableMap.of());
        }
    }

    @VisibleForTesting
    protected void setSystemAccessControl(String name, Map<String, String> properties)
    {
        requireNonNull(name, "name is null");
        requireNonNull(properties, "properties is null");

        checkState(systemAccessControlLoading.compareAndSet(false, true), "System access control already initialized");

        log.info("-- Loading system access control --");

        SystemAccessControlFactory systemAccessControlFactory = systemAccessControlFactories.get(name);
        checkState(systemAccessControlFactory != null, "Access control %s is not registered", name);

        SystemAccessControl systemAccessControl = systemAccessControlFactory.create(ImmutableMap.copyOf(properties));
        this.systemAccessControl.set(systemAccessControl);

        log.info("-- Loaded system access control %s --", name);
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        requireNonNull(principal, "principal is null");
        requireNonNull(userName, "userName is null");

        authenticationCheck(() -> systemAccessControl.get().checkCanSetUser(principal, userName));
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, Set<String> catalogs)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogs, "catalogs is null");

        return systemAccessControl.get().filterCatalogs(new SystemSecurityContext(identity), catalogs);
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, String catalogName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogName, "catalog is null");

        authenticationCheck(() -> systemAccessControl.get().checkCanAccessCatalog(new SystemSecurityContext(identity), catalogName));
    }

    @Override
    public void checkCanCreateSchema(SecurityContext context, CatalogSchemaName schemaName)
    {
        requireNonNull(context, "context is null");
        requireNonNull(schemaName, "schemaName is null");

        authenticationCheck(() -> checkCanAccessCatalog(context.getIdentity(), schemaName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanCreateSchema(context.toSystemSecurityContext(), schemaName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), schemaName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanCreateSchema(entry.toConnectorSecurityContext(context), schemaName.getSchemaName()));
        }
    }

    @Override
    public void checkCanDropSchema(SecurityContext context, CatalogSchemaName schemaName)
    {
        requireNonNull(context, "context is null");
        requireNonNull(schemaName, "schemaName is null");

        authenticationCheck(() -> checkCanAccessCatalog(context.getIdentity(), schemaName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanDropSchema(context.toSystemSecurityContext(), schemaName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), schemaName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDropSchema(entry.toConnectorSecurityContext(context), schemaName.getSchemaName()));
        }
    }

    @Override
    public void checkCanRenameSchema(SecurityContext context, CatalogSchemaName schemaName, String newSchemaName)
    {
        requireNonNull(context, "context is null");
        requireNonNull(schemaName, "schemaName is null");

        authenticationCheck(() -> checkCanAccessCatalog(context.getIdentity(), schemaName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanRenameSchema(context.toSystemSecurityContext(), schemaName, newSchemaName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), schemaName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanRenameSchema(entry.toConnectorSecurityContext(context), schemaName.getSchemaName(), newSchemaName));
        }
    }

    @Override
    public void checkCanShowSchemas(SecurityContext context, String catalogName)
    {
        requireNonNull(context, "context is null");
        requireNonNull(catalogName, "catalogName is null");

        authenticationCheck(() -> checkCanAccessCatalog(context.getIdentity(), catalogName));

        authorizationCheck(() -> systemAccessControl.get().checkCanShowSchemas(context.toSystemSecurityContext(), catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), catalogName);
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanShowSchemas(entry.toConnectorSecurityContext(context)));
        }
    }

    @Override
    public Set<String> filterSchemas(SecurityContext context, String catalogName, Set<String> schemaNames)
    {
        requireNonNull(context, "context is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(schemaNames, "schemaNames is null");

        if (filterCatalogs(context.getIdentity(), ImmutableSet.of(catalogName)).isEmpty()) {
            return ImmutableSet.of();
        }

        schemaNames = systemAccessControl.get().filterSchemas(context.toSystemSecurityContext(), catalogName, schemaNames);

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), catalogName);
        if (entry != null) {
            schemaNames = entry.getAccessControl().filterSchemas(entry.toConnectorSecurityContext(context), schemaNames);
        }
        return schemaNames;
    }

    @Override
    public void checkCanCreateTable(SecurityContext context, QualifiedObjectName tableName)
    {
        requireNonNull(context, "context is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(context.getIdentity(), tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanCreateTable(context.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanCreateTable(entry.toConnectorSecurityContext(context), tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanDropTable(SecurityContext context, QualifiedObjectName tableName)
    {
        requireNonNull(context, "context is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(context.getIdentity(), tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanDropTable(context.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDropTable(entry.toConnectorSecurityContext(context), tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanRenameTable(SecurityContext context, QualifiedObjectName tableName, QualifiedObjectName newTableName)
    {
        requireNonNull(context, "context is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(newTableName, "newTableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(context.getIdentity(), tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanRenameTable(context.toSystemSecurityContext(), tableName.asCatalogSchemaTableName(), newTableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanRenameTable(entry.toConnectorSecurityContext(context), tableName.asSchemaTableName(), newTableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanSetTableComment(SecurityContext context, QualifiedObjectName tableName)
    {
        requireNonNull(context, "context is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(context.getIdentity(), tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanSetTableComment(context.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanSetTableComment(entry.toConnectorSecurityContext(context), tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanShowTablesMetadata(SecurityContext context, CatalogSchemaName schema)
    {
        requireNonNull(context, "context is null");
        requireNonNull(schema, "schema is null");

        authenticationCheck(() -> checkCanAccessCatalog(context.getIdentity(), schema.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanShowTablesMetadata(context.toSystemSecurityContext(), schema));

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), schema.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanShowTablesMetadata(entry.toConnectorSecurityContext(context), schema.getSchemaName()));
        }
    }

    @Override
    public Set<SchemaTableName> filterTables(SecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        requireNonNull(context, "context is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(tableNames, "tableNames is null");

        if (filterCatalogs(context.getIdentity(), ImmutableSet.of(catalogName)).isEmpty()) {
            return ImmutableSet.of();
        }

        tableNames = systemAccessControl.get().filterTables(context.toSystemSecurityContext(), catalogName, tableNames);

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), catalogName);
        if (entry != null) {
            tableNames = entry.getAccessControl().filterTables(entry.toConnectorSecurityContext(context), tableNames);
        }
        return tableNames;
    }

    @Override
    public void checkCanShowColumnsMetadata(SecurityContext context, CatalogSchemaTableName table)
    {
        requireNonNull(context, "context is null");
        requireNonNull(table, "table is null");

        authorizationCheck(() -> systemAccessControl.get().checkCanShowColumnsMetadata(context.toSystemSecurityContext(), table));

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), table.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanShowColumnsMetadata(entry.toConnectorSecurityContext(context), table.getSchemaTableName()));
        }
    }

    @Override
    public List<ColumnMetadata> filterColumns(SecurityContext context, CatalogSchemaTableName table, List<ColumnMetadata> columns)
    {
        requireNonNull(context, "context is null");
        requireNonNull(table, "tableName is null");

        if (filterTables(context, table.getCatalogName(), ImmutableSet.of(table.getSchemaTableName())).isEmpty()) {
            return ImmutableList.of();
        }

        columns = systemAccessControl.get().filterColumns(context.toSystemSecurityContext(), table, columns);

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), table.getCatalogName());
        if (entry != null) {
            columns = entry.getAccessControl().filterColumns(entry.toConnectorSecurityContext(context), table.getSchemaTableName(), columns);
        }
        return columns;
    }

    @Override
    public void checkCanAddColumns(SecurityContext context, QualifiedObjectName tableName)
    {
        requireNonNull(context, "context is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(context.getIdentity(), tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanAddColumn(context.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanAddColumn(entry.toConnectorSecurityContext(context), tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanDropColumn(SecurityContext context, QualifiedObjectName tableName)
    {
        requireNonNull(context, "context is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(context.getIdentity(), tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanDropColumn(context.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDropColumn(entry.toConnectorSecurityContext(context), tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanRenameColumn(SecurityContext context, QualifiedObjectName tableName)
    {
        requireNonNull(context, "context is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(context.getIdentity(), tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanRenameColumn(context.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanRenameColumn(entry.toConnectorSecurityContext(context), tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanInsertIntoTable(SecurityContext context, QualifiedObjectName tableName)
    {
        requireNonNull(context, "context is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(context.getIdentity(), tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanInsertIntoTable(context.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanInsertIntoTable(entry.toConnectorSecurityContext(context), tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanDeleteFromTable(SecurityContext context, QualifiedObjectName tableName)
    {
        requireNonNull(context, "context is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(context.getIdentity(), tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanDeleteFromTable(context.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDeleteFromTable(entry.toConnectorSecurityContext(context), tableName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanCreateView(SecurityContext context, QualifiedObjectName viewName)
    {
        requireNonNull(context, "context is null");
        requireNonNull(viewName, "viewName is null");

        authenticationCheck(() -> checkCanAccessCatalog(context.getIdentity(), viewName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanCreateView(context.toSystemSecurityContext(), viewName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), viewName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanCreateView(entry.toConnectorSecurityContext(context), viewName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanDropView(SecurityContext context, QualifiedObjectName viewName)
    {
        requireNonNull(context, "context is null");
        requireNonNull(viewName, "viewName is null");

        authenticationCheck(() -> checkCanAccessCatalog(context.getIdentity(), viewName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanDropView(context.toSystemSecurityContext(), viewName.asCatalogSchemaTableName()));

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), viewName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDropView(entry.toConnectorSecurityContext(context), viewName.asSchemaTableName()));
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> columnNames)
    {
        requireNonNull(context, "context is null");
        requireNonNull(tableName, "tableName is null");

        authenticationCheck(() -> checkCanAccessCatalog(context.getIdentity(), tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanCreateViewWithSelectFromColumns(context.toSystemSecurityContext(), tableName.asCatalogSchemaTableName(), columnNames));

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanCreateViewWithSelectFromColumns(entry.toConnectorSecurityContext(context), tableName.asSchemaTableName(), columnNames));
        }
    }

    @Override
    public void checkCanGrantTablePrivilege(SecurityContext context, Privilege privilege, QualifiedObjectName tableName, PrestoPrincipal grantee, boolean withGrantOption)
    {
        requireNonNull(context, "context is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(privilege, "privilege is null");

        authenticationCheck(() -> checkCanAccessCatalog(context.getIdentity(), tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanGrantTablePrivilege(context.toSystemSecurityContext(), privilege, tableName.asCatalogSchemaTableName(), grantee, withGrantOption));

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanGrantTablePrivilege(entry.toConnectorSecurityContext(context), privilege, tableName.asSchemaTableName(), grantee, withGrantOption));
        }
    }

    @Override
    public void checkCanRevokeTablePrivilege(SecurityContext context, Privilege privilege, QualifiedObjectName tableName, PrestoPrincipal revokee, boolean grantOptionFor)
    {
        requireNonNull(context, "context is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(privilege, "privilege is null");

        authenticationCheck(() -> checkCanAccessCatalog(context.getIdentity(), tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanRevokeTablePrivilege(context.toSystemSecurityContext(), privilege, tableName.asCatalogSchemaTableName(), revokee, grantOptionFor));

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanRevokeTablePrivilege(entry.toConnectorSecurityContext(context), privilege, tableName.asSchemaTableName(), revokee, grantOptionFor));
        }
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(propertyName, "propertyName is null");

        authorizationCheck(() -> systemAccessControl.get().checkCanSetSystemSessionProperty(new SystemSecurityContext(identity), propertyName));
    }

    @Override
    public void checkCanSetCatalogSessionProperty(TransactionId transactionId, Identity identity, String catalogName, String propertyName)
    {
        requireNonNull(transactionId, "transactionId is null");
        requireNonNull(identity, "identity is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(propertyName, "propertyName is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, catalogName));

        authorizationCheck(() -> systemAccessControl.get().checkCanSetCatalogSessionProperty(new SystemSecurityContext(identity), catalogName, propertyName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanSetCatalogSessionProperty(entry.toConnectorSecurityContext(transactionId, identity), propertyName));
        }
    }

    @Override
    public void checkCanSelectFromColumns(SecurityContext context, QualifiedObjectName tableName, Set<String> columnNames)
    {
        requireNonNull(context, "context is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(columnNames, "columnNames is null");

        authenticationCheck(() -> checkCanAccessCatalog(context.getIdentity(), tableName.getCatalogName()));

        authorizationCheck(() -> systemAccessControl.get().checkCanSelectFromColumns(context.toSystemSecurityContext(), tableName.asCatalogSchemaTableName(), columnNames));

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), tableName.getCatalogName());
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanSelectFromColumns(entry.toConnectorSecurityContext(context), tableName.asSchemaTableName(), columnNames));
        }
    }

    @Override
    public void checkCanCreateRole(SecurityContext context, String role, Optional<PrestoPrincipal> grantor, String catalogName)
    {
        requireNonNull(context, "context is null");
        requireNonNull(role, "role is null");
        requireNonNull(grantor, "grantor is null");
        requireNonNull(catalogName, "catalogName is null");

        authenticationCheck(() -> checkCanAccessCatalog(context.getIdentity(), catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), catalogName);
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanCreateRole(entry.toConnectorSecurityContext(context), role, grantor));
        }
    }

    @Override
    public void checkCanDropRole(SecurityContext context, String role, String catalogName)
    {
        requireNonNull(context, "context is null");
        requireNonNull(role, "role is null");
        requireNonNull(catalogName, "catalogName is null");

        authenticationCheck(() -> checkCanAccessCatalog(context.getIdentity(), catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), catalogName);
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanDropRole(entry.toConnectorSecurityContext(context), role));
        }
    }

    @Override
    public void checkCanGrantRoles(SecurityContext context, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor, String catalogName)
    {
        requireNonNull(context, "context is null");
        requireNonNull(roles, "roles is null");
        requireNonNull(grantees, "grantees is null");
        requireNonNull(grantor, "grantor is null");
        requireNonNull(catalogName, "catalogName is null");

        authenticationCheck(() -> checkCanAccessCatalog(context.getIdentity(), catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), catalogName);
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanGrantRoles(entry.toConnectorSecurityContext(context), roles, grantees, withAdminOption, grantor, catalogName));
        }
    }

    @Override
    public void checkCanRevokeRoles(SecurityContext context, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor, String catalogName)
    {
        requireNonNull(context, "context is null");
        requireNonNull(roles, "roles is null");
        requireNonNull(grantees, "grantees is null");
        requireNonNull(grantor, "grantor is null");
        requireNonNull(catalogName, "catalogName is null");

        authenticationCheck(() -> checkCanAccessCatalog(context.getIdentity(), catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), catalogName);
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanRevokeRoles(entry.toConnectorSecurityContext(context), roles, grantees, adminOptionFor, grantor, catalogName));
        }
    }

    @Override
    public void checkCanSetRole(TransactionId transactionId, Identity identity, String role, String catalogName)
    {
        requireNonNull(transactionId, "transactionId is null");
        requireNonNull(identity, "identity is null");
        requireNonNull(role, "role is null");
        requireNonNull(catalogName, "catalog is null");

        authenticationCheck(() -> checkCanAccessCatalog(identity, catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(transactionId, catalogName);
        if (entry != null) {
            authorizationCheck(() -> entry.getAccessControl().checkCanSetRole(entry.toConnectorSecurityContext(transactionId, identity), role, catalogName));
        }
    }

    @Override
    public void checkCanShowRoles(SecurityContext context, String catalogName)
    {
        requireNonNull(context, "context is null");
        requireNonNull(catalogName, "catalogName is null");

        authenticationCheck(() -> checkCanAccessCatalog(context.getIdentity(), catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), catalogName);
        if (entry != null) {
            authenticationCheck(() -> entry.getAccessControl().checkCanShowRoles(entry.toConnectorSecurityContext(context), catalogName));
        }
    }

    @Override
    public void checkCanShowCurrentRoles(SecurityContext context, String catalogName)
    {
        requireNonNull(context, "context is null");
        requireNonNull(catalogName, "catalogName is null");

        authenticationCheck(() -> checkCanAccessCatalog(context.getIdentity(), catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), catalogName);
        if (entry != null) {
            authenticationCheck(() -> entry.getAccessControl().checkCanShowCurrentRoles(entry.toConnectorSecurityContext(context), catalogName));
        }
    }

    @Override
    public void checkCanShowRoleGrants(SecurityContext context, String catalogName)
    {
        requireNonNull(context, "context is null");
        requireNonNull(catalogName, "catalogName is null");

        authenticationCheck(() -> checkCanAccessCatalog(context.getIdentity(), catalogName));

        CatalogAccessControlEntry entry = getConnectorAccessControl(context.getTransactionId(), catalogName);
        if (entry != null) {
            authenticationCheck(() -> entry.getAccessControl().checkCanShowRoleGrants(entry.toConnectorSecurityContext(context), catalogName));
        }
    }

    private CatalogAccessControlEntry getConnectorAccessControl(TransactionId transactionId, String catalogName)
    {
        return transactionManager.getOptionalCatalogMetadata(transactionId, catalogName)
                .map(metadata -> connectorAccessControl.get(metadata.getCatalogName()))
                .orElse(null);
    }

    @Managed
    @Nested
    public CounterStat getAuthenticationSuccess()
    {
        return authenticationSuccess;
    }

    @Managed
    @Nested
    public CounterStat getAuthenticationFail()
    {
        return authenticationFail;
    }

    @Managed
    @Nested
    public CounterStat getAuthorizationSuccess()
    {
        return authorizationSuccess;
    }

    @Managed
    @Nested
    public CounterStat getAuthorizationFail()
    {
        return authorizationFail;
    }

    private void authenticationCheck(Runnable runnable)
    {
        try {
            runnable.run();
            authenticationSuccess.update(1);
        }
        catch (PrestoException e) {
            authenticationFail.update(1);
            throw e;
        }
    }

    private void authorizationCheck(Runnable runnable)
    {
        try {
            runnable.run();
            authorizationSuccess.update(1);
        }
        catch (PrestoException e) {
            authorizationFail.update(1);
            throw e;
        }
    }

    private class CatalogAccessControlEntry
    {
        private final CatalogName catalogName;
        private final ConnectorAccessControl accessControl;

        public CatalogAccessControlEntry(CatalogName catalogName, ConnectorAccessControl accessControl)
        {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            this.accessControl = requireNonNull(accessControl, "accessControl is null");
        }

        public CatalogName getCatalogName()
        {
            return catalogName;
        }

        public ConnectorAccessControl getAccessControl()
        {
            return accessControl;
        }

        public ConnectorTransactionHandle getTransactionHandle(TransactionId transactionId)
        {
            return transactionManager.getConnectorTransaction(transactionId, catalogName);
        }

        public ConnectorSecurityContext toConnectorSecurityContext(SecurityContext context)
        {
            return toConnectorSecurityContext(context.getTransactionId(), context.getIdentity());
        }

        public ConnectorSecurityContext toConnectorSecurityContext(TransactionId requiredTransactionId, Identity identity)
        {
            return new FullConnectorSecurityContext(
                    transactionManager.getConnectorTransaction(requiredTransactionId, catalogName),
                    identity.toConnectorIdentity(catalogName.getCatalogName()));
        }
    }

    private static class InitializingSystemAccessControl
            implements SystemAccessControl
    {
        @Override
        public void checkCanSetUser(Optional<Principal> principal, String userName)
        {
            throw new PrestoException(SERVER_STARTING_UP, "Presto server is still initializing");
        }

        @Override
        public void checkCanSetSystemSessionProperty(SystemSecurityContext context, String propertyName)
        {
            throw new PrestoException(SERVER_STARTING_UP, "Presto server is still initializing");
        }

        @Override
        public void checkCanAccessCatalog(SystemSecurityContext context, String catalogName)
        {
            throw new PrestoException(SERVER_STARTING_UP, "Presto server is still initializing");
        }
    }
}
