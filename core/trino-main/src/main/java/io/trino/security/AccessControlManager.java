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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.trino.connector.CatalogServiceProvider;
import io.trino.eventlistener.EventListenerManager;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.plugin.base.security.DefaultSystemAccessControl;
import io.trino.plugin.base.security.FileBasedSystemAccessControl;
import io.trino.plugin.base.security.ForwardingSystemAccessControl;
import io.trino.plugin.base.security.ReadOnlySystemAccessControl;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.CatalogHandle.CatalogHandleType;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSecurityContext;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.FunctionKind;
import io.trino.spi.security.Identity;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemAccessControlFactory;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.Type;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.Principal;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_MASK;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SERVER_STARTING_UP;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AccessControlManager
        implements AccessControl
{
    private static final Logger log = Logger.get(AccessControlManager.class);

    private static final File CONFIG_FILE = new File("etc/access-control.properties");
    private static final String NAME_PROPERTY = "access-control.name";

    private final TransactionManager transactionManager;
    private final EventListenerManager eventListenerManager;
    private final List<File> configFiles;
    private final String defaultAccessControlName;
    private final Map<String, SystemAccessControlFactory> systemAccessControlFactories = new ConcurrentHashMap<>();
    private final AtomicReference<CatalogServiceProvider<Optional<ConnectorAccessControl>>> connectorAccessControlProvider = new AtomicReference<>();

    private final AtomicReference<List<SystemAccessControl>> systemAccessControls = new AtomicReference<>();

    private final CounterStat authorizationSuccess = new CounterStat();
    private final CounterStat authorizationFail = new CounterStat();

    @Inject
    public AccessControlManager(
            TransactionManager transactionManager,
            EventListenerManager eventListenerManager,
            AccessControlConfig config,
            @DefaultSystemAccessControlName String defaultAccessControlName)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.eventListenerManager = requireNonNull(eventListenerManager, "eventListenerManager is null");
        this.configFiles = ImmutableList.copyOf(config.getAccessControlFiles());
        this.defaultAccessControlName = requireNonNull(defaultAccessControlName, "defaultAccessControl is null");
        addSystemAccessControlFactory(new DefaultSystemAccessControl.Factory());
        addSystemAccessControlFactory(new AllowAllSystemAccessControl.Factory());
        addSystemAccessControlFactory(new ReadOnlySystemAccessControl.Factory());
        addSystemAccessControlFactory(new FileBasedSystemAccessControl.Factory());
    }

    public final void addSystemAccessControlFactory(SystemAccessControlFactory accessControlFactory)
    {
        requireNonNull(accessControlFactory, "accessControlFactory is null");

        if (systemAccessControlFactories.putIfAbsent(accessControlFactory.getName(), accessControlFactory) != null) {
            throw new IllegalArgumentException(format("Access control '%s' is already registered", accessControlFactory.getName()));
        }
    }

    /**
     * Lazy registry for connector access controls due to circular dependency between access control and connector creation in CatalogManager.
     */
    public void setConnectorAccessControlProvider(CatalogServiceProvider<Optional<ConnectorAccessControl>> connectorAccessControlProvider)
    {
        if (!this.connectorAccessControlProvider.compareAndSet(null, connectorAccessControlProvider)) {
            throw new IllegalStateException("connectorAccessControlProvider already set");
        }
    }

    public void loadSystemAccessControl()
    {
        List<File> configFiles = this.configFiles;
        if (configFiles.isEmpty()) {
            if (!CONFIG_FILE.exists()) {
                loadSystemAccessControl(defaultAccessControlName, ImmutableMap.of());
                log.info("Using system access control: %s", defaultAccessControlName);
                return;
            }
            configFiles = ImmutableList.of(CONFIG_FILE);
        }

        List<SystemAccessControl> systemAccessControls = configFiles.stream()
                .map(this::createSystemAccessControl)
                .collect(toImmutableList());

        systemAccessControls.stream()
                .map(SystemAccessControl::getEventListeners)
                .flatMap(listeners -> ImmutableSet.copyOf(listeners).stream())
                .forEach(eventListenerManager::addEventListener);

        setSystemAccessControls(systemAccessControls);
    }

    private SystemAccessControl createSystemAccessControl(File configFile)
    {
        log.info("-- Loading system access control %s --", configFile);
        configFile = configFile.getAbsoluteFile();

        Map<String, String> properties;
        try {
            properties = new HashMap<>(loadPropertiesFrom(configFile.getPath()));
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read configuration file: " + configFile, e);
        }

        String name = properties.remove(NAME_PROPERTY);
        checkState(!isNullOrEmpty(name), "Access control configuration does not contain '%s' property: %s", NAME_PROPERTY, configFile);

        SystemAccessControlFactory factory = systemAccessControlFactories.get(name);
        checkState(factory != null, "Access control '%s' is not registered: %s", name, configFile);

        SystemAccessControl systemAccessControl;
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            systemAccessControl = factory.create(ImmutableMap.copyOf(properties));
        }

        log.info("-- Loaded system access control %s --", name);
        return systemAccessControl;
    }

    @VisibleForTesting
    public void loadSystemAccessControl(String name, Map<String, String> properties)
    {
        requireNonNull(name, "name is null");
        requireNonNull(properties, "properties is null");

        SystemAccessControlFactory factory = systemAccessControlFactories.get(name);
        checkState(factory != null, "Access control '%s' is not registered", name);

        SystemAccessControl systemAccessControl;
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            systemAccessControl = factory.create(ImmutableMap.copyOf(properties));
        }

        systemAccessControl.getEventListeners()
                .forEach(eventListenerManager::addEventListener);

        setSystemAccessControls(ImmutableList.of(systemAccessControl));
    }

    @VisibleForTesting
    public void addSystemAccessControl(SystemAccessControl systemAccessControl)
    {
        systemAccessControls.updateAndGet(currentControls ->
                ImmutableList.<SystemAccessControl>builder()
                        .addAll(currentControls)
                        .add(systemAccessControl)
                        .build());
    }

    @VisibleForTesting
    public void setSystemAccessControls(List<SystemAccessControl> systemAccessControls)
    {
        checkState(this.systemAccessControls.compareAndSet(null, systemAccessControls), "System access control already initialized");
    }

    @Override
    public void checkCanImpersonateUser(Identity identity, String userName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(userName, "userName is null");

        systemAuthorizationCheck(control -> control.checkCanImpersonateUser(new SystemSecurityContext(identity, Optional.empty()), userName));
    }

    @Override
    @Deprecated
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        requireNonNull(principal, "principal is null");
        requireNonNull(userName, "userName is null");

        systemAuthorizationCheck(control -> control.checkCanSetUser(principal, userName));
    }

    @Override
    public void checkCanReadSystemInformation(Identity identity)
    {
        requireNonNull(identity, "identity is null");

        systemAuthorizationCheck(control -> control.checkCanReadSystemInformation(new SystemSecurityContext(identity, Optional.empty())));
    }

    @Override
    public void checkCanWriteSystemInformation(Identity identity)
    {
        requireNonNull(identity, "identity is null");

        systemAuthorizationCheck(control -> control.checkCanWriteSystemInformation(new SystemSecurityContext(identity, Optional.empty())));
    }

    @Override
    public void checkCanExecuteQuery(Identity identity)
    {
        requireNonNull(identity, "identity is null");

        systemAuthorizationCheck(control -> control.checkCanExecuteQuery(new SystemSecurityContext(identity, Optional.empty())));
    }

    @Override
    public void checkCanViewQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        requireNonNull(identity, "identity is null");

        systemAuthorizationCheck(control -> control.checkCanViewQueryOwnedBy(new SystemSecurityContext(identity, Optional.empty()), queryOwner));
    }

    @Override
    public Collection<Identity> filterQueriesOwnedBy(Identity identity, Collection<Identity> queryOwners)
    {
        for (SystemAccessControl systemAccessControl : getSystemAccessControls()) {
            queryOwners = systemAccessControl.filterViewQueryOwnedBy(new SystemSecurityContext(identity, Optional.empty()), queryOwners);
        }
        return queryOwners;
    }

    @Override
    public void checkCanKillQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(queryOwner, "queryOwner is null");

        systemAuthorizationCheck(control -> control.checkCanKillQueryOwnedBy(new SystemSecurityContext(identity, Optional.empty()), queryOwner));
    }

    @Override
    public void checkCanCreateCatalog(SecurityContext securityContext, String catalog)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(catalog, "catalog is null");

        systemAuthorizationCheck(control -> control.checkCanCreateCatalog(securityContext.toSystemSecurityContext(), catalog));
    }

    @Override
    public void checkCanDropCatalog(SecurityContext securityContext, String catalog)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(catalog, "catalog is null");

        systemAuthorizationCheck(control -> control.checkCanDropCatalog(securityContext.toSystemSecurityContext(), catalog));
    }

    @Override
    public Set<String> filterCatalogs(SecurityContext securityContext, Set<String> catalogs)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(catalogs, "catalogs is null");

        for (SystemAccessControl systemAccessControl : getSystemAccessControls()) {
            catalogs = systemAccessControl.filterCatalogs(securityContext.toSystemSecurityContext(), catalogs);
        }
        return catalogs;
    }

    @Override
    public void checkCanCreateSchema(SecurityContext securityContext, CatalogSchemaName schemaName, Map<String, Object> properties)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(schemaName, "schemaName is null");

        checkCanAccessCatalog(securityContext, schemaName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanCreateSchema(securityContext.toSystemSecurityContext(), schemaName, properties));

        catalogAuthorizationCheck(schemaName.getCatalogName(), securityContext, (control, context) -> control.checkCanCreateSchema(context, schemaName.getSchemaName(), properties));
    }

    @Override
    public void checkCanDropSchema(SecurityContext securityContext, CatalogSchemaName schemaName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(schemaName, "schemaName is null");

        checkCanAccessCatalog(securityContext, schemaName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanDropSchema(securityContext.toSystemSecurityContext(), schemaName));

        catalogAuthorizationCheck(schemaName.getCatalogName(), securityContext, (control, context) -> control.checkCanDropSchema(context, schemaName.getSchemaName()));
    }

    @Override
    public void checkCanRenameSchema(SecurityContext securityContext, CatalogSchemaName schemaName, String newSchemaName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(schemaName, "schemaName is null");

        checkCanAccessCatalog(securityContext, schemaName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanRenameSchema(securityContext.toSystemSecurityContext(), schemaName, newSchemaName));

        catalogAuthorizationCheck(schemaName.getCatalogName(), securityContext, (control, context) -> control.checkCanRenameSchema(context, schemaName.getSchemaName(), newSchemaName));
    }

    @Override
    public void checkCanSetSchemaAuthorization(SecurityContext securityContext, CatalogSchemaName schemaName, TrinoPrincipal principal)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(schemaName, "schemaName is null");

        checkCanAccessCatalog(securityContext, schemaName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanSetSchemaAuthorization(securityContext.toSystemSecurityContext(), schemaName, principal));

        catalogAuthorizationCheck(schemaName.getCatalogName(), securityContext, (control, context) -> control.checkCanSetSchemaAuthorization(context, schemaName.getSchemaName(), principal));
    }

    @Override
    public void checkCanShowSchemas(SecurityContext securityContext, String catalogName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(catalogName, "catalogName is null");

        checkCanAccessCatalog(securityContext, catalogName);

        systemAuthorizationCheck(control -> control.checkCanShowSchemas(securityContext.toSystemSecurityContext(), catalogName));

        catalogAuthorizationCheck(catalogName, securityContext, ConnectorAccessControl::checkCanShowSchemas);
    }

    @Override
    public Set<String> filterSchemas(SecurityContext securityContext, String catalogName, Set<String> schemaNames)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(schemaNames, "schemaNames is null");

        if (filterCatalogs(securityContext, ImmutableSet.of(catalogName)).isEmpty()) {
            return ImmutableSet.of();
        }

        for (SystemAccessControl systemAccessControl : getSystemAccessControls()) {
            schemaNames = systemAccessControl.filterSchemas(securityContext.toSystemSecurityContext(), catalogName, schemaNames);
        }

        ConnectorAccessControl connectorAccessControl = getConnectorAccessControl(securityContext.getTransactionId(), catalogName);
        if (connectorAccessControl != null) {
            schemaNames = connectorAccessControl.filterSchemas(toConnectorSecurityContext(catalogName, securityContext), schemaNames);
        }
        return schemaNames;
    }

    @Override
    public void checkCanShowCreateSchema(SecurityContext securityContext, CatalogSchemaName schemaName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(schemaName, "schemaName is null");

        checkCanAccessCatalog(securityContext, schemaName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanShowCreateSchema(securityContext.toSystemSecurityContext(), schemaName));

        catalogAuthorizationCheck(schemaName.getCatalogName(), securityContext, (control, context) -> control.checkCanShowCreateSchema(context, schemaName.getSchemaName()));
    }

    @Override
    public void checkCanShowCreateTable(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanShowCreateTable(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(tableName.getCatalogName(), securityContext, (control, context) -> control.checkCanShowCreateTable(context, tableName.asSchemaTableName()));
    }

    @Override
    public void checkCanCreateTable(SecurityContext securityContext, QualifiedObjectName tableName, Map<String, Object> properties)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanCreateTable(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName(), properties));

        catalogAuthorizationCheck(tableName.getCatalogName(), securityContext, (control, context) -> control.checkCanCreateTable(context, tableName.asSchemaTableName(), properties));
    }

    @Override
    public void checkCanDropTable(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanDropTable(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(tableName.getCatalogName(), securityContext, (control, context) -> control.checkCanDropTable(context, tableName.asSchemaTableName()));
    }

    @Override
    public void checkCanRenameTable(SecurityContext securityContext, QualifiedObjectName tableName, QualifiedObjectName newTableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(newTableName, "newTableName is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanRenameTable(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName(), newTableName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(tableName.getCatalogName(), securityContext, (control, context) -> control.checkCanRenameTable(context, tableName.asSchemaTableName(), newTableName.asSchemaTableName()));
    }

    @Override
    public void checkCanSetTableProperties(SecurityContext securityContext, QualifiedObjectName tableName, Map<String, Optional<Object>> properties)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(properties, "nonNullProperties is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanSetTableProperties(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName(), properties));

        catalogAuthorizationCheck(tableName.getCatalogName(), securityContext, (control, context) -> control.checkCanSetTableProperties(context, tableName.asSchemaTableName(), properties));
    }

    @Override
    public void checkCanSetTableComment(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanSetTableComment(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(tableName.getCatalogName(), securityContext, (control, context) -> control.checkCanSetTableComment(context, tableName.asSchemaTableName()));
    }

    @Override
    public void checkCanSetViewComment(SecurityContext securityContext, QualifiedObjectName viewName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(viewName, "viewName is null");

        checkCanAccessCatalog(securityContext, viewName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanSetViewComment(securityContext.toSystemSecurityContext(), viewName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(viewName.getCatalogName(), securityContext, (control, context) -> control.checkCanSetViewComment(context, viewName.asSchemaTableName()));
    }

    @Override
    public void checkCanSetColumnComment(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanSetColumnComment(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(tableName.getCatalogName(), securityContext, (control, context) -> control.checkCanSetColumnComment(context, tableName.asSchemaTableName()));
    }

    @Override
    public void checkCanShowTables(SecurityContext securityContext, CatalogSchemaName schema)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(schema, "schema is null");

        checkCanAccessCatalog(securityContext, schema.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanShowTables(securityContext.toSystemSecurityContext(), schema));

        catalogAuthorizationCheck(schema.getCatalogName(), securityContext, (control, context) -> control.checkCanShowTables(context, schema.getSchemaName()));
    }

    @Override
    public Set<SchemaTableName> filterTables(SecurityContext securityContext, String catalogName, Set<SchemaTableName> tableNames)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(tableNames, "tableNames is null");

        if (filterCatalogs(securityContext, ImmutableSet.of(catalogName)).isEmpty()) {
            return ImmutableSet.of();
        }

        for (SystemAccessControl systemAccessControl : getSystemAccessControls()) {
            tableNames = systemAccessControl.filterTables(securityContext.toSystemSecurityContext(), catalogName, tableNames);
        }

        ConnectorAccessControl connectorAccessControl = getConnectorAccessControl(securityContext.getTransactionId(), catalogName);
        if (connectorAccessControl != null) {
            tableNames = connectorAccessControl.filterTables(toConnectorSecurityContext(catalogName, securityContext), tableNames);
        }
        return tableNames;
    }

    @Override
    public void checkCanShowColumns(SecurityContext securityContext, CatalogSchemaTableName table)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(table, "table is null");

        checkCanAccessCatalog(securityContext, table.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanShowColumns(securityContext.toSystemSecurityContext(), table));

        catalogAuthorizationCheck(table.getCatalogName(), securityContext, (control, context) -> control.checkCanShowColumns(context, table.getSchemaTableName()));
    }

    @Override
    public Set<String> filterColumns(SecurityContext securityContext, CatalogSchemaTableName table, Set<String> columns)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(table, "tableName is null");

        if (filterTables(securityContext, table.getCatalogName(), ImmutableSet.of(table.getSchemaTableName())).isEmpty()) {
            return ImmutableSet.of();
        }

        for (SystemAccessControl systemAccessControl : getSystemAccessControls()) {
            columns = systemAccessControl.filterColumns(securityContext.toSystemSecurityContext(), table, columns);
        }

        ConnectorAccessControl connectorAccessControl = getConnectorAccessControl(securityContext.getTransactionId(), table.getCatalogName());
        if (connectorAccessControl != null) {
            columns = connectorAccessControl.filterColumns(toConnectorSecurityContext(table.getCatalogName(), securityContext), table.getSchemaTableName(), columns);
        }
        return columns;
    }

    @Override
    public void checkCanAddColumns(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanAddColumn(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(tableName.getCatalogName(), securityContext, (control, context) -> control.checkCanAddColumn(context, tableName.asSchemaTableName()));
    }

    @Override
    public void checkCanAlterColumn(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanAlterColumn(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(tableName.getCatalogName(), securityContext, (control, context) -> control.checkCanAlterColumn(context, tableName.asSchemaTableName()));
    }

    @Override
    public void checkCanDropColumn(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanDropColumn(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(tableName.getCatalogName(), securityContext, (control, context) -> control.checkCanDropColumn(context, tableName.asSchemaTableName()));
    }

    @Override
    public void checkCanRenameColumn(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanRenameColumn(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(tableName.getCatalogName(), securityContext, (control, context) -> control.checkCanRenameColumn(context, tableName.asSchemaTableName()));
    }

    @Override
    public void checkCanSetTableAuthorization(SecurityContext securityContext, QualifiedObjectName tableName, TrinoPrincipal principal)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(principal, "principal is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanSetTableAuthorization(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName(), principal));

        catalogAuthorizationCheck(
                tableName.getCatalogName(),
                securityContext,
                (control, context) -> control.checkCanSetTableAuthorization(context, tableName.asSchemaTableName(), principal));
    }

    @Override
    public void checkCanInsertIntoTable(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanInsertIntoTable(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(tableName.getCatalogName(), securityContext, (control, context) -> control.checkCanInsertIntoTable(context, tableName.asSchemaTableName()));
    }

    @Override
    public void checkCanDeleteFromTable(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanDeleteFromTable(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(tableName.getCatalogName(), securityContext, (control, context) -> control.checkCanDeleteFromTable(context, tableName.asSchemaTableName()));
    }

    @Override
    public void checkCanTruncateTable(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanTruncateTable(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(tableName.getCatalogName(), securityContext, (control, context) -> control.checkCanTruncateTable(context, tableName.asSchemaTableName()));
    }

    @Override
    public void checkCanUpdateTableColumns(SecurityContext securityContext, QualifiedObjectName tableName, Set<String> updatedColumnNames)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanUpdateTableColumns(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName(), updatedColumnNames));

        catalogAuthorizationCheck(tableName.getCatalogName(), securityContext, (control, context) -> control.checkCanUpdateTableColumns(context, tableName.asSchemaTableName(), updatedColumnNames));
    }

    @Override
    public void checkCanCreateView(SecurityContext securityContext, QualifiedObjectName viewName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(viewName, "viewName is null");

        checkCanAccessCatalog(securityContext, viewName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanCreateView(securityContext.toSystemSecurityContext(), viewName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(viewName.getCatalogName(), securityContext, (control, context) -> control.checkCanCreateView(context, viewName.asSchemaTableName()));
    }

    @Override
    public void checkCanRenameView(SecurityContext securityContext, QualifiedObjectName viewName, QualifiedObjectName newViewName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(viewName, "viewName is null");
        requireNonNull(newViewName, "newViewName is null");

        checkCanAccessCatalog(securityContext, viewName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanRenameView(securityContext.toSystemSecurityContext(), viewName.asCatalogSchemaTableName(), newViewName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(viewName.getCatalogName(), securityContext, (control, context) -> control.checkCanRenameView(context, viewName.asSchemaTableName(), newViewName.asSchemaTableName()));
    }

    @Override
    public void checkCanSetViewAuthorization(SecurityContext securityContext, QualifiedObjectName viewName, TrinoPrincipal principal)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(viewName, "viewName is null");
        requireNonNull(principal, "principal is null");

        checkCanAccessCatalog(securityContext, viewName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanSetViewAuthorization(securityContext.toSystemSecurityContext(), viewName.asCatalogSchemaTableName(), principal));

        catalogAuthorizationCheck(
                viewName.getCatalogName(),
                securityContext,
                (control, context) -> control.checkCanSetViewAuthorization(context, viewName.asSchemaTableName(), principal));
    }

    @Override
    public void checkCanDropView(SecurityContext securityContext, QualifiedObjectName viewName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(viewName, "viewName is null");

        checkCanAccessCatalog(securityContext, viewName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanDropView(securityContext.toSystemSecurityContext(), viewName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(viewName.getCatalogName(), securityContext, (control, context) -> control.checkCanDropView(context, viewName.asSchemaTableName()));
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SecurityContext securityContext, QualifiedObjectName tableName, Set<String> columnNames)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanCreateViewWithSelectFromColumns(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName(), columnNames));

        catalogAuthorizationCheck(tableName.getCatalogName(), securityContext, (control, context) -> control.checkCanCreateViewWithSelectFromColumns(context, tableName.asSchemaTableName(), columnNames));
    }

    @Override
    public void checkCanCreateMaterializedView(SecurityContext securityContext, QualifiedObjectName materializedViewName, Map<String, Object> properties)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(materializedViewName, "materializedViewName is null");
        requireNonNull(properties, "properties is null");

        checkCanAccessCatalog(securityContext, materializedViewName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanCreateMaterializedView(securityContext.toSystemSecurityContext(), materializedViewName.asCatalogSchemaTableName(), properties));

        catalogAuthorizationCheck(materializedViewName.getCatalogName(), securityContext, (control, context) -> control.checkCanCreateMaterializedView(context, materializedViewName.asSchemaTableName(), properties));
    }

    @Override
    public void checkCanRefreshMaterializedView(SecurityContext securityContext, QualifiedObjectName materializedViewName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(materializedViewName, "materializedViewName is null");

        checkCanAccessCatalog(securityContext, materializedViewName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanRefreshMaterializedView(securityContext.toSystemSecurityContext(), materializedViewName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(materializedViewName.getCatalogName(), securityContext, (control, context) -> control.checkCanRefreshMaterializedView(context, materializedViewName.asSchemaTableName()));
    }

    @Override
    public void checkCanDropMaterializedView(SecurityContext securityContext, QualifiedObjectName materializedViewName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(materializedViewName, "materializedViewName is null");

        checkCanAccessCatalog(securityContext, materializedViewName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanDropMaterializedView(securityContext.toSystemSecurityContext(), materializedViewName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(materializedViewName.getCatalogName(), securityContext, (control, context) -> control.checkCanDropMaterializedView(context, materializedViewName.asSchemaTableName()));
    }

    @Override
    public void checkCanRenameMaterializedView(SecurityContext securityContext, QualifiedObjectName viewName, QualifiedObjectName newViewName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(viewName, "viewName is null");
        requireNonNull(newViewName, "newViewName is null");

        checkCanAccessCatalog(securityContext, viewName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanRenameMaterializedView(securityContext.toSystemSecurityContext(), viewName.asCatalogSchemaTableName(), newViewName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(viewName.getCatalogName(), securityContext, (control, context) -> control.checkCanRenameMaterializedView(context, viewName.asSchemaTableName(), newViewName.asSchemaTableName()));
    }

    @Override
    public void checkCanSetMaterializedViewProperties(SecurityContext securityContext, QualifiedObjectName materializedViewName, Map<String, Optional<Object>> properties)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(materializedViewName, "materializedViewName is null");
        requireNonNull(properties, "nonNullProperties is null");

        checkCanAccessCatalog(securityContext, materializedViewName.getCatalogName());

        systemAuthorizationCheck(control ->
                control.checkCanSetMaterializedViewProperties(
                        securityContext.toSystemSecurityContext(),
                        materializedViewName.asCatalogSchemaTableName(),
                        properties));

        catalogAuthorizationCheck(
                materializedViewName.getCatalogName(),
                securityContext,
                (control, context) -> control.checkCanSetMaterializedViewProperties(context, materializedViewName.asSchemaTableName(), properties));
    }

    @Override
    public void checkCanGrantExecuteFunctionPrivilege(SecurityContext securityContext, String functionName, Identity grantee, boolean grantOption)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(functionName, "functionName is null");

        systemAuthorizationCheck(control -> control.checkCanGrantExecuteFunctionPrivilege(
                securityContext.toSystemSecurityContext(),
                functionName,
                new TrinoPrincipal(PrincipalType.USER, grantee.getUser()),
                grantOption));
    }

    @Override
    public void checkCanGrantExecuteFunctionPrivilege(SecurityContext securityContext, FunctionKind functionKind, QualifiedObjectName functionName, Identity grantee, boolean grantOption)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(functionKind, "functionKind is null");
        requireNonNull(functionName, "functionName is null");

        systemAuthorizationCheck(control -> control.checkCanGrantExecuteFunctionPrivilege(
                securityContext.toSystemSecurityContext(),
                functionKind,
                functionName.asCatalogSchemaRoutineName(),
                new TrinoPrincipal(PrincipalType.USER, grantee.getUser()),
                grantOption));

        catalogAuthorizationCheck(
                functionName.getCatalogName(),
                securityContext,
                (control, context) -> control.checkCanGrantExecuteFunctionPrivilege(
                        context,
                        functionKind,
                        functionName.asSchemaRoutineName(),
                        new TrinoPrincipal(PrincipalType.USER, grantee.getUser()),
                        grantOption));
    }

    @Override
    public void checkCanGrantSchemaPrivilege(SecurityContext securityContext, Privilege privilege, CatalogSchemaName schemaName, TrinoPrincipal grantee, boolean grantOption)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(privilege, "privilege is null");

        checkCanAccessCatalog(securityContext, schemaName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanGrantSchemaPrivilege(securityContext.toSystemSecurityContext(), privilege, schemaName, grantee, grantOption));

        catalogAuthorizationCheck(
                schemaName.getCatalogName(),
                securityContext,
                (control, context) -> control.checkCanGrantSchemaPrivilege(context, privilege, schemaName.getSchemaName(), grantee, grantOption));
    }

    @Override
    public void checkCanDenySchemaPrivilege(SecurityContext securityContext, Privilege privilege, CatalogSchemaName schemaName, TrinoPrincipal grantee)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(privilege, "privilege is null");

        checkCanAccessCatalog(securityContext, schemaName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanDenySchemaPrivilege(securityContext.toSystemSecurityContext(), privilege, schemaName, grantee));

        catalogAuthorizationCheck(
                schemaName.getCatalogName(),
                securityContext,
                (control, context) -> control.checkCanDenySchemaPrivilege(context, privilege, schemaName.getSchemaName(), grantee));
    }

    @Override
    public void checkCanRevokeSchemaPrivilege(SecurityContext securityContext, Privilege privilege, CatalogSchemaName schemaName, TrinoPrincipal revokee, boolean grantOption)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(privilege, "privilege is null");

        checkCanAccessCatalog(securityContext, schemaName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanRevokeSchemaPrivilege(securityContext.toSystemSecurityContext(), privilege, schemaName, revokee, grantOption));

        catalogAuthorizationCheck(
                schemaName.getCatalogName(),
                securityContext,
                (control, context) -> control.checkCanRevokeSchemaPrivilege(context, privilege, schemaName.getSchemaName(), revokee, grantOption));
    }

    @Override
    public void checkCanGrantTablePrivilege(SecurityContext securityContext, Privilege privilege, QualifiedObjectName tableName, TrinoPrincipal grantee, boolean grantOption)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(privilege, "privilege is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanGrantTablePrivilege(securityContext.toSystemSecurityContext(), privilege, tableName.asCatalogSchemaTableName(), grantee, grantOption));

        catalogAuthorizationCheck(tableName.getCatalogName(), securityContext, (control, context) -> control.checkCanGrantTablePrivilege(context, privilege, tableName.asSchemaTableName(), grantee, grantOption));
    }

    @Override
    public void checkCanDenyTablePrivilege(SecurityContext securityContext, Privilege privilege, QualifiedObjectName tableName, TrinoPrincipal grantee)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(privilege, "privilege is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanDenyTablePrivilege(securityContext.toSystemSecurityContext(), privilege, tableName.asCatalogSchemaTableName(), grantee));

        catalogAuthorizationCheck(tableName.getCatalogName(), securityContext, (control, context) -> control.checkCanDenyTablePrivilege(context, privilege, tableName.asSchemaTableName(), grantee));
    }

    @Override
    public void checkCanRevokeTablePrivilege(SecurityContext securityContext, Privilege privilege, QualifiedObjectName tableName, TrinoPrincipal revokee, boolean grantOption)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(privilege, "privilege is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanRevokeTablePrivilege(securityContext.toSystemSecurityContext(), privilege, tableName.asCatalogSchemaTableName(), revokee, grantOption));

        catalogAuthorizationCheck(tableName.getCatalogName(), securityContext, (control, context) -> control.checkCanRevokeTablePrivilege(context, privilege, tableName.asSchemaTableName(), revokee, grantOption));
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(propertyName, "propertyName is null");

        systemAuthorizationCheck(control -> control.checkCanSetSystemSessionProperty(new SystemSecurityContext(identity, Optional.empty()), propertyName));
    }

    @Override
    public void checkCanSetCatalogSessionProperty(SecurityContext securityContext, String catalogName, String propertyName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(propertyName, "propertyName is null");

        checkCanAccessCatalog(securityContext, catalogName);

        systemAuthorizationCheck(control -> control.checkCanSetCatalogSessionProperty(securityContext.toSystemSecurityContext(), catalogName, propertyName));

        catalogAuthorizationCheck(catalogName, securityContext, (control, context) -> control.checkCanSetCatalogSessionProperty(context, propertyName));
    }

    @Override
    public void checkCanSelectFromColumns(SecurityContext securityContext, QualifiedObjectName tableName, Set<String> columnNames)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(columnNames, "columnNames is null");

        checkCanAccessCatalog(securityContext, tableName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanSelectFromColumns(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName(), columnNames));

        catalogAuthorizationCheck(tableName.getCatalogName(), securityContext, (control, context) -> control.checkCanSelectFromColumns(context, tableName.asSchemaTableName(), columnNames));
    }

    @Override
    public void checkCanCreateRole(SecurityContext securityContext, String role, Optional<TrinoPrincipal> grantor, Optional<String> catalogName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(role, "role is null");
        requireNonNull(grantor, "grantor is null");
        requireNonNull(catalogName, "catalogName is null");

        if (catalogName.isPresent()) {
            checkCanAccessCatalog(securityContext, catalogName.get());
            checkCatalogRoles(securityContext, catalogName.get());
            catalogAuthorizationCheck(catalogName.get(), securityContext, (control, context) -> control.checkCanCreateRole(context, role, grantor));
        }
        else {
            systemAuthorizationCheck(control -> control.checkCanCreateRole(securityContext.toSystemSecurityContext(), role, grantor));
        }
    }

    @Override
    public void checkCanDropRole(SecurityContext securityContext, String role, Optional<String> catalogName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(role, "role is null");
        requireNonNull(catalogName, "catalogName is null");

        if (catalogName.isPresent()) {
            checkCanAccessCatalog(securityContext, catalogName.get());
            checkCatalogRoles(securityContext, catalogName.get());
            catalogAuthorizationCheck(catalogName.get(), securityContext, (control, context) -> control.checkCanDropRole(context, role));
        }
        else {
            systemAuthorizationCheck(control -> control.checkCanDropRole(securityContext.toSystemSecurityContext(), role));
        }
    }

    @Override
    public void checkCanGrantRoles(SecurityContext securityContext, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, Optional<String> catalogName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(roles, "roles is null");
        requireNonNull(grantees, "grantees is null");
        requireNonNull(grantor, "grantor is null");
        requireNonNull(catalogName, "catalogName is null");

        if (catalogName.isPresent()) {
            checkCanAccessCatalog(securityContext, catalogName.get());
            checkCatalogRoles(securityContext, catalogName.get());
            catalogAuthorizationCheck(catalogName.get(), securityContext, (control, context) -> control.checkCanGrantRoles(context, roles, grantees, adminOption, grantor));
        }
        else {
            systemAuthorizationCheck(control -> control.checkCanGrantRoles(securityContext.toSystemSecurityContext(), roles, grantees, adminOption, grantor));
        }
    }

    @Override
    public void checkCanRevokeRoles(SecurityContext securityContext, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor, Optional<String> catalogName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(roles, "roles is null");
        requireNonNull(grantees, "grantees is null");
        requireNonNull(grantor, "grantor is null");
        requireNonNull(catalogName, "catalogName is null");

        if (catalogName.isPresent()) {
            checkCanAccessCatalog(securityContext, catalogName.get());
            checkCatalogRoles(securityContext, catalogName.get());
            catalogAuthorizationCheck(catalogName.get(), securityContext, (control, context) -> control.checkCanRevokeRoles(context, roles, grantees, adminOption, grantor));
        }
        else {
            systemAuthorizationCheck(control -> control.checkCanRevokeRoles(securityContext.toSystemSecurityContext(), roles, grantees, adminOption, grantor));
        }
    }

    @Override
    public void checkCanSetCatalogRole(SecurityContext securityContext, String role, String catalogName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(role, "role is null");
        requireNonNull(catalogName, "catalogName is null");

        checkCanAccessCatalog(securityContext, catalogName);
        catalogAuthorizationCheck(catalogName, securityContext, (control, context) -> control.checkCanSetRole(context, role));
    }

    @Override
    public void checkCanShowRoleAuthorizationDescriptors(SecurityContext securityContext, Optional<String> catalogName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(catalogName, "catalogName is null");

        if (catalogName.isPresent()) {
            checkCanAccessCatalog(securityContext, catalogName.get());
            checkCatalogRoles(securityContext, catalogName.get());
            catalogAuthorizationCheck(catalogName.get(), securityContext, ConnectorAccessControl::checkCanShowRoleAuthorizationDescriptors);
        }
        else {
            systemAuthorizationCheck(control -> control.checkCanShowRoleAuthorizationDescriptors(securityContext.toSystemSecurityContext()));
        }
    }

    @Override
    public void checkCanShowRoles(SecurityContext securityContext, Optional<String> catalogName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(catalogName, "catalogName is null");

        if (catalogName.isPresent()) {
            checkCanAccessCatalog(securityContext, catalogName.get());
            checkCatalogRoles(securityContext, catalogName.get());
            catalogAuthorizationCheck(catalogName.get(), securityContext, ConnectorAccessControl::checkCanShowRoles);
        }
        else {
            systemAuthorizationCheck(control -> control.checkCanShowRoles(securityContext.toSystemSecurityContext()));
        }
    }

    @Override
    public void checkCanShowCurrentRoles(SecurityContext securityContext, Optional<String> catalogName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(catalogName, "catalogName is null");

        if (catalogName.isPresent()) {
            checkCanAccessCatalog(securityContext, catalogName.get());
            checkCatalogRoles(securityContext, catalogName.get());
            catalogAuthorizationCheck(catalogName.get(), securityContext, ConnectorAccessControl::checkCanShowCurrentRoles);
        }
        else {
            systemAuthorizationCheck(control -> control.checkCanShowCurrentRoles(securityContext.toSystemSecurityContext()));
        }
    }

    @Override
    public void checkCanShowRoleGrants(SecurityContext securityContext, Optional<String> catalogName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(catalogName, "catalogName is null");

        if (catalogName.isPresent()) {
            checkCanAccessCatalog(securityContext, catalogName.get());
            checkCatalogRoles(securityContext, catalogName.get());
            catalogAuthorizationCheck(catalogName.get(), securityContext, ConnectorAccessControl::checkCanShowRoleGrants);
        }
        else {
            systemAuthorizationCheck(control -> control.checkCanShowRoleGrants(securityContext.toSystemSecurityContext()));
        }
    }

    @Override
    public void checkCanExecuteProcedure(SecurityContext securityContext, QualifiedObjectName procedureName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(procedureName, "procedureName is null");

        checkCanAccessCatalog(securityContext, procedureName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanExecuteProcedure(securityContext.toSystemSecurityContext(), procedureName.asCatalogSchemaRoutineName()));

        catalogAuthorizationCheck(
                procedureName.getCatalogName(),
                securityContext,
                (control, context) -> control.checkCanExecuteProcedure(context, procedureName.asSchemaRoutineName()));
    }

    @Override
    public void checkCanExecuteFunction(SecurityContext context, String functionName)
    {
        requireNonNull(context, "context is null");
        requireNonNull(functionName, "functionName is null");

        systemAuthorizationCheck(control -> control.checkCanExecuteFunction(context.toSystemSecurityContext(), functionName));
    }

    @Override
    public void checkCanExecuteFunction(SecurityContext securityContext, FunctionKind functionKind, QualifiedObjectName functionName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(functionKind, "functionKind is null");
        requireNonNull(functionName, "functionName is null");

        checkCanAccessCatalog(securityContext, functionName.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanExecuteFunction(
                securityContext.toSystemSecurityContext(),
                functionKind,
                functionName.asCatalogSchemaRoutineName()));

        catalogAuthorizationCheck(
                functionName.getCatalogName(),
                securityContext,
                (control, context) -> control.checkCanExecuteFunction(context, functionKind, functionName.asSchemaRoutineName()));
    }

    @Override
    public void checkCanExecuteTableProcedure(SecurityContext securityContext, QualifiedObjectName tableName, String procedureName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(procedureName, "procedureName is null");
        requireNonNull(tableName, "tableName is null");

        systemAuthorizationCheck(control -> control.checkCanExecuteTableProcedure(
                securityContext.toSystemSecurityContext(),
                tableName.asCatalogSchemaTableName(),
                procedureName));

        catalogAuthorizationCheck(
                tableName.getCatalogName(),
                securityContext,
                (control, context) -> control.checkCanExecuteTableProcedure(
                        context,
                        tableName.asSchemaTableName(),
                        procedureName));
    }

    @Override
    public List<ViewExpression> getRowFilters(SecurityContext context, QualifiedObjectName tableName)
    {
        requireNonNull(context, "context is null");
        requireNonNull(tableName, "tableName is null");

        ImmutableList.Builder<ViewExpression> filters = ImmutableList.builder();
        ConnectorAccessControl connectorAccessControl = getConnectorAccessControl(context.getTransactionId(), tableName.getCatalogName());

        if (connectorAccessControl != null) {
            connectorAccessControl.getRowFilters(toConnectorSecurityContext(tableName.getCatalogName(), context), tableName.asSchemaTableName())
                    .forEach(filters::add);
        }

        for (SystemAccessControl systemAccessControl : getSystemAccessControls()) {
            systemAccessControl.getRowFilters(context.toSystemSecurityContext(), tableName.asCatalogSchemaTableName())
                    .forEach(filters::add);
        }

        return filters.build();
    }

    @Override
    public Optional<ViewExpression> getColumnMask(SecurityContext context, QualifiedObjectName tableName, String columnName, Type type)
    {
        requireNonNull(context, "context is null");
        requireNonNull(tableName, "tableName is null");

        ImmutableList.Builder<ViewExpression> masks = ImmutableList.builder();

        ConnectorAccessControl connectorAccessControl = getConnectorAccessControl(context.getTransactionId(), tableName.getCatalogName());
        if (connectorAccessControl != null) {
            connectorAccessControl.getColumnMask(toConnectorSecurityContext(tableName.getCatalogName(), context), tableName.asSchemaTableName(), columnName, type)
                    .ifPresent(masks::add);
        }

        for (SystemAccessControl systemAccessControl : getSystemAccessControls()) {
            systemAccessControl.getColumnMask(context.toSystemSecurityContext(), tableName.asCatalogSchemaTableName(), columnName, type)
                    .ifPresent(masks::add);
        }

        List<ViewExpression> allMasks = masks.build();
        if (allMasks.size() > 1) {
            throw new TrinoException(INVALID_COLUMN_MASK, format("Column must have a single mask: %s", columnName));
        }

        return allMasks.stream().findFirst();
    }

    private ConnectorAccessControl getConnectorAccessControl(TransactionId transactionId, String catalogName)
    {
        CatalogServiceProvider<Optional<ConnectorAccessControl>> connectorAccessControlProvider = this.connectorAccessControlProvider.get();
        if (connectorAccessControlProvider == null) {
            return null;
        }

        return transactionManager.getCatalogHandle(transactionId, catalogName)
                .flatMap(connectorAccessControlProvider::getService)
                .orElse(null);
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

    private void checkCanAccessCatalog(SecurityContext securityContext, String catalogName)
    {
        try {
            for (SystemAccessControl systemAccessControl : getSystemAccessControls()) {
                systemAccessControl.checkCanAccessCatalog(securityContext.toSystemSecurityContext(), catalogName);
            }
            authorizationSuccess.update(1);
        }
        catch (TrinoException e) {
            authorizationFail.update(1);
            throw e;
        }
    }

    private void systemAuthorizationCheck(Consumer<SystemAccessControl> check)
    {
        try {
            for (SystemAccessControl systemAccessControl : getSystemAccessControls()) {
                check.accept(systemAccessControl);
            }
            authorizationSuccess.update(1);
        }
        catch (TrinoException e) {
            authorizationFail.update(1);
            throw e;
        }
    }

    private void catalogAuthorizationCheck(String catalogName, SecurityContext securityContext, BiConsumer<ConnectorAccessControl, ConnectorSecurityContext> check)
    {
        ConnectorAccessControl connectorAccessControl = getConnectorAccessControl(securityContext.getTransactionId(), catalogName);
        if (connectorAccessControl == null) {
            return;
        }

        try {
            check.accept(connectorAccessControl, toConnectorSecurityContext(catalogName, securityContext));
            authorizationSuccess.update(1);
        }
        catch (TrinoException e) {
            authorizationFail.update(1);
            throw e;
        }
    }

    private void checkCatalogRoles(SecurityContext securityContext, String catalogName)
    {
        ConnectorAccessControl connectorAccessControl = getConnectorAccessControl(securityContext.getTransactionId(), catalogName);
        if (connectorAccessControl == null) {
            throw new TrinoException(NOT_SUPPORTED, format("Catalog %s does not support catalog roles", catalogName));
        }
    }

    private List<SystemAccessControl> getSystemAccessControls()
    {
        List<SystemAccessControl> accessControls = systemAccessControls.get();
        if (accessControls != null) {
            return accessControls;
        }
        return ImmutableList.of(new InitializingSystemAccessControl());
    }

    private ConnectorSecurityContext toConnectorSecurityContext(String catalogName, SecurityContext securityContext)
    {
        return toConnectorSecurityContext(catalogName, securityContext.getTransactionId(), securityContext.getIdentity(), securityContext.getQueryId());
    }

    private ConnectorSecurityContext toConnectorSecurityContext(String catalogName, TransactionId requiredTransactionId, Identity identity, QueryId queryId)
    {
        return new ConnectorSecurityContext(
                transactionManager.getRequiredCatalogMetadata(requiredTransactionId, catalogName).getTransactionHandleFor(CatalogHandleType.NORMAL),
                identity.toConnectorIdentity(catalogName),
                queryId);
    }

    private static class InitializingSystemAccessControl
            extends ForwardingSystemAccessControl
    {
        @Override
        protected SystemAccessControl delegate()
        {
            throw new TrinoException(SERVER_STARTING_UP, "Trino server is still initializing");
        }
    }
}
