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
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogServiceProvider;
import io.trino.eventlistener.EventListenerManager;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.plugin.base.security.DefaultSystemAccessControl;
import io.trino.plugin.base.security.FileBasedSystemAccessControl;
import io.trino.plugin.base.security.ForwardingSystemAccessControl;
import io.trino.plugin.base.security.ReadOnlySystemAccessControl;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.CatalogHandle.CatalogHandleType;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSecurityContext;
import io.trino.spi.connector.EntityKindAndName;
import io.trino.spi.connector.EntityPrivilege;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.FunctionKind;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemAccessControlFactory;
import io.trino.spi.security.SystemAccessControlFactory.SystemAccessControlContext;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_MASK;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SERVER_STARTING_UP;
import static io.trino.spi.security.AccessDeniedException.denyCatalogAccess;
import static io.trino.spi.security.AccessDeniedException.denySetEntityAuthorization;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AccessControlManager
        implements AccessControl
{
    private static final Logger log = Logger.get(AccessControlManager.class);

    private static final File CONFIG_FILE = new File("etc/access-control.properties");
    private static final String NAME_PROPERTY = "access-control.name";

    private final NodeVersion nodeVersion;
    private final TransactionManager transactionManager;
    private final EventListenerManager eventListenerManager;
    private final List<File> configFiles;
    private final OpenTelemetry openTelemetry;
    private final String defaultAccessControlName;
    private final Map<String, SystemAccessControlFactory> systemAccessControlFactories = new ConcurrentHashMap<>();
    private final AtomicReference<CatalogServiceProvider<Optional<ConnectorAccessControl>>> connectorAccessControlProvider = new AtomicReference<>();

    private final AtomicReference<List<SystemAccessControl>> systemAccessControls = new AtomicReference<>();

    private final CounterStat authorizationSuccess = new CounterStat();
    private final CounterStat authorizationFail = new CounterStat();
    private final SecretsResolver secretsResolver;

    @Inject
    public AccessControlManager(
            NodeVersion nodeVersion,
            TransactionManager transactionManager,
            EventListenerManager eventListenerManager,
            AccessControlConfig config,
            OpenTelemetry openTelemetry,
            SecretsResolver secretsResolver,
            @DefaultSystemAccessControlName String defaultAccessControlName)
    {
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.eventListenerManager = requireNonNull(eventListenerManager, "eventListenerManager is null");
        this.configFiles = ImmutableList.copyOf(config.getAccessControlFiles());
        this.openTelemetry = requireNonNull(openTelemetry, "openTelemetry is null");
        this.secretsResolver = requireNonNull(secretsResolver, "secretsResolver is null");
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

    @PreDestroy
    public void destroy()
    {
        try (AutoCloseableCloser closer = AutoCloseableCloser.create()) {
            for (SystemAccessControl systemAccessControl : systemAccessControls.get()) {
                closer.register(systemAccessControl::shutdown);
            }
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
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
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            systemAccessControl = factory.create(ImmutableMap.copyOf(secretsResolver.getResolvedConfiguration(properties)), createContext(name));
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
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            systemAccessControl = factory.create(ImmutableMap.copyOf(secretsResolver.getResolvedConfiguration(properties)), createContext(name));
        }

        systemAccessControl.getEventListeners()
                .forEach(eventListenerManager::addEventListener);

        setSystemAccessControls(ImmutableList.of(systemAccessControl));
    }

    private SystemAccessControlContext createContext(String systemAccessControlName)
    {
        return new SystemAccessControlContext()
        {
            private final Tracer tracer = openTelemetry.getTracer("trino.system-access-control." + systemAccessControlName);
            private final String version = nodeVersion.getVersion();

            @Override
            public String getVersion()
            {
                return version;
            }

            @Override
            public OpenTelemetry getOpenTelemetry()
            {
                return openTelemetry;
            }

            @Override
            public Tracer getTracer()
            {
                return tracer;
            }
        };
    }

    public void setSystemAccessControls(List<SystemAccessControl> systemAccessControls)
    {
        systemAccessControls.forEach(AccessControlManager::verifySystemAccessControl);
        checkState(this.systemAccessControls.compareAndSet(null, systemAccessControls), "System access control already initialized");
    }

    @Override
    public void checkCanImpersonateUser(Identity identity, String userName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(userName, "userName is null");

        systemAuthorizationCheck(control -> control.checkCanImpersonateUser(identity, userName));
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

        systemAuthorizationCheck(control -> control.checkCanReadSystemInformation(identity));
    }

    @Override
    public void checkCanWriteSystemInformation(Identity identity)
    {
        requireNonNull(identity, "identity is null");

        systemAuthorizationCheck(control -> control.checkCanWriteSystemInformation(identity));
    }

    @Override
    public void checkCanExecuteQuery(Identity identity, QueryId queryId)
    {
        requireNonNull(identity, "identity is null");

        systemAuthorizationCheck(control -> control.checkCanExecuteQuery(identity, queryId));
    }

    @Override
    public void checkCanViewQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        requireNonNull(identity, "identity is null");

        systemAuthorizationCheck(control -> control.checkCanViewQueryOwnedBy(identity, queryOwner));
    }

    @Override
    public Collection<Identity> filterQueriesOwnedBy(Identity identity, Collection<Identity> queryOwners)
    {
        if (queryOwners.isEmpty()) {
            // Do not call plugin-provided implementation unnecessarily.
            return ImmutableSet.of();
        }
        for (SystemAccessControl systemAccessControl : getSystemAccessControls()) {
            queryOwners = systemAccessControl.filterViewQueryOwnedBy(identity, queryOwners);
        }
        return queryOwners;
    }

    @Override
    public void checkCanKillQueryOwnedBy(Identity identity, Identity queryOwner)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(queryOwner, "queryOwner is null");

        systemAuthorizationCheck(control -> control.checkCanKillQueryOwnedBy(identity, queryOwner));
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

        if (catalogs.isEmpty()) {
            // Do not call plugin-provided implementation unnecessarily.
            return ImmutableSet.of();
        }

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

        if (schemaNames.isEmpty()) {
            // Do not call plugin-provided implementation unnecessarily.
            return ImmutableSet.of();
        }

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

        checkCanAccessCatalog(securityContext, tableName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanShowCreateTable(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(tableName.catalogName(), securityContext, (control, context) -> control.checkCanShowCreateTable(context, tableName.asSchemaTableName()));
    }

    @Override
    public void checkCanCreateTable(SecurityContext securityContext, QualifiedObjectName tableName, Map<String, Object> properties)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanCreateTable(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName(), properties));

        catalogAuthorizationCheck(tableName.catalogName(), securityContext, (control, context) -> control.checkCanCreateTable(context, tableName.asSchemaTableName(), properties));
    }

    @Override
    public void checkCanDropTable(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanDropTable(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(tableName.catalogName(), securityContext, (control, context) -> control.checkCanDropTable(context, tableName.asSchemaTableName()));
    }

    @Override
    public void checkCanRenameTable(SecurityContext securityContext, QualifiedObjectName tableName, QualifiedObjectName newTableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(newTableName, "newTableName is null");

        checkCanAccessCatalog(securityContext, tableName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanRenameTable(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName(), newTableName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(tableName.catalogName(), securityContext, (control, context) -> control.checkCanRenameTable(context, tableName.asSchemaTableName(), newTableName.asSchemaTableName()));
    }

    @Override
    public void checkCanSetTableProperties(SecurityContext securityContext, QualifiedObjectName tableName, Map<String, Optional<Object>> properties)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(properties, "nonNullProperties is null");

        checkCanAccessCatalog(securityContext, tableName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanSetTableProperties(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName(), properties));

        catalogAuthorizationCheck(tableName.catalogName(), securityContext, (control, context) -> control.checkCanSetTableProperties(context, tableName.asSchemaTableName(), properties));
    }

    @Override
    public void checkCanSetTableComment(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanSetTableComment(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(tableName.catalogName(), securityContext, (control, context) -> control.checkCanSetTableComment(context, tableName.asSchemaTableName()));
    }

    @Override
    public void checkCanSetViewComment(SecurityContext securityContext, QualifiedObjectName viewName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(viewName, "viewName is null");

        checkCanAccessCatalog(securityContext, viewName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanSetViewComment(securityContext.toSystemSecurityContext(), viewName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(viewName.catalogName(), securityContext, (control, context) -> control.checkCanSetViewComment(context, viewName.asSchemaTableName()));
    }

    @Override
    public void checkCanSetColumnComment(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanSetColumnComment(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(tableName.catalogName(), securityContext, (control, context) -> control.checkCanSetColumnComment(context, tableName.asSchemaTableName()));
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

        if (tableNames.isEmpty()) {
            // Do not call plugin-provided implementation unnecessarily.
            return ImmutableSet.of();
        }

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
    public Map<SchemaTableName, Set<String>> filterColumns(SecurityContext securityContext, String catalogName, Map<SchemaTableName, Set<String>> tableColumns)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(tableColumns, "tableColumns is null");

        Set<SchemaTableName> filteredTables = filterTables(securityContext, catalogName, tableColumns.keySet());
        if (!filteredTables.equals(tableColumns.keySet())) {
            tableColumns = Maps.filterKeys(tableColumns, filteredTables::contains);
        }

        if (tableColumns.isEmpty()) {
            // Do not call plugin-provided implementation unnecessarily.
            return ImmutableMap.of();
        }

        for (SystemAccessControl systemAccessControl : getSystemAccessControls()) {
            tableColumns = systemAccessControl.filterColumns(securityContext.toSystemSecurityContext(), catalogName, tableColumns);
        }

        ConnectorAccessControl connectorAccessControl = getConnectorAccessControl(securityContext.getTransactionId(), catalogName);
        if (connectorAccessControl != null) {
            tableColumns = connectorAccessControl.filterColumns(toConnectorSecurityContext(catalogName, securityContext), tableColumns);
        }
        return tableColumns;
    }

    @Override
    public void checkCanAddColumns(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanAddColumn(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(tableName.catalogName(), securityContext, (control, context) -> control.checkCanAddColumn(context, tableName.asSchemaTableName()));
    }

    @Override
    public void checkCanAlterColumn(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanAlterColumn(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(tableName.catalogName(), securityContext, (control, context) -> control.checkCanAlterColumn(context, tableName.asSchemaTableName()));
    }

    @Override
    public void checkCanDropColumn(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanDropColumn(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(tableName.catalogName(), securityContext, (control, context) -> control.checkCanDropColumn(context, tableName.asSchemaTableName()));
    }

    @Override
    public void checkCanRenameColumn(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanRenameColumn(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(tableName.catalogName(), securityContext, (control, context) -> control.checkCanRenameColumn(context, tableName.asSchemaTableName()));
    }

    @Override
    public void checkCanInsertIntoTable(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanInsertIntoTable(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(tableName.catalogName(), securityContext, (control, context) -> control.checkCanInsertIntoTable(context, tableName.asSchemaTableName()));
    }

    @Override
    public void checkCanDeleteFromTable(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanDeleteFromTable(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(tableName.catalogName(), securityContext, (control, context) -> control.checkCanDeleteFromTable(context, tableName.asSchemaTableName()));
    }

    @Override
    public void checkCanTruncateTable(SecurityContext securityContext, QualifiedObjectName tableName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanTruncateTable(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(tableName.catalogName(), securityContext, (control, context) -> control.checkCanTruncateTable(context, tableName.asSchemaTableName()));
    }

    @Override
    public void checkCanUpdateTableColumns(SecurityContext securityContext, QualifiedObjectName tableName, Set<String> updatedColumnNames)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanUpdateTableColumns(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName(), updatedColumnNames));

        catalogAuthorizationCheck(tableName.catalogName(), securityContext, (control, context) -> control.checkCanUpdateTableColumns(context, tableName.asSchemaTableName(), updatedColumnNames));
    }

    @Override
    public void checkCanCreateView(SecurityContext securityContext, QualifiedObjectName viewName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(viewName, "viewName is null");

        checkCanAccessCatalog(securityContext, viewName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanCreateView(securityContext.toSystemSecurityContext(), viewName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(viewName.catalogName(), securityContext, (control, context) -> control.checkCanCreateView(context, viewName.asSchemaTableName()));
    }

    @Override
    public void checkCanRenameView(SecurityContext securityContext, QualifiedObjectName viewName, QualifiedObjectName newViewName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(viewName, "viewName is null");
        requireNonNull(newViewName, "newViewName is null");

        checkCanAccessCatalog(securityContext, viewName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanRenameView(securityContext.toSystemSecurityContext(), viewName.asCatalogSchemaTableName(), newViewName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(viewName.catalogName(), securityContext, (control, context) -> control.checkCanRenameView(context, viewName.asSchemaTableName(), newViewName.asSchemaTableName()));
    }

    @Override
    public void checkCanDropView(SecurityContext securityContext, QualifiedObjectName viewName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(viewName, "viewName is null");

        checkCanAccessCatalog(securityContext, viewName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanDropView(securityContext.toSystemSecurityContext(), viewName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(viewName.catalogName(), securityContext, (control, context) -> control.checkCanDropView(context, viewName.asSchemaTableName()));
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SecurityContext securityContext, QualifiedObjectName tableName, Set<String> columnNames)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");

        checkCanAccessCatalog(securityContext, tableName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanCreateViewWithSelectFromColumns(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName(), columnNames));

        catalogAuthorizationCheck(tableName.catalogName(), securityContext, (control, context) -> control.checkCanCreateViewWithSelectFromColumns(context, tableName.asSchemaTableName(), columnNames));
    }

    @Override
    public void checkCanCreateMaterializedView(SecurityContext securityContext, QualifiedObjectName materializedViewName, Map<String, Object> properties)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(materializedViewName, "materializedViewName is null");
        requireNonNull(properties, "properties is null");

        checkCanAccessCatalog(securityContext, materializedViewName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanCreateMaterializedView(securityContext.toSystemSecurityContext(), materializedViewName.asCatalogSchemaTableName(), properties));

        catalogAuthorizationCheck(materializedViewName.catalogName(), securityContext, (control, context) -> control.checkCanCreateMaterializedView(context, materializedViewName.asSchemaTableName(), properties));
    }

    @Override
    public void checkCanRefreshMaterializedView(SecurityContext securityContext, QualifiedObjectName materializedViewName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(materializedViewName, "materializedViewName is null");

        checkCanAccessCatalog(securityContext, materializedViewName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanRefreshMaterializedView(securityContext.toSystemSecurityContext(), materializedViewName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(materializedViewName.catalogName(), securityContext, (control, context) -> control.checkCanRefreshMaterializedView(context, materializedViewName.asSchemaTableName()));
    }

    @Override
    public void checkCanDropMaterializedView(SecurityContext securityContext, QualifiedObjectName materializedViewName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(materializedViewName, "materializedViewName is null");

        checkCanAccessCatalog(securityContext, materializedViewName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanDropMaterializedView(securityContext.toSystemSecurityContext(), materializedViewName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(materializedViewName.catalogName(), securityContext, (control, context) -> control.checkCanDropMaterializedView(context, materializedViewName.asSchemaTableName()));
    }

    @Override
    public void checkCanRenameMaterializedView(SecurityContext securityContext, QualifiedObjectName viewName, QualifiedObjectName newViewName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(viewName, "viewName is null");
        requireNonNull(newViewName, "newViewName is null");

        checkCanAccessCatalog(securityContext, viewName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanRenameMaterializedView(securityContext.toSystemSecurityContext(), viewName.asCatalogSchemaTableName(), newViewName.asCatalogSchemaTableName()));

        catalogAuthorizationCheck(viewName.catalogName(), securityContext, (control, context) -> control.checkCanRenameMaterializedView(context, viewName.asSchemaTableName(), newViewName.asSchemaTableName()));
    }

    @Override
    public void checkCanSetMaterializedViewProperties(SecurityContext securityContext, QualifiedObjectName materializedViewName, Map<String, Optional<Object>> properties)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(materializedViewName, "materializedViewName is null");
        requireNonNull(properties, "nonNullProperties is null");

        checkCanAccessCatalog(securityContext, materializedViewName.catalogName());

        systemAuthorizationCheck(control ->
                control.checkCanSetMaterializedViewProperties(
                        securityContext.toSystemSecurityContext(),
                        materializedViewName.asCatalogSchemaTableName(),
                        properties));

        catalogAuthorizationCheck(
                materializedViewName.catalogName(),
                securityContext,
                (control, context) -> control.checkCanSetMaterializedViewProperties(context, materializedViewName.asSchemaTableName(), properties));
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

        checkCanAccessCatalog(securityContext, tableName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanGrantTablePrivilege(securityContext.toSystemSecurityContext(), privilege, tableName.asCatalogSchemaTableName(), grantee, grantOption));

        catalogAuthorizationCheck(tableName.catalogName(), securityContext, (control, context) -> control.checkCanGrantTablePrivilege(context, privilege, tableName.asSchemaTableName(), grantee, grantOption));
    }

    @Override
    public void checkCanDenyTablePrivilege(SecurityContext securityContext, Privilege privilege, QualifiedObjectName tableName, TrinoPrincipal grantee)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(privilege, "privilege is null");

        checkCanAccessCatalog(securityContext, tableName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanDenyTablePrivilege(securityContext.toSystemSecurityContext(), privilege, tableName.asCatalogSchemaTableName(), grantee));

        catalogAuthorizationCheck(tableName.catalogName(), securityContext, (control, context) -> control.checkCanDenyTablePrivilege(context, privilege, tableName.asSchemaTableName(), grantee));
    }

    @Override
    public void checkCanRevokeTablePrivilege(SecurityContext securityContext, Privilege privilege, QualifiedObjectName tableName, TrinoPrincipal revokee, boolean grantOption)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(privilege, "privilege is null");

        checkCanAccessCatalog(securityContext, tableName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanRevokeTablePrivilege(securityContext.toSystemSecurityContext(), privilege, tableName.asCatalogSchemaTableName(), revokee, grantOption));

        catalogAuthorizationCheck(tableName.catalogName(), securityContext, (control, context) -> control.checkCanRevokeTablePrivilege(context, privilege, tableName.asSchemaTableName(), revokee, grantOption));
    }

    @Override
    public void checkCanGrantEntityPrivilege(SecurityContext securityContext, EntityPrivilege privilege, EntityKindAndName entity, TrinoPrincipal grantee, boolean grantOption)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(entity, "entity is null");
        requireNonNull(privilege, "privilege is null");

        systemAuthorizationCheck(control -> control.checkCanGrantEntityPrivilege(securityContext.toSystemSecurityContext(), privilege, entity, grantee, grantOption));
    }

    @Override
    public void checkCanDenyEntityPrivilege(SecurityContext securityContext, EntityPrivilege privilege, EntityKindAndName entity, TrinoPrincipal grantee)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(entity, "entity is null");
        requireNonNull(privilege, "privilege is null");

        systemAuthorizationCheck(control -> control.checkCanDenyEntityPrivilege(securityContext.toSystemSecurityContext(), privilege, entity, grantee));
    }

    @Override
    public void checkCanRevokeEntityPrivilege(SecurityContext securityContext, EntityPrivilege privilege, EntityKindAndName entity, TrinoPrincipal revokee, boolean grantOption)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(entity, "entity is null");
        requireNonNull(privilege, "privilege is null");

        systemAuthorizationCheck(control -> control.checkCanRevokeEntityPrivilege(securityContext.toSystemSecurityContext(), privilege, entity, revokee, grantOption));
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, QueryId queryId, String propertyName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(propertyName, "propertyName is null");

        systemAuthorizationCheck(control -> control.checkCanSetSystemSessionProperty(identity, queryId, propertyName));
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

        checkCanAccessCatalog(securityContext, tableName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanSelectFromColumns(securityContext.toSystemSecurityContext(), tableName.asCatalogSchemaTableName(), columnNames));

        catalogAuthorizationCheck(tableName.catalogName(), securityContext, (control, context) -> control.checkCanSelectFromColumns(context, tableName.asSchemaTableName(), columnNames));
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

        checkCanAccessCatalog(securityContext, procedureName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanExecuteProcedure(securityContext.toSystemSecurityContext(), procedureName.asCatalogSchemaRoutineName()));

        catalogAuthorizationCheck(
                procedureName.catalogName(),
                securityContext,
                (control, context) -> control.checkCanExecuteProcedure(context, procedureName.asSchemaRoutineName()));
    }

    @Override
    public boolean canExecuteFunction(SecurityContext securityContext, QualifiedObjectName functionName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(functionName, "functionName is null");

        if (!canAccessCatalog(securityContext, functionName.catalogName())) {
            return false;
        }

        if (!systemAuthorizationTest(control -> control.canExecuteFunction(securityContext.toSystemSecurityContext(), functionName.asCatalogSchemaRoutineName()))) {
            return false;
        }

        return catalogAuthorizationTest(
                functionName.catalogName(),
                securityContext,
                (control, context) -> control.canExecuteFunction(context, functionName.asSchemaRoutineName()));
    }

    @Override
    public boolean canCreateViewWithExecuteFunction(SecurityContext securityContext, QualifiedObjectName functionName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(functionName, "functionName is null");

        if (!canAccessCatalog(securityContext, functionName.catalogName())) {
            return false;
        }

        if (!systemAuthorizationTest(control -> control.canCreateViewWithExecuteFunction(securityContext.toSystemSecurityContext(), functionName.asCatalogSchemaRoutineName()))) {
            return false;
        }

        return catalogAuthorizationTest(
                functionName.catalogName(),
                securityContext,
                (control, context) -> control.canCreateViewWithExecuteFunction(context, functionName.asSchemaRoutineName()));
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
                tableName.catalogName(),
                securityContext,
                (control, context) -> control.checkCanExecuteTableProcedure(
                        context,
                        tableName.asSchemaTableName(),
                        procedureName));
    }

    @Override
    public void checkCanShowFunctions(SecurityContext securityContext, CatalogSchemaName schema)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(schema, "schema is null");

        checkCanAccessCatalog(securityContext, schema.getCatalogName());

        systemAuthorizationCheck(control -> control.checkCanShowFunctions(securityContext.toSystemSecurityContext(), schema));

        catalogAuthorizationCheck(schema.getCatalogName(), securityContext, (control, context) -> control.checkCanShowFunctions(context, schema.getSchemaName()));
    }

    @Override
    public Set<SchemaFunctionName> filterFunctions(SecurityContext securityContext, String catalogName, Set<SchemaFunctionName> functionNames)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(functionNames, "functionNames is null");

        if (functionNames.isEmpty()) {
            return ImmutableSet.of();
        }

        if (filterCatalogs(securityContext, ImmutableSet.of(catalogName)).isEmpty()) {
            return ImmutableSet.of();
        }

        for (SystemAccessControl systemAccessControl : getSystemAccessControls()) {
            functionNames = systemAccessControl.filterFunctions(securityContext.toSystemSecurityContext(), catalogName, functionNames);
        }

        ConnectorAccessControl connectorAccessControl = getConnectorAccessControl(securityContext.getTransactionId(), catalogName);
        if (connectorAccessControl != null) {
            functionNames = connectorAccessControl.filterFunctions(toConnectorSecurityContext(catalogName, securityContext), functionNames);
        }

        return functionNames;
    }

    @Override
    public void checkCanCreateFunction(SecurityContext securityContext, QualifiedObjectName functionName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(functionName, "functionName is null");

        checkCanAccessCatalog(securityContext, functionName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanCreateFunction(securityContext.toSystemSecurityContext(), functionName.asCatalogSchemaRoutineName()));

        catalogAuthorizationCheck(functionName.catalogName(), securityContext, (control, context) -> control.checkCanCreateFunction(context, functionName.asSchemaRoutineName()));
    }

    @Override
    public void checkCanDropFunction(SecurityContext securityContext, QualifiedObjectName functionName)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(functionName, "functionName is null");

        checkCanAccessCatalog(securityContext, functionName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanDropFunction(securityContext.toSystemSecurityContext(), functionName.asCatalogSchemaRoutineName()));

        catalogAuthorizationCheck(functionName.catalogName(), securityContext, (control, context) -> control.checkCanDropFunction(context, functionName.asSchemaRoutineName()));
    }

    @Override
    public void checkCanShowCreateFunction(SecurityContext context, QualifiedObjectName functionName)
    {
        requireNonNull(context, "context is null");
        requireNonNull(functionName, "functionName is null");

        checkCanAccessCatalog(context, functionName.catalogName());

        systemAuthorizationCheck(control -> control.checkCanShowCreateFunction(context.toSystemSecurityContext(), functionName.asCatalogSchemaRoutineName()));

        catalogAuthorizationCheck(functionName.catalogName(), context, (control, connectorContext) -> control.checkCanShowCreateFunction(connectorContext, functionName.asSchemaRoutineName()));
    }

    @Override
    public List<ViewExpression> getRowFilters(SecurityContext context, QualifiedObjectName tableName)
    {
        requireNonNull(context, "context is null");
        requireNonNull(tableName, "tableName is null");

        ImmutableList.Builder<ViewExpression> filters = ImmutableList.builder();
        ConnectorAccessControl connectorAccessControl = getConnectorAccessControl(context.getTransactionId(), tableName.catalogName());

        if (connectorAccessControl != null) {
            connectorAccessControl.getRowFilters(toConnectorSecurityContext(tableName.catalogName(), context), tableName.asSchemaTableName())
                    .forEach(filters::add);
        }

        for (SystemAccessControl systemAccessControl : getSystemAccessControls()) {
            systemAccessControl.getRowFilters(context.toSystemSecurityContext(), tableName.asCatalogSchemaTableName())
                    .forEach(filters::add);
        }

        return filters.build();
    }

    @Override
    public Map<ColumnSchema, ViewExpression> getColumnMasks(SecurityContext context, QualifiedObjectName tableName, List<ColumnSchema> columns)
    {
        requireNonNull(context, "context is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(columns, "columns is null");

        ImmutableMap.Builder<ColumnSchema, ViewExpression> columnMasksBuilder = ImmutableMap.builder();

        ConnectorAccessControl connectorAccessControl = getConnectorAccessControl(context.getTransactionId(), tableName.catalogName());
        if (connectorAccessControl != null) {
            Map<ColumnSchema, ViewExpression> connectorMasks = connectorAccessControl.getColumnMasks(toConnectorSecurityContext(tableName.catalogName(), context), tableName.asSchemaTableName(), columns);
            columnMasksBuilder.putAll(connectorMasks);
        }

        for (SystemAccessControl systemAccessControl : getSystemAccessControls()) {
            Map<ColumnSchema, ViewExpression> systemMasks = systemAccessControl.getColumnMasks(context.toSystemSecurityContext(), tableName.asCatalogSchemaTableName(), columns);
            columnMasksBuilder.putAll(systemMasks);
        }

        try {
            return columnMasksBuilder.buildOrThrow();
        }
        catch (IllegalArgumentException exception) {
            throw new TrinoException(INVALID_COLUMN_MASK, "Multiple masks for the same column found", exception);
        }
    }

    @Override
    public void checkCanSetEntityAuthorization(SecurityContext securityContext, EntityKindAndName entityKindAndName, TrinoPrincipal principal)
    {
        requireNonNull(securityContext, "securityContext is null");
        requireNonNull(entityKindAndName, "entityKindAndName is null");
        requireNonNull(principal, "principal is null");
        systemAuthorizationCheck(control -> control.checkCanSetEntityAuthorization(securityContext.toSystemSecurityContext(), entityKindAndName, principal));
        String ownedKind = entityKindAndName.entityKind();
        List<String> name = entityKindAndName.name();
        catalogAuthorizationCheck(name.get(0), securityContext, (control, context) -> {
            switch (ownedKind) {
                case "SCHEMA":
                    control.checkCanSetSchemaAuthorization(context, name.get(1), principal);
                    break;
                case "TABLE":
                    control.checkCanSetTableAuthorization(context, new SchemaTableName(name.get(1), name.get(2)), principal);
                    break;
                case "VIEW":
                    control.checkCanSetViewAuthorization(context, new SchemaTableName(name.get(1), name.get(2)), principal);
                    break;
                default:
                    denySetEntityAuthorization(new EntityKindAndName(ownedKind, name), principal);
            }
        });
    }

    private ConnectorAccessControl getConnectorAccessControl(TransactionId transactionId, String catalogName)
    {
        CatalogServiceProvider<Optional<ConnectorAccessControl>> connectorAccessControlProvider = this.connectorAccessControlProvider.get();
        if (connectorAccessControlProvider == null) {
            return null;
        }

        ConnectorAccessControl connectorAccessControl = transactionManager.getCatalogHandle(transactionId, catalogName)
                .flatMap(connectorAccessControlProvider::getService)
                .orElse(null);

        return connectorAccessControl;
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
        if (!canAccessCatalog(securityContext, catalogName)) {
            denyCatalogAccess(catalogName);
        }
    }

    private boolean canAccessCatalog(SecurityContext securityContext, String catalogName)
    {
        for (SystemAccessControl systemAccessControl : getSystemAccessControls()) {
            if (!systemAccessControl.canAccessCatalog(securityContext.toSystemSecurityContext(), catalogName)) {
                authorizationFail.update(1);
                return false;
            }
        }
        authorizationSuccess.update(1);
        return true;
    }

    private boolean systemAuthorizationTest(Predicate<SystemAccessControl> check)
    {
        for (SystemAccessControl systemAccessControl : getSystemAccessControls()) {
            if (!check.test(systemAccessControl)) {
                authorizationFail.update(1);
                return false;
            }
        }
        authorizationSuccess.update(1);
        return true;
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

    private boolean catalogAuthorizationTest(String catalogName, SecurityContext securityContext, BiPredicate<ConnectorAccessControl, ConnectorSecurityContext> check)
    {
        ConnectorAccessControl connectorAccessControl = getConnectorAccessControl(securityContext.getTransactionId(), catalogName);
        if (connectorAccessControl == null) {
            return true;
        }

        boolean result = check.test(connectorAccessControl, toConnectorSecurityContext(catalogName, securityContext));
        if (result) {
            authorizationSuccess.update(1);
        }
        else {
            authorizationFail.update(1);
        }
        return result;
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

    private static void verifySystemAccessControl(SystemAccessControl systemAccessControl)
    {
        Class<?> clazz = systemAccessControl.getClass();
        mustNotDeclareMethod(clazz, "checkCanAccessCatalog", SystemSecurityContext.class, String.class);
        mustNotDeclareMethod(clazz, "checkCanGrantExecuteFunctionPrivilege", SystemSecurityContext.class, String.class, TrinoPrincipal.class, boolean.class);
        mustNotDeclareMethod(clazz, "checkCanExecuteFunction", SystemSecurityContext.class, String.class);
        mustNotDeclareMethod(clazz, "checkCanExecuteFunction", SystemSecurityContext.class, FunctionKind.class, CatalogSchemaRoutineName.class);
        mustNotDeclareMethod(clazz, "checkCanGrantExecuteFunctionPrivilege", SystemSecurityContext.class, FunctionKind.class, CatalogSchemaRoutineName.class, TrinoPrincipal.class, boolean.class);
    }

    private static void mustNotDeclareMethod(Class<?> clazz, String name, Class<?>... parameterTypes)
    {
        try {
            clazz.getMethod(name, parameterTypes);
            throw new IllegalArgumentException(format("Access control %s must not implement removed method %s(%s)",
                    clazz.getName(),
                    name, Arrays.stream(parameterTypes).map(Class::getName).collect(Collectors.joining(", "))));
        }
        catch (ReflectiveOperationException _) {
        }
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
