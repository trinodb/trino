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
package io.prestosql.execution.warnings;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.Signature;
import io.prestosql.spi.PrestoWarning;
import io.prestosql.spi.deprecatedwarnings.DeprecatedWarningMessage;
import io.prestosql.spi.deprecatedwarnings.DeprecatedWarningsConfigurationManager;
import io.prestosql.spi.deprecatedwarnings.DeprecatedWarningsConfigurationManagerContext;
import io.prestosql.spi.deprecatedwarnings.DeprecatedWarningsConfigurationManagerFactory;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.deprecatedwarnings.DeprecatedWarningContext;
import io.prestosql.sql.deprecatedwarnings.DeprecatedWarningsConfigurationManagerContextInstance;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.connector.StandardWarningCode.SESSION_DEPRECATED_WARNINGS;
import static io.prestosql.spi.connector.StandardWarningCode.TABLE_DEPRECATED_WARNINGS;
import static io.prestosql.spi.connector.StandardWarningCode.UDF_DEPRECATED_WARNINGS;
import static io.prestosql.util.PropertiesUtil.loadProperties;
import static java.lang.String.format;

public class InternalDeprecatedWarningsManager
{
    private static final Logger log = Logger.get(InternalDeprecatedWarningsManager.class);
    private static final File DEPRECATED_WARNINGS_CONFIGURATION = new File("etc/deprecated-warnings-config.properties");
    private static final String DEPRECATED_WARNINGS_MANAGER_NAME = "deprecated-warnings-config.configuration-manager";

    private final Map<String, DeprecatedWarningsConfigurationManagerFactory> factories = new ConcurrentHashMap<>();
    private final AtomicReference<DeprecatedWarningsConfigurationManager> delegate = new AtomicReference<>();
    private final DeprecatedWarningsConfigurationManagerContext configurationManagerContext;

    @Inject
    public InternalDeprecatedWarningsManager(NodeInfo nodeInfo)
    {
        configurationManagerContext = new DeprecatedWarningsConfigurationManagerContextInstance(nodeInfo.getEnvironment());
    }

    public List<PrestoWarning> getDeprecatedWarnings(DeprecatedWarningContext deprecatedWarningContext)
    {
        if (delegate.get() == null) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<PrestoWarning> allWarnings = new ImmutableList.Builder<>();

        Set<PrestoWarning> tableDeprecatedWarnings = generateTableDeprecatedWarning(
                deprecatedWarningContext.getTables(), deprecatedWarningContext.getViews());
        allWarnings.addAll(tableDeprecatedWarnings);

        Set<PrestoWarning> udfDeprecatedWarnings = generateUDFDeprecatedWarning(deprecatedWarningContext.getFunctionSignatures());
        allWarnings.addAll(udfDeprecatedWarnings);

        Set<PrestoWarning> sessionPropertyDeprecatedWarnings = generateSessionPropertyDeprecatedWarning(deprecatedWarningContext.getSystemProperties());
        allWarnings.addAll(sessionPropertyDeprecatedWarnings);

        return allWarnings.build();
    }

    private Set<PrestoWarning> generateTableDeprecatedWarning(
            List<QualifiedObjectName> tables, Set<QualifiedObjectName> views)
    {
        ImmutableSet.Builder<PrestoWarning> warnings = new ImmutableSet.Builder<>();
        for (QualifiedObjectName qualifiedObjectName : tables) {
            String catalogName = qualifiedObjectName.getCatalogName();
            String schemaName = qualifiedObjectName.getSchemaName();
            String tableName = qualifiedObjectName.getObjectName();

            List<DeprecatedWarningMessage> deprecatedWarningMessages = delegate.get().getTableDeprecatedInfo(catalogName, schemaName, tableName);
            for (DeprecatedWarningMessage deprecatedWarningMessage : deprecatedWarningMessages) {
                String warningMessage = deprecatedWarningMessage.getWarningMessage().get();
                String jiraLink = deprecatedWarningMessage.getJiraLink().get();
                PrestoWarning warning;
                if (!views.contains(qualifiedObjectName)) {
                    warning = new PrestoWarning(TABLE_DEPRECATED_WARNINGS, format("Table %s.%s.%s has the following warning: %s : %s",
                        catalogName, schemaName, tableName, warningMessage, jiraLink));
                }
                else {
                    warning = new PrestoWarning(TABLE_DEPRECATED_WARNINGS, format("View %s.%s.%s has the following warning: %s : %s",
                        catalogName, schemaName, tableName, warningMessage, jiraLink));
                }
                warnings.add(warning);
            }
        }
        return warnings.build();
    }

    private Set<PrestoWarning> generateUDFDeprecatedWarning(List<Signature> functionSignatures)
    {
        ImmutableSet.Builder<PrestoWarning> warnings = new ImmutableSet.Builder<>();
        for (Signature signature : functionSignatures) {
            String functionName = signature.getName();
            String arguments = signature.getArgumentTypes().stream().map(TypeSignature::getBase).collect(Collectors.joining(","));
            String returnType = signature.getReturnType().getBase();

            List<DeprecatedWarningMessage> deprecatedWarningMessages = delegate.get().getUDFDeprecatedInfo(functionName, arguments, returnType);
            for (DeprecatedWarningMessage deprecatedWarningMessage : deprecatedWarningMessages) {
                String warningMessage = deprecatedWarningMessage.getWarningMessage().get();
                String jiraLink = deprecatedWarningMessage.getJiraLink().get();
                warnings.add(new PrestoWarning(UDF_DEPRECATED_WARNINGS, format("UDF: %s(%s):%s has the following warning: %s : %s",
                        functionName, arguments, returnType, warningMessage, jiraLink)));
            }
        }
        return warnings.build();
    }

    private Set<PrestoWarning> generateSessionPropertyDeprecatedWarning(Map<String, String> systemProperties)
    {
        ImmutableSet.Builder<PrestoWarning> warnings = new ImmutableSet.Builder<>();
        for (String systemProperty : systemProperties.keySet()) {
            List<DeprecatedWarningMessage> deprecatedWarningMessages =
                    delegate.get().getSessionPropertyDeprecatedInfo(systemProperty, systemProperties.get(systemProperty));
            for (DeprecatedWarningMessage deprecatedWarningMessage : deprecatedWarningMessages) {
                String warningMessage = deprecatedWarningMessage.getWarningMessage().get();
                String jiraLink = deprecatedWarningMessage.getJiraLink().get();
                warnings.add(new PrestoWarning(SESSION_DEPRECATED_WARNINGS, format("Session Property: %s has the following warning: %s : %s",
                        systemProperty, warningMessage, jiraLink)));
            }
        }
        return warnings.build();
    }

    public void addConfigurationManagerFactory(
            DeprecatedWarningsConfigurationManagerFactory deprecatedWarningsConfigurationManagerFactory)
    {
        if (factories.putIfAbsent(deprecatedWarningsConfigurationManagerFactory.getName(),
                deprecatedWarningsConfigurationManagerFactory) != null) {
            throw new IllegalArgumentException(format("Deprecated warnings property configuration manager '%s' is already registered", deprecatedWarningsConfigurationManagerFactory
                .getName()));
        }
    }

    public void loadConfigurationManager()
            throws IOException
    {
        if (!DEPRECATED_WARNINGS_CONFIGURATION.exists()) {
            return;
        }

        Map<String, String> propertyMap = new HashMap<>(loadProperties(DEPRECATED_WARNINGS_CONFIGURATION));

        log.info("-- Loading deprecated warnings configuration manager --");

        String configurationManagerName = propertyMap.remove(DEPRECATED_WARNINGS_MANAGER_NAME);
        checkArgument(configurationManagerName != null, "Deprecated warnings configuration %s does not contain %s", DEPRECATED_WARNINGS_CONFIGURATION, DEPRECATED_WARNINGS_MANAGER_NAME);

        setConfigurationManager(configurationManagerName, propertyMap);

        log.info("-- Loaded deprecated warnings configuration manager %s --", configurationManagerName);
    }

    @VisibleForTesting
    public void setConfigurationManager(String name, Map<String, String> properties)
    {
        DeprecatedWarningsConfigurationManagerFactory factory = factories.get(name);
        checkState(factory != null, "Deprecated warnings configuration manager %s is not registered", name);

        DeprecatedWarningsConfigurationManager manager = factory.create(properties, configurationManagerContext);
        checkState(delegate.compareAndSet(null, manager), "deprecatedWarningsConfigurationManager is already set");
    }

    @VisibleForTesting
    public DeprecatedWarningsConfigurationManagerContext getConfigurationManagerContext()
    {
        return configurationManagerContext;
    }
}
