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
package io.prestosql.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.stats.CounterStat;
import io.prestosql.Session;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.spi.resourcegroups.SessionPropertyConfigurationManagerContext;
import io.prestosql.spi.session.SessionConfigurationContext;
import io.prestosql.spi.session.SessionPropertyConfigurationManager;
import io.prestosql.spi.session.SessionPropertyConfigurationManagerFactory;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.prestosql.util.PropertiesUtil.loadProperties;
import static java.lang.String.format;

public class SessionPropertyDefaults
{
    private static final Logger log = Logger.get(SessionPropertyDefaults.class);

    private static final File CONFIG_FILE = new File("etc/session-property-config.properties");
    private static final String NAME_PROPERTY = "session-property-config.configuration-manager";

    private final SessionPropertyConfigurationManagerContext configurationManagerContext;
    private final Map<String, SessionPropertyConfigurationManagerFactory> factories = new ConcurrentHashMap<>();
    private final AtomicReference<SessionPropertyConfigurationManager> delegate = new AtomicReference<>();

    private final SessionPropertyManager sessionPropertyManager;

    private final CounterStat systemPropertyOverrideFailures = new CounterStat();

    @Inject
    public SessionPropertyDefaults(NodeInfo nodeInfo, SessionPropertyManager sessionPropertyManager)
    {
        this.configurationManagerContext = new SessionPropertyConfigurationManagerContextInstance(nodeInfo.getEnvironment());
        this.sessionPropertyManager = sessionPropertyManager;
    }

    public void addConfigurationManagerFactory(SessionPropertyConfigurationManagerFactory sessionConfigFactory)
    {
        if (factories.putIfAbsent(sessionConfigFactory.getName(), sessionConfigFactory) != null) {
            throw new IllegalArgumentException(format("Session property configuration manager '%s' is already registered", sessionConfigFactory.getName()));
        }
    }

    public void loadConfigurationManager()
            throws IOException
    {
        File configFile = CONFIG_FILE.getAbsoluteFile();
        if (!configFile.exists()) {
            return;
        }

        Map<String, String> properties = new HashMap<>(loadProperties(configFile));

        String name = properties.remove(NAME_PROPERTY);
        checkState(!isNullOrEmpty(name), "Session property configuration %s does not contain '%s'", configFile, NAME_PROPERTY);

        setConfigurationManager(name, properties);
    }

    @VisibleForTesting
    public void setConfigurationManager(String name, Map<String, String> properties)
    {
        log.info("-- Loading session property configuration manager --");

        SessionPropertyConfigurationManagerFactory factory = factories.get(name);
        checkState(factory != null, "Session property configuration manager '%s' is not registered", name);

        SessionPropertyConfigurationManager manager = factory.create(properties, configurationManagerContext);
        checkState(delegate.compareAndSet(null, manager), "sessionPropertyConfigurationManager is already set");

        log.info("-- Loaded session property configuration manager %s --", name);
    }

    public Session newSessionWithDefaultProperties(Session session, Optional<String> queryType, ResourceGroupId resourceGroupId)
    {
        SessionPropertyConfigurationManager configurationManager = delegate.get();
        if (configurationManager == null) {
            return session;
        }

        SessionConfigurationContext context = new SessionConfigurationContext(
                session.getIdentity().getUser(),
                session.getSource(),
                session.getClientTags(),
                queryType,
                resourceGroupId);

        Map<String, String> systemPropertyOverrides = filterInvalidSystemPropertyOverrides(configurationManager.getSystemSessionProperties(context));
        Map<String, Map<String, String>> catalogPropertyOverrides = configurationManager.getCatalogSessionProperties(context);
        return session.withDefaultProperties(systemPropertyOverrides, catalogPropertyOverrides);
    }

    private Map<String, String> filterInvalidSystemPropertyOverrides(Map<String, String> systemProperties)
    {
        ImmutableMap.Builder<String, String> validProperties = ImmutableMap.builder();

        for (Map.Entry<String, String> property : systemProperties.entrySet()) {
            try {
                sessionPropertyManager.validateSystemSessionProperty(property.getKey(), property.getValue());
            }
            catch (RuntimeException e) {
                log.error(e, format("Failed to set value %s for property %s",
                        property.getValue(), property.getKey()));
                systemPropertyOverrideFailures.update(1);
                continue;
            }
            validProperties.put(property.getKey(), property.getValue());
        }
        return validProperties.build();
    }

    @Managed
    @Nested
    public CounterStat getSystemPropertyOverrideFailures()
    {
        return systemPropertyOverrideFailures;
    }
}
