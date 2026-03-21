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
package io.trino.server;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.log.TerminalColors;
import io.airlift.units.Duration;
import io.trino.connector.CatalogFactory;
import io.trino.connector.CatalogStoreManager;
import io.trino.eventlistener.EventListenerManager;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.resourcegroups.ResourceGroupManager;
import io.trino.metadata.BlockEncodingManager;
import io.trino.metadata.GlobalFunctionCatalog;
import io.trino.metadata.HandleResolver;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.metadata.InternalFunctionBundle.InternalFunctionBundleBuilder;
import io.trino.metadata.LanguageFunctionEngineManager;
import io.trino.metadata.TypeRegistry;
import io.trino.security.AccessControlManager;
import io.trino.security.GroupProviderManager;
import io.trino.server.protocol.spooling.SpoolingManagerRegistry;
import io.trino.server.security.CertificateAuthenticatorManager;
import io.trino.server.security.HeaderAuthenticatorManager;
import io.trino.server.security.PasswordAuthenticatorManager;
import io.trino.spi.Plugin;
import io.trino.spi.block.BlockEncoding;
import io.trino.spi.catalog.CatalogStoreFactory;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.eventlistener.EventListenerFactory;
import io.trino.spi.exchange.ExchangeManagerFactory;
import io.trino.spi.function.LanguageFunctionEngine;
import io.trino.spi.resourcegroups.ResourceGroupConfigurationManagerFactory;
import io.trino.spi.security.CertificateAuthenticatorFactory;
import io.trino.spi.security.GroupProviderFactory;
import io.trino.spi.security.HeaderAuthenticatorFactory;
import io.trino.spi.security.PasswordAuthenticatorFactory;
import io.trino.spi.security.SystemAccessControlFactory;
import io.trino.spi.session.SessionPropertyConfigurationManagerFactory;
import io.trino.spi.spool.SpoolingManagerFactory;
import io.trino.spi.type.ParametricType;
import io.trino.spi.type.Type;

import java.net.URL;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.log.TerminalColors.Color.PURPLE;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class PluginManager
        implements PluginInstaller
{
    private static final TerminalColors COLORS = new TerminalColors(true);

    private static final List<String> SPI_PACKAGES = ImmutableList.<String>builder()
            .add("io.trino.spi.")
            .add("com.fasterxml.jackson.annotation.")
            .add("io.airlift.slice.")
            .add("io.opentelemetry.api.")
            .add("io.opentelemetry.context.")
            .build();

    private static final Logger log = Logger.get(PluginManager.class);

    private final PluginsProvider pluginsProvider;
    private final Optional<CatalogStoreManager> catalogStoreManager;
    private final CatalogFactory connectorFactory;
    private final GlobalFunctionCatalog globalFunctionCatalog;
    private final LanguageFunctionEngineManager languageFunctionEngineManager;
    private final ResourceGroupManager<?> resourceGroupManager;
    private final AccessControlManager accessControlManager;
    private final Optional<PasswordAuthenticatorManager> passwordAuthenticatorManager;
    private final CertificateAuthenticatorManager certificateAuthenticatorManager;
    private final Optional<HeaderAuthenticatorManager> headerAuthenticatorManager;
    private final EventListenerManager eventListenerManager;
    private final GroupProviderManager groupProviderManager;
    private final ExchangeManagerRegistry exchangeManagerRegistry;
    private final SpoolingManagerRegistry spoolingManagerRegistry;
    private final SessionPropertyDefaults sessionPropertyDefaults;
    private final TypeRegistry typeRegistry;
    private final BlockEncodingManager blockEncodingManager;
    private final HandleResolver handleResolver;
    private final AtomicBoolean pluginsLoading = new AtomicBoolean();

    @Inject
    public PluginManager(
            PluginsProvider pluginsProvider,
            Optional<CatalogStoreManager> catalogStoreManager,
            CatalogFactory connectorFactory,
            GlobalFunctionCatalog globalFunctionCatalog,
            LanguageFunctionEngineManager languageFunctionEngineManager,
            ResourceGroupManager<?> resourceGroupManager,
            AccessControlManager accessControlManager,
            Optional<PasswordAuthenticatorManager> passwordAuthenticatorManager,
            CertificateAuthenticatorManager certificateAuthenticatorManager,
            Optional<HeaderAuthenticatorManager> headerAuthenticatorManager,
            EventListenerManager eventListenerManager,
            GroupProviderManager groupProviderManager,
            SessionPropertyDefaults sessionPropertyDefaults,
            TypeRegistry typeRegistry,
            BlockEncodingManager blockEncodingManager,
            HandleResolver handleResolver,
            ExchangeManagerRegistry exchangeManagerRegistry,
            SpoolingManagerRegistry spoolingManagerRegistry)
    {
        this.pluginsProvider = requireNonNull(pluginsProvider, "pluginsProvider is null");
        this.catalogStoreManager = requireNonNull(catalogStoreManager, "catalogStoreManager is null");
        this.connectorFactory = requireNonNull(connectorFactory, "connectorFactory is null");
        this.globalFunctionCatalog = requireNonNull(globalFunctionCatalog, "globalFunctionCatalog is null");
        this.languageFunctionEngineManager = requireNonNull(languageFunctionEngineManager, "languageFunctionEngineManager is null");
        this.resourceGroupManager = requireNonNull(resourceGroupManager, "resourceGroupManager is null");
        this.accessControlManager = requireNonNull(accessControlManager, "accessControlManager is null");
        this.passwordAuthenticatorManager = requireNonNull(passwordAuthenticatorManager, "passwordAuthenticatorManager is null");
        this.certificateAuthenticatorManager = requireNonNull(certificateAuthenticatorManager, "certificateAuthenticatorManager is null");
        this.headerAuthenticatorManager = requireNonNull(headerAuthenticatorManager, "headerAuthenticatorManager is null");
        this.eventListenerManager = requireNonNull(eventListenerManager, "eventListenerManager is null");
        this.groupProviderManager = requireNonNull(groupProviderManager, "groupProviderManager is null");
        this.sessionPropertyDefaults = requireNonNull(sessionPropertyDefaults, "sessionPropertyDefaults is null");
        this.typeRegistry = requireNonNull(typeRegistry, "typeRegistry is null");
        this.blockEncodingManager = requireNonNull(blockEncodingManager, "blockEncodingManager is null");
        this.handleResolver = requireNonNull(handleResolver, "handleResolver is null");
        this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");
        this.spoolingManagerRegistry = requireNonNull(spoolingManagerRegistry, "spoolingManagerRegistry is null");
    }

    @Override
    public void loadPlugins()
    {
        if (!pluginsLoading.compareAndSet(false, true)) {
            return;
        }

        pluginsProvider.loadPlugins(this::loadPlugin, PluginManager::createClassLoader);
        typeRegistry.verifyTypes();
    }

    private void loadPlugin(String plugin, Supplier<PluginClassLoader> createClassLoader)
    {
        PluginClassLoader pluginClassLoader = createClassLoader.get();
        handleResolver.registerClassLoader(pluginClassLoader);
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(pluginClassLoader)) {
            for (InstalledFeatures features : loadPlugin(plugin, pluginClassLoader)) {
                if (!features.isEmpty()) {
                    log.info("Loaded plugin %s (%s) with features:", COLORS.colored(features.pluginClass().getSimpleName(), PURPLE), features.loadingTime().convertToMostSuccinctTimeUnit());
                    for (Feature feature : Feature.values()) {
                        List<String> names = features.names(feature);
                        if (!names.isEmpty()) {
                            log.info("  %s: %s", feature.getDescription(), Joiner.on(", ").join(names));
                        }
                    }
                }
            }
        }
    }

    private List<InstalledFeatures> loadPlugin(String pluginPath, PluginClassLoader pluginClassLoader)
    {
        ServiceLoader<Plugin> serviceLoader = ServiceLoader.load(Plugin.class, pluginClassLoader);
        List<Plugin> plugins = ImmutableList.copyOf(serviceLoader);
        checkState(!plugins.isEmpty(), "%s - No service providers of type %s in the classpath: %s", pluginPath, Plugin.class.getName(), asList(pluginClassLoader.getURLs()));

        ImmutableList.Builder<InstalledFeatures> features = ImmutableList.builderWithExpectedSize(plugins.size());
        for (Plugin plugin : plugins) {
            features.add(installPlugin(plugin));
        }
        return features.build();
    }

    @Override
    public InstalledFeatures installPlugin(Plugin plugin)
    {
        return installPluginInternal(plugin);
    }

    private InstalledFeatures installPluginInternal(Plugin plugin)
    {
        InstalledFeatures.Builder builder = InstalledFeatures.builder(plugin.getClass());
        long startTime = System.nanoTime();
        catalogStoreManager.ifPresent(catalogStoreManager -> {
            for (CatalogStoreFactory catalogStoreFactory : plugin.getCatalogStoreFactories()) {
                builder.withFeature(Feature.CATALOG_STORE, catalogStoreFactory.getName());
                catalogStoreManager.addCatalogStoreFactory(catalogStoreFactory);
            }
        });

        for (BlockEncoding blockEncoding : plugin.getBlockEncodings()) {
            builder.withFeature(Feature.BLOCK_ENCODING, blockEncoding.getName());
            blockEncodingManager.addBlockEncoding(blockEncoding);
        }

        for (Type type : plugin.getTypes()) {
            builder.withFeature(Feature.TYPE, type.getDisplayName());
            typeRegistry.addType(type);
        }

        for (ParametricType parametricType : plugin.getParametricTypes()) {
            builder.withFeature(Feature.PARAMETRIC_TYPE, parametricType.getName());
            typeRegistry.addParametricType(parametricType);
        }

        for (ConnectorFactory connectorFactory : plugin.getConnectorFactories()) {
            builder.withFeature(Feature.CONNECTOR, connectorFactory.getName());
            this.connectorFactory.addConnectorFactory(connectorFactory);
        }

        Set<Class<?>> functions = plugin.getFunctions();
        if (!functions.isEmpty()) {
            InternalFunctionBundleBuilder functionsBuilder = InternalFunctionBundle.builder();
            functions.forEach(functionsBuilder::functions);
            InternalFunctionBundle bundle = functionsBuilder.build();
            bundle.getFunctions()
                    .stream()
                    .filter(function -> !function.isHidden())
                    .forEach(metadata -> builder.withFeature(Feature.FUNCTION, metadata.getCanonicalName()));
            globalFunctionCatalog.addFunctions(bundle);
        }

        for (LanguageFunctionEngine languageFunctionEngine : plugin.getLanguageFunctionEngines()) {
            builder.withFeature(Feature.LANGUAGE_FUNCTION, languageFunctionEngine.getLanguage());
            languageFunctionEngineManager.addLanguageFunctionEngine(languageFunctionEngine);
        }

        for (SessionPropertyConfigurationManagerFactory sessionConfigFactory : plugin.getSessionPropertyConfigurationManagerFactories()) {
            builder.withFeature(Feature.SESSION_PROPERTY_CONFIGURATION_MANAGER, sessionConfigFactory.getName());
            sessionPropertyDefaults.addConfigurationManagerFactory(sessionConfigFactory);
        }

        for (ResourceGroupConfigurationManagerFactory configurationManagerFactory : plugin.getResourceGroupConfigurationManagerFactories()) {
            builder.withFeature(Feature.RESOURCE_GROUP_CONFIGURATION_MANAGER, configurationManagerFactory.getName());
            resourceGroupManager.addConfigurationManagerFactory(configurationManagerFactory);
        }

        for (SystemAccessControlFactory accessControlFactory : plugin.getSystemAccessControlFactories()) {
            builder.withFeature(Feature.ACCESS_CONTROL, accessControlFactory.getName());
            accessControlManager.addSystemAccessControlFactory(accessControlFactory);
        }

        passwordAuthenticatorManager.ifPresent(authenticationManager -> {
            for (PasswordAuthenticatorFactory authenticatorFactory : plugin.getPasswordAuthenticatorFactories()) {
                builder.withFeature(Feature.PASSWORD_AUTHENTICATOR, authenticatorFactory.getName());
                authenticationManager.addPasswordAuthenticatorFactory(authenticatorFactory);
            }
        });

        for (CertificateAuthenticatorFactory authenticatorFactory : plugin.getCertificateAuthenticatorFactories()) {
            builder.withFeature(Feature.CERTIFICATE_AUTHENTICATOR, authenticatorFactory.getName());
            certificateAuthenticatorManager.addCertificateAuthenticatorFactory(authenticatorFactory);
        }

        headerAuthenticatorManager.ifPresent(authenticationManager -> {
            for (HeaderAuthenticatorFactory authenticatorFactory : plugin.getHeaderAuthenticatorFactories()) {
                builder.withFeature(Feature.HEADER_AUTHENTICATOR, authenticatorFactory.getName());
                authenticationManager.addHeaderAuthenticatorFactory(authenticatorFactory);
            }
        });

        for (EventListenerFactory eventListenerFactory : plugin.getEventListenerFactories()) {
            builder.withFeature(Feature.EVENT_LISTENER, eventListenerFactory.getName());
            eventListenerManager.addEventListenerFactory(eventListenerFactory);
        }

        for (GroupProviderFactory groupProviderFactory : plugin.getGroupProviderFactories()) {
            builder.withFeature(Feature.GROUP_PROVIDER, groupProviderFactory.getName());
            groupProviderManager.addGroupProviderFactory(groupProviderFactory);
        }

        for (ExchangeManagerFactory exchangeManagerFactory : plugin.getExchangeManagerFactories()) {
            builder.withFeature(Feature.EXCHANGE_MANAGER, exchangeManagerFactory.getName());
            exchangeManagerRegistry.addExchangeManagerFactory(exchangeManagerFactory);
        }

        for (SpoolingManagerFactory spoolingManagerFactory : plugin.getSpoolingManagerFactories()) {
            builder.withFeature(Feature.SPOOLING_MANAGER, spoolingManagerFactory.getName());
            spoolingManagerRegistry.addSpoolingManagerFactory(spoolingManagerFactory);
        }

        return builder
                .withLoadingTime(Duration.nanosSince(startTime))
                .build();
    }

    public static PluginClassLoader createClassLoader(String pluginName, List<URL> urls)
    {
        ClassLoader parent = PluginManager.class.getClassLoader();
        PluginClassLoader classLoader = new PluginClassLoader(pluginName, urls, parent, SPI_PACKAGES);

        if (log.isDebugEnabled()) {
            log.debug("Classpath for plugin %s: ", pluginName);
            for (URL url : classLoader.getURLs()) {
                log.debug("    %s", url.getPath());
            }
        }
        return classLoader;
    }

    public interface PluginsProvider
    {
        void loadPlugins(Loader loader, ClassLoaderFactory createClassLoader);

        interface Loader
        {
            void load(String description, Supplier<PluginClassLoader> getClassLoader);
        }

        interface ClassLoaderFactory
        {
            PluginClassLoader create(String pluginName, List<URL> urls);
        }
    }
}
