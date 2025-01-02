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

import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.ApplicationConfigurationException;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.compress.v3.lz4.Lz4NativeCompressor;
import io.airlift.compress.v3.snappy.SnappyNativeCompressor;
import io.airlift.compress.v3.zstd.ZstdNativeCompressor;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.http.server.HttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.JmxHttpModule;
import io.airlift.jmx.JmxModule;
import io.airlift.json.JsonModule;
import io.airlift.log.LogJmxModule;
import io.airlift.log.Logger;
import io.airlift.node.NodeModule;
import io.airlift.openmetrics.JmxOpenMetricsModule;
import io.airlift.tracing.TracingModule;
import io.airlift.units.Duration;
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogManagerConfig;
import io.trino.connector.CatalogManagerConfig.CatalogMangerKind;
import io.trino.connector.CatalogManagerModule;
import io.trino.connector.CatalogStoreManager;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.eventlistener.EventListenerManager;
import io.trino.eventlistener.EventListenerModule;
import io.trino.exchange.ExchangeManagerModule;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.resourcegroups.ResourceGroupManager;
import io.trino.execution.warnings.WarningCollectorModule;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.security.AccessControlManager;
import io.trino.security.AccessControlModule;
import io.trino.security.GroupProviderManager;
import io.trino.server.protocol.spooling.SpoolingManagerRegistry;
import io.trino.server.security.CertificateAuthenticatorManager;
import io.trino.server.security.HeaderAuthenticatorManager;
import io.trino.server.security.PasswordAuthenticatorManager;
import io.trino.server.security.ServerSecurityModule;
import io.trino.server.security.oauth2.OAuth2Client;
import io.trino.spi.connector.CatalogHandle;
import io.trino.transaction.TransactionManagerModule;
import io.trino.util.EmbedVersion;
import org.weakref.jmx.guice.MBeanModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.MoreCollectors.toOptional;
import static io.airlift.discovery.client.ServiceAnnouncement.ServiceAnnouncementBuilder;
import static io.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static io.trino.server.TrinoSystemRequirements.verifySystemRequirements;
import static java.lang.String.format;
import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.util.stream.Collectors.joining;

public class Server
{
    public final void start(String trinoVersion)
    {
        new EmbedVersion(trinoVersion).embedVersion(() -> doStart(trinoVersion)).run();
    }

    private void doStart(String trinoVersion)
    {
        // Trino server behavior does not depend on locale settings.
        // Use en_US as this is what Trino is tested with.
        Locale.setDefault(Locale.US);

        long startTime = System.nanoTime();
        verifySystemRequirements();

        Logger log = Logger.get(Server.class);
        log.info("Java version: %s", StandardSystemProperty.JAVA_VERSION.value());

        ImmutableList.Builder<Module> modules = ImmutableList.builder();
        modules.add(
                new NodeModule(),
                new DiscoveryModule(),
                new HttpServerModule(),
                new JsonModule(),
                new JaxrsModule(),
                new MBeanModule(),
                new PrefixObjectNameGeneratorModule("io.trino"),
                new JmxModule(),
                new JmxHttpModule(),
                new JmxOpenMetricsModule(),
                new LogJmxModule(),
                new TracingModule("trino", trinoVersion),
                new ServerSecurityModule(),
                new AccessControlModule(),
                new EventListenerModule(),
                new ExchangeManagerModule(),
                new CoordinatorDiscoveryModule(),
                new CatalogManagerModule(),
                new TransactionManagerModule(),
                new ServerMainModule(trinoVersion),
                new NodeStateManagerModule(),
                new WarningCollectorModule());

        modules.addAll(getAdditionalModules());

        Bootstrap app = new Bootstrap(modules.build())
                .loadSecretsPlugins();

        try {
            Injector injector = app.initialize();

            log.info("Trino version: %s", injector.getInstance(NodeVersion.class).getVersion());
            log.info("Zstandard native compression: %s", formatEnabled(ZstdNativeCompressor.isEnabled()));
            log.info("Lz4 native compression: %s", formatEnabled(Lz4NativeCompressor.isEnabled()));
            log.info("Snappy native compression: %s", formatEnabled(SnappyNativeCompressor.isEnabled()));

            logLocation(log, "Working directory", Paths.get("."));
            logLocation(log, "Etc directory", Paths.get("etc"));

            injector.getInstance(PluginInstaller.class).loadPlugins();

            var catalogStoreManager = injector.getInstance(Key.get(new TypeLiteral<Optional<CatalogStoreManager>>() {}));
            catalogStoreManager.ifPresent(CatalogStoreManager::loadConfiguredCatalogStore);

            ConnectorServicesProvider connectorServicesProvider = injector.getInstance(ConnectorServicesProvider.class);
            connectorServicesProvider.loadInitialCatalogs();

            // Only static catalog manager announces catalogs
            // Connector event listeners are only supported for statically loaded catalogs
            if (injector.getInstance(CatalogManagerConfig.class).getCatalogMangerKind() == CatalogMangerKind.STATIC) {
                CatalogManager catalogManager = injector.getInstance(CatalogManager.class);
                // TODO: remove this huge hack
                updateConnectorIds(injector.getInstance(Announcer.class), catalogManager);
            }

            injector.getInstance(SessionPropertyDefaults.class).loadConfigurationManager();
            injector.getInstance(ResourceGroupManager.class).loadConfigurationManager();
            injector.getInstance(AccessControlManager.class).loadSystemAccessControl();
            injector.getInstance(Key.get(new TypeLiteral<Optional<PasswordAuthenticatorManager>>() {}))
                    .ifPresent(PasswordAuthenticatorManager::loadPasswordAuthenticator);
            injector.getInstance(GroupProviderManager.class).loadConfiguredGroupProvider();
            injector.getInstance(ExchangeManagerRegistry.class).loadExchangeManager();
            injector.getInstance(SpoolingManagerRegistry.class).loadSpoolingManager();
            injector.getInstance(CertificateAuthenticatorManager.class).loadCertificateAuthenticator();
            injector.getInstance(Key.get(new TypeLiteral<Optional<HeaderAuthenticatorManager>>() {}))
                    .ifPresent(HeaderAuthenticatorManager::loadHeaderAuthenticator);

            if (injector.getInstance(ServerConfig.class).isCoordinator()) {
                injector.getInstance(EventListenerManager.class).loadEventListeners();
            }

            injector.getInstance(Key.get(new TypeLiteral<Optional<OAuth2Client>>() {}))
                    .ifPresent(OAuth2Client::load);

            injector.getInstance(Announcer.class).start();

            injector.getInstance(StartupStatus.class).startupComplete();
            log.info("Server startup completed in %s", Duration.nanosSince(startTime).convertToMostSuccinctTimeUnit());
            log.info("======== SERVER STARTED ========");
        }
        catch (ApplicationConfigurationException e) {
            StringBuilder message = new StringBuilder();
            message.append("Configuration is invalid\n");
            message.append("==========\n");
            addMessages(message, "Errors", ImmutableList.copyOf(e.getErrors()));
            addMessages(message, "Warnings", ImmutableList.copyOf(e.getWarnings()));
            message.append("\n");
            message.append("==========");
            log.error("%s", message);
            System.exit(100);
        }
        catch (Throwable e) {
            log.error(e);
            System.exit(100);
        }
    }

    private static void addMessages(StringBuilder output, String type, List<Object> messages)
    {
        if (messages.isEmpty()) {
            return;
        }
        output.append("\n").append(type).append(":\n\n");
        for (int index = 0; index < messages.size(); index++) {
            output.append(format("%s) %s\n", index + 1, messages.get(index)));
        }
    }

    protected Iterable<? extends Module> getAdditionalModules()
    {
        return ImmutableList.of();
    }

    private static void updateConnectorIds(Announcer announcer, CatalogManager catalogManager)
    {
        // get existing announcement
        ServiceAnnouncement announcement = getTrinoAnnouncement(announcer.getServiceAnnouncements());

        // automatically build catalogHandleIds if not configured
        String catalogHandleIds = catalogManager.getCatalogNames().stream()
                .map(catalogManager::getCatalog)
                .flatMap(Optional::stream)
                .map(Catalog::getCatalogHandle)
                .map(CatalogHandle::getId)
                .distinct()
                .sorted()
                .collect(joining(","));

        // build announcement with updated sources
        ServiceAnnouncementBuilder builder = serviceAnnouncement(announcement.getType());
        builder.addProperties(announcement.getProperties());
        builder.addProperty("catalogHandleIds", catalogHandleIds);

        // update announcement
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(builder.build());
    }

    private static ServiceAnnouncement getTrinoAnnouncement(Set<ServiceAnnouncement> announcements)
    {
        return announcements.stream()
                .filter(announcement -> announcement.getType().equals("trino"))
                .collect(toOptional())
                .orElseThrow(() -> new IllegalArgumentException("Trino announcement not found: " + announcements));
    }

    private static void logLocation(Logger log, String name, Path path)
    {
        if (!Files.exists(path, NOFOLLOW_LINKS)) {
            log.info("%s: [does not exist]", name);
            return;
        }
        try {
            path = path.toAbsolutePath().toRealPath();
        }
        catch (IOException e) {
            log.info("%s: [not accessible]", name);
            return;
        }
        log.info("%s: %s", name, path);
    }

    private static String formatEnabled(boolean flag)
    {
        return flag ? "enabled" : "disabled";
    }
}
