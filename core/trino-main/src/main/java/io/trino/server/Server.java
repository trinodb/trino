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
import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.util.Types;
import io.airlift.bootstrap.ApplicationConfigurationException;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.event.client.EventModule;
import io.airlift.event.client.JsonEventModule;
import io.airlift.http.server.HttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.JmxHttpModule;
import io.airlift.jmx.JmxModule;
import io.airlift.json.JsonModule;
import io.airlift.log.LogJmxModule;
import io.airlift.log.Logger;
import io.airlift.node.NodeModule;
import io.airlift.tracetoken.TraceTokenModule;
import io.trino.client.NodeVersion;
import io.trino.eventlistener.EventListenerManager;
import io.trino.eventlistener.EventListenerModule;
import io.trino.exchange.ExchangeManagerModule;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.resourcegroups.ResourceGroupManager;
import io.trino.execution.warnings.WarningCollectorModule;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.StaticCatalogStore;
import io.trino.security.AccessControlManager;
import io.trino.security.AccessControlModule;
import io.trino.security.GroupProviderManager;
import io.trino.server.security.CertificateAuthenticatorManager;
import io.trino.server.security.HeaderAuthenticatorManager;
import io.trino.server.security.PasswordAuthenticatorManager;
import io.trino.server.security.ServerSecurityModule;
import io.trino.version.EmbedVersion;
import org.weakref.jmx.guice.MBeanModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.discovery.client.ServiceAnnouncement.ServiceAnnouncementBuilder;
import static io.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static io.trino.server.TrinoSystemRequirements.verifyJvmRequirements;
import static io.trino.server.TrinoSystemRequirements.verifySystemTimeIsReasonable;
import static java.lang.String.format;
import static java.nio.file.LinkOption.NOFOLLOW_LINKS;

public class Server
{
    public final void start(String trinoVersion)
    {
        new EmbedVersion(trinoVersion).embedVersion(() -> doStart(trinoVersion)).run();
    }

    private void doStart(String trinoVersion)
    {
        verifyJvmRequirements();
        verifySystemTimeIsReasonable();

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
                new LogJmxModule(),
                new TraceTokenModule(),
                new EventModule(),
                new JsonEventModule(),
                new ServerSecurityModule(),
                new AccessControlModule(),
                new EventListenerModule(),
                new ExchangeManagerModule(),
                new CoordinatorDiscoveryModule(),
                new ServerMainModule(trinoVersion),
                new GracefulShutdownModule(),
                new WarningCollectorModule());

        modules.addAll(getAdditionalModules());

        Bootstrap app = new Bootstrap(modules.build());

        try {
            Injector injector = app.initialize();

            log.info("Trino version: %s", injector.getInstance(NodeVersion.class).getVersion());
            logLocation(log, "Working directory", Paths.get("."));
            logLocation(log, "Etc directory", Paths.get("etc"));

            injector.getInstance(PluginManager.class).loadPlugins();

            injector.getInstance(StaticCatalogStore.class).loadCatalogs();

            // TODO: remove this huge hack
            updateConnectorIds(injector.getInstance(Announcer.class), injector.getInstance(CatalogManager.class));

            injector.getInstance(SessionPropertyDefaults.class).loadConfigurationManager();
            injector.getInstance(ResourceGroupManager.class).loadConfigurationManager();
            injector.getInstance(AccessControlManager.class).loadSystemAccessControl();
            injector.getInstance(optionalKey(PasswordAuthenticatorManager.class))
                    .ifPresent(PasswordAuthenticatorManager::loadPasswordAuthenticator);
            injector.getInstance(EventListenerManager.class).loadEventListeners();
            injector.getInstance(GroupProviderManager.class).loadConfiguredGroupProvider();
            injector.getInstance(ExchangeManagerRegistry.class).loadExchangeManager();
            injector.getInstance(CertificateAuthenticatorManager.class).loadCertificateAuthenticator();
            injector.getInstance(optionalKey(HeaderAuthenticatorManager.class))
                    .ifPresent(HeaderAuthenticatorManager::loadHeaderAuthenticator);

            injector.getInstance(Announcer.class).start();

            injector.getInstance(StartupStatus.class).startupComplete();

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
            System.exit(1);
        }
        catch (Throwable e) {
            log.error(e);
            System.exit(1);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> Key<Optional<T>> optionalKey(Class<T> type)
    {
        return Key.get((TypeLiteral<Optional<T>>) TypeLiteral.get(Types.newParameterizedType(Optional.class, type)));
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

    private static void updateConnectorIds(Announcer announcer, CatalogManager metadata)
    {
        // get existing announcement
        ServiceAnnouncement announcement = getTrinoAnnouncement(announcer.getServiceAnnouncements());

        // automatically build connectorIds if not configured
        Set<String> connectorIds = metadata.getCatalogs().stream()
                .map(Catalog::getConnectorCatalogName)
                .map(Object::toString)
                .collect(toImmutableSet());

        // build announcement with updated sources
        ServiceAnnouncementBuilder builder = serviceAnnouncement(announcement.getType());
        builder.addProperties(announcement.getProperties());
        builder.addProperty("connectorIds", Joiner.on(',').join(connectorIds));

        // update announcement
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(builder.build());
    }

    private static ServiceAnnouncement getTrinoAnnouncement(Set<ServiceAnnouncement> announcements)
    {
        for (ServiceAnnouncement announcement : announcements) {
            if (announcement.getType().equals("trino")) {
                return announcement;
            }
        }
        throw new IllegalArgumentException("Trino announcement not found: " + announcements);
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
}
