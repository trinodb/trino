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
import io.airlift.http.server.HttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.JmxModule;
import io.airlift.json.JsonModule;
import io.airlift.log.LogJmxModule;
import io.airlift.log.Logger;
import io.airlift.log.TerminalColors;
import io.airlift.node.NodeModule;
import io.airlift.openmetrics.JmxOpenMetricsModule;
import io.airlift.tracing.TracingModule;
import io.airlift.units.Duration;
import io.trino.connector.CatalogManagerModule;
import io.trino.connector.CatalogStoreManager;
import io.trino.connector.ConnectorServicesProvider;
import io.trino.eventlistener.EventListenerManager;
import io.trino.eventlistener.EventListenerModule;
import io.trino.exchange.ExchangeManagerModule;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.resourcegroups.ResourceGroupManager;
import io.trino.execution.warnings.WarningCollectorModule;
import io.trino.node.Announcer;
import io.trino.node.NodeManagerModule;
import io.trino.security.AccessControlManager;
import io.trino.security.AccessControlModule;
import io.trino.security.GroupProviderManager;
import io.trino.server.protocol.spooling.SpoolingManagerRegistry;
import io.trino.server.security.CertificateAuthenticatorManager;
import io.trino.server.security.HeaderAuthenticatorManager;
import io.trino.server.security.PasswordAuthenticatorManager;
import io.trino.server.security.ServerSecurityModule;
import io.trino.server.security.oauth2.OAuth2Client;
import io.trino.transaction.TransactionManagerModule;
import io.trino.util.EmbedVersion;
import org.weakref.jmx.guice.MBeanModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.google.common.base.StandardSystemProperty.JAVA_VERSION;
import static io.airlift.log.TerminalColors.Color.CYAN;
import static io.airlift.log.TerminalColors.Color.PURPLE;
import static io.airlift.log.TerminalColors.Color.YELLOW;
import static io.trino.server.TrinoSystemRequirements.verifySystemRequirements;
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
        long startTime = System.nanoTime();

        // Trino server behavior does not depend on locale settings.
        // Use en_US as this is what Trino is tested with.
        Locale.setDefault(Locale.US);
        verifySystemRequirements();

        Logger log = Logger.get(Server.class);
        displayBanner(log, trinoVersion);

        List<Module> modules = ImmutableList.<Module>builder()
                .add(new AccessControlModule())
                .add(new CatalogManagerModule())
                .add(new EventListenerModule())
                .add(new ExchangeManagerModule())
                .add(new HttpServerModule())
                .add(new JaxrsModule())
                .add(new JmxModule())
                .add(new JmxOpenMetricsModule())
                .add(new JsonModule())
                .add(new LogJmxModule())
                .add(new MBeanModule())
                .add(new NodeManagerModule(trinoVersion))
                .add(new NodeModule())
                .add(new NodeStateManagerModule())
                .add(new PrefixObjectNameGeneratorModule("io.trino"))
                .add(new ServerMainModule(trinoVersion))
                .add(new ServerSecurityModule())
                .add(new TracingModule("trino", trinoVersion))
                .add(new TransactionManagerModule())
                .add(new WarningCollectorModule())
                .addAll(getAdditionalModules())
                .build();

        Bootstrap app = new Bootstrap("io.trino.bootstrap.engine", modules)
                .loadSecretsPlugins();

        try {
            Injector injector = app.initialize();

            log.info("Zstandard native compression: %s", formatEnabled(ZstdNativeCompressor.isEnabled()));
            log.info("Lz4 native compression: %s", formatEnabled(Lz4NativeCompressor.isEnabled()));
            log.info("Snappy native compression: %s", formatEnabled(SnappyNativeCompressor.isEnabled()));

            injector.getInstance(PluginInstaller.class).loadPlugins();

            var catalogStoreManager = injector.getInstance(Key.get(new TypeLiteral<Optional<CatalogStoreManager>>() {}));
            catalogStoreManager.ifPresent(CatalogStoreManager::loadConfiguredCatalogStore);

            ConnectorServicesProvider connectorServicesProvider = injector.getInstance(ConnectorServicesProvider.class);
            connectorServicesProvider.loadInitialCatalogs();

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

    private static String resolveLocation(Path path)
    {
        if (!Files.exists(path, NOFOLLOW_LINKS)) {
            return "[does not exist]";
        }
        try {
            return path.toAbsolutePath().toRealPath().toString();
        }
        catch (IOException e) {
            return "[not accessible]";
        }
    }

    private static String formatEnabled(boolean flag)
    {
        return flag ? "enabled" : "disabled";
    }

    private static void displayBanner(Logger log, String trinoVersion)
    {
        TerminalColors colors = new TerminalColors(true);
        String banner = "\n"
                + "             " + colors.colored("Trino version:     ", CYAN) + colors.colored(trinoVersion, YELLOW) + "\n"
                + colors.colored("   (\\(\\", PURPLE) + "      " + colors.colored("Java version:      ", CYAN) + colors.colored(JAVA_VERSION.value(), YELLOW) + "\n"
                + colors.colored("   ( -.-)", PURPLE) + "    " + colors.colored("Working directory: ", CYAN) + colors.colored(resolveLocation(Path.of(".")), YELLOW) + "\n"
                + colors.colored("  o_(\")(\")", PURPLE) + "   " + colors.colored("Etc directory:     ", CYAN) + colors.colored(resolveLocation(Path.of("etc")), YELLOW) + "\n";
        log.info("%s", banner);
    }
}
