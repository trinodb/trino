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
package io.trino.plugin.base.security;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.ConfigurationException;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.sun.net.httpserver.HttpServer;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.units.Duration;
import io.trino.spi.QueryId;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemSecurityContext;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestFileBasedSystemAccessControlModule
{
    private static final Identity alice = Identity.forUser("alice").withGroups(ImmutableSet.of("staff")).build();
    private static final CatalogSchemaTableName aliceView = new CatalogSchemaTableName("alice-catalog", "schema", "view");
    private static final Optional<QueryId> queryId = Optional.empty();

    private static final SystemSecurityContext ALICE = new SystemSecurityContext(alice, queryId);

    @Test
    public void noHttpClientConfigForLocalFile()
    {
        Injector injector = initModule(ImmutableMap.of(
                "security.config-file", getResourcePath("file-based-system-catalog.json")));
        SystemAccessControl accessControl = injector.getInstance(SystemAccessControl.class);
        accessControl.checkCanCreateView(ALICE, aliceView);
        Supplier<FileBasedSystemAccessControlRules> provider = injector.getInstance(
                Key.get(new TypeLiteral<Supplier<FileBasedSystemAccessControlRules>>() {}));
        assertTrue(provider instanceof LocalFileAccessControlRulesProvider);
        assertThatThrownBy(() -> injector.getInstance(Key.get(HttpClient.class, ForAccessControlRules.class)))
                .isInstanceOf(ConfigurationException.class).hasMessageContaining("is not explicitly bound");
        assertThatThrownBy(() -> injector.getInstance(Key.get(HttpClientConfig.class)))
                .isInstanceOf(ConfigurationException.class).hasMessageContaining("is not explicitly bound");
    }

    @Test
    public void httpClientConfigForRestFile()
            throws IOException
    {
        byte[] response = Files.readAllBytes(Path.of(
            getResourcePath("file-based-system-catalog.json")));

        try (TestingHttpServer testingHttpServer = new TestingHttpServer(2002, response)) {
            Injector injector = initModule(ImmutableMap.of(
                    "security.config-file", "http://localhost:2002/test",
                    "access-control.http-client.connect-timeout", "123ms"));
            SystemAccessControl accessControl = injector.getInstance(SystemAccessControl.class);
            accessControl.checkCanCreateView(ALICE, aliceView);
            Supplier<FileBasedSystemAccessControlRules> provider =
                    injector.getInstance(Key.get(new TypeLiteral<Supplier<FileBasedSystemAccessControlRules>>() {}));
            assertTrue(provider instanceof RestFileBasedSystemAccessControlRulesProvider);
            HttpClientConfig httpConfig = injector.getInstance(Key.get(HttpClientConfig.class));
            assertEquals(httpConfig.getConnectTimeout(), new Duration(123, TimeUnit.MILLISECONDS));
        }
        catch (Exception ex) {
            throw new IllegalStateException("Could not close testingHttpServer!", ex);
        }
    }

    private static Injector initModule(Map<String, String> config)
    {
        requireNonNull(config, "config is null");

        FileBasedSystemAccessControlModule module = new FileBasedSystemAccessControlModule(config);
        Bootstrap bootstrap = new Bootstrap(module);
        Injector injector = bootstrap
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector;
    }

    private String getResourcePath(String resourceName)
    {
        return requireNonNull(this.getClass().getClassLoader().getResource(resourceName), "Resource does not exist: " + resourceName).getPath();
    }

    private static class TestingHttpServer
            implements AutoCloseable
    {
        private final HttpServer httpServer;

        TestingHttpServer(int port, byte[] response)
                throws IOException
        {
            httpServer = HttpServer.create(new InetSocketAddress(port), 0);
            httpServer.createContext("/test", exchange ->
            {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.length);
                exchange.getResponseBody().write(response);
                exchange.close();
            });
            httpServer.start();
        }

        @Override
        public void close()
                throws Exception
        {
            httpServer.stop(0);
        }
    }
}
