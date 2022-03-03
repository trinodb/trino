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
package io.trino.testing.containers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class Httpd
        extends BaseTestContainer
{
    private static final Logger log = Logger.get(Httpd.class);

    public static final String DEFAULT_IMAGE = "httpd:2.4.51";
    public static final int DEFAULT_LISTEN_PORT = 8888;

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builderForHttpProxy(int proxyPort)
    {
        return new Builder()
                .withHostName("localhost")
                .withListenPort(proxyPort)
                .addLoadModule("mpm_event_module", "modules/mod_mpm_event.so")
                .addLoadModule("authn_file_module", "modules/mod_authn_file.so")
                .addLoadModule("authn_core_module", "modules/mod_authn_core.so")
                .addLoadModule("authz_host_module", "modules/mod_authz_host.so")
                .addLoadModule("authz_groupfile_module", "modules/mod_authz_groupfile.so")
                .addLoadModule("authz_user_module", "modules/mod_authz_user.so")
                .addLoadModule("authz_core_module", "modules/mod_authz_core.so")
                .addLoadModule("access_compat_module", "modules/mod_access_compat.so")
                .addLoadModule("auth_basic_module", "modules/mod_auth_basic.so")
                .addLoadModule("reqtimeout_module", "modules/mod_reqtimeout.so")
                .addLoadModule("filter_module", "modules/mod_filter.so")
                .addLoadModule("mime_module", "modules/mod_mime.so")
                .addLoadModule("log_config_module", "modules/mod_log_config.so")
                .addLoadModule("env_module", "modules/mod_env.so")
                .addLoadModule("headers_module", "modules/mod_headers.so")
                .addLoadModule("setenvif_module", "modules/mod_setenvif.so")
                .addLoadModule("version_module", "modules/mod_version.so")
                .addLoadModule("proxy_module", "modules/mod_proxy.so")
                .addLoadModule("proxy_connect_module", "modules/mod_proxy_connect.so")
                .addLoadModule("proxy_http_module", "modules/mod_proxy_http.so")
                .addLoadModule("unixd_module", "modules/mod_unixd.so")
                .addLoadModule("status_module", "modules/mod_status.so")
                .addLoadModule("autoindex_module", "modules/mod_autoindex.so")
                .addLoadModule("dir_module", "modules/mod_dir.so")
                .addLoadModule("alias_module", "modules/mod_alias.so");
    }

    private final int listenPort;
    private final Map<String, String> loadModules;
    private final List<String> modulesConfiguration;

    private Httpd(
            String image,
            String hostName,
            int listenPort,
            Map<String, String> loadModules,
            List<String> modulesConfiguration,
            Map<String, String> envVars,
            Optional<Network> network,
            int retryLimit)
    {
        super(
                image,
                hostName,
                ImmutableSet.of(listenPort),
                ImmutableMap.of(),
                envVars,
                network,
                retryLimit);
        this.listenPort = listenPort;
        this.loadModules = requireNonNull(loadModules, "loadModules is null");
        this.modulesConfiguration = requireNonNull(modulesConfiguration, "modulesConfiguration is null");
        preareAndCopyHttpdConfiguration(hostName);
    }

    private void preareAndCopyHttpdConfiguration(String hostName)
    {
        try {
            Path httpConf = Files.createTempFile("httpd", ".conf");
            StringBuilder httpConfBody = new StringBuilder();
            httpConfBody.append("ServerRoot \"/usr/local/apache2\"\n\n");
            httpConfBody.append("ServerName " + hostName + "\n\n");
            httpConfBody.append("Listen " + listenPort + "\n\n");
            loadModules.entrySet().stream()
                    .map(entry -> "LoadModule " + entry.getKey() + " " + entry.getValue() + "\n")
                    .forEach(httpConfBody::append);
            httpConfBody.append("\n");
            modulesConfiguration.stream().map(conf -> conf + "\n\n").forEach(httpConfBody::append);
            Files.write(httpConf, httpConfBody.toString().getBytes(UTF_8));
            copyHostFileToContainer(httpConf.toString(), "/usr/local/apache2/conf/httpd.conf");
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to write httpd.conf for Httpd test container", e);
        }
    }

    @Override
    public void start()
    {
        super.start();
        log.info("Httpd container started. Listening on: " + listenPort);
    }

    public int getListenPort()
    {
        return getMappedHostAndPortForExposedPort(listenPort).getPort();
    }

    public static class Builder
            extends BaseTestContainer.Builder<Httpd.Builder, Httpd>
    {
        private int listenPort = DEFAULT_LISTEN_PORT;
        private ImmutableMap.Builder<String, String> loadModules = ImmutableMap.builder();
        private ImmutableList.Builder<String> modulesConfiguration = ImmutableList.builder();

        private Builder()
        {
            this.image = DEFAULT_IMAGE;
        }

        public Builder withListenPort(int listenPort)
        {
            this.listenPort = listenPort;
            return self;
        }

        public Builder addLoadModule(String moduleName, String moduleLib)
        {
            this.loadModules.put(moduleName, moduleLib);
            return self;
        }

        public Builder addModuleConfiguration(String moduleConfiguration)
        {
            this.modulesConfiguration.add(moduleConfiguration);
            return self;
        }

        @Override
        public Httpd build()
        {
            return new Httpd(image, hostName, listenPort, loadModules.buildOrThrow(), modulesConfiguration.build(), envVars, network, startupRetryLimit);
        }
    }
}
