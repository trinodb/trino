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
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.server.PluginManager.PluginsProvider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.base.ClosingBinder.closingBinder;

public final class DevelopmentServer
        extends Server
{
    private DevelopmentServer() {}

    @Override
    protected Iterable<? extends Module> getAdditionalModules()
    {
        try {
            Path pluginPath = Files.createTempDirectory("plugins");

            return ImmutableList.of(binder -> {
                newOptionalBinder(binder, PluginsProvider.class).setBinding()
                        .to(DevelopmentPluginsProvider.class).in(Scopes.SINGLETON);
                configBinder(binder).bindConfig(DevelopmentLoaderConfig.class);

                // Use a temporary directory to satisfy configuration validation
                configBinder(binder).bindConfigDefaults(ServerPluginsProviderConfig.class, config ->
                        config.setInstalledPluginsDirs(ImmutableList.of(pluginPath)));

                closingBinder(binder).registerCloseable(() -> Files.deleteIfExists(pluginPath));
            });
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void main(String[] args)
    {
        new DevelopmentServer().start("dev");
    }
}
