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
import com.google.common.collect.Ordering;
import io.airlift.resolver.ArtifactResolver;
import io.airlift.resolver.DefaultArtifact;
import io.trino.server.PluginManager.PluginsProvider;
import org.sonatype.aether.artifact.Artifact;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.server.PluginDiscovery.discoverPlugins;
import static io.trino.server.PluginDiscovery.writePluginServices;
import static io.trino.util.Executors.executeUntilFailure;
import static java.util.Objects.requireNonNull;

public class DevelopmentPluginsProvider
        implements PluginsProvider
{
    private final ArtifactResolver resolver;
    private final List<String> plugins;
    private final Executor executor;

    @Inject
    public DevelopmentPluginsProvider(DevelopmentLoaderConfig config, @ForStartup Executor executor)
    {
        this.resolver = new ArtifactResolver(config.getMavenLocalRepository(), config.getMavenRemoteRepository());
        this.plugins = ImmutableList.copyOf(config.getPlugins());
        this.executor = requireNonNull(executor, "executor is null");
    }

    @Override
    public void loadPlugins(Loader loader, ClassLoaderFactory createClassLoader)
    {
        executeUntilFailure(
                executor,
                plugins.stream()
                        .map(plugin -> (Callable<?>) () -> {
                            loader.load(plugin, () -> buildClassLoader(plugin, createClassLoader));
                            return null;
                        })
                        .collect(toImmutableList()));
    }

    private PluginClassLoader buildClassLoader(String plugin, ClassLoaderFactory classLoaderFactory)
    {
        try {
            return doBuildClassLoader(plugin, urls -> classLoaderFactory.create(plugin, urls));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private PluginClassLoader doBuildClassLoader(String plugin, Function<List<URL>, PluginClassLoader> classLoaderFactory)
            throws IOException
    {
        File file = new File(plugin);
        if (file.isFile() && (file.getName().equals("pom.xml") || file.getName().endsWith(".pom"))) {
            return buildClassLoaderFromPom(file, classLoaderFactory);
        }
        return buildClassLoaderFromCoordinates(plugin, classLoaderFactory);
    }

    private PluginClassLoader buildClassLoaderFromPom(File pomFile, Function<List<URL>, PluginClassLoader> classLoaderFactory)
            throws IOException
    {
        List<Artifact> artifacts = resolver.resolvePom(pomFile);
        PluginClassLoader classLoader = createClassLoader(artifacts, classLoaderFactory);

        Artifact artifact = artifacts.get(0);
        Set<String> plugins = discoverPlugins(artifact, classLoader);
        if (!plugins.isEmpty()) {
            File root = new File(artifact.getFile().getParentFile().getCanonicalFile(), "plugin-discovery");
            writePluginServices(plugins, root);
            classLoader = classLoader.withUrl(root.toURI().toURL());
        }

        return classLoader;
    }

    private PluginClassLoader buildClassLoaderFromCoordinates(String coordinates, Function<List<URL>, PluginClassLoader> classLoaderFactory)
            throws IOException
    {
        Artifact rootArtifact = new DefaultArtifact(coordinates);
        List<Artifact> artifacts = resolver.resolveArtifacts(rootArtifact);
        return createClassLoader(artifacts, classLoaderFactory);
    }

    private static PluginClassLoader createClassLoader(List<Artifact> artifacts, Function<List<URL>, PluginClassLoader> classLoaderFactory)
            throws IOException
    {
        List<URL> urls = new ArrayList<>();
        for (Artifact artifact : sortedArtifacts(artifacts)) {
            if (artifact.getFile() == null) {
                throw new RuntimeException("Could not resolve artifact: " + artifact);
            }
            File file = artifact.getFile().getCanonicalFile();
            urls.add(file.toURI().toURL());
        }
        return classLoaderFactory.apply(urls);
    }

    private static List<Artifact> sortedArtifacts(List<Artifact> artifacts)
    {
        List<Artifact> list = new ArrayList<>(artifacts);
        list.sort(Ordering.natural().nullsLast().onResultOf(Artifact::getFile));
        return list;
    }
}
