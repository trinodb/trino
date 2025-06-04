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
import io.airlift.configuration.Config;
import io.airlift.resolver.ArtifactResolver;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public class DevelopmentLoaderConfig
{
    private List<String> plugins = ImmutableList.of();
    private String mavenLocalRepository = ArtifactResolver.USER_LOCAL_REPO;
    private List<String> mavenRemoteRepository = ImmutableList.of(ArtifactResolver.MAVEN_CENTRAL_URI);

    public List<String> getPlugins()
    {
        return plugins;
    }

    @Config("plugin.bundles")
    public DevelopmentLoaderConfig setPlugins(List<String> plugins)
    {
        this.plugins = ImmutableList.copyOf(plugins);
        return this;
    }

    @NotNull
    public String getMavenLocalRepository()
    {
        return mavenLocalRepository;
    }

    @Config("maven.repo.local")
    public DevelopmentLoaderConfig setMavenLocalRepository(String mavenLocalRepository)
    {
        this.mavenLocalRepository = mavenLocalRepository;
        return this;
    }

    @NotNull
    public List<String> getMavenRemoteRepository()
    {
        return mavenRemoteRepository;
    }

    @Config("maven.repo.remote")
    public DevelopmentLoaderConfig setMavenRemoteRepository(List<String> mavenRemoteRepository)
    {
        this.mavenRemoteRepository = ImmutableList.copyOf(mavenRemoteRepository);
        return this;
    }
}
