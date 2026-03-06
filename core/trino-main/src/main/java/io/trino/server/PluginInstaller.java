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
import io.airlift.units.Duration;
import io.trino.spi.Plugin;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public interface PluginInstaller
{
    void loadPlugins();

    InstalledFeatures installPlugin(Plugin plugin);

    record InstalledFeatures(Class<? extends Plugin> pluginClass, Duration loadingTime, List<InstalledFeature> features)
    {
        public InstalledFeatures
        {
            requireNonNull(pluginClass, "pluginClass is null");
            requireNonNull(loadingTime, "loadingTime is null");
            features = ImmutableList.copyOf(requireNonNull(features, "features is null"));
        }

        public boolean isEmpty()
        {
            return features.isEmpty();
        }

        public List<String> names(Feature feature)
        {
            return features.stream()
                    .filter(value -> value.type == feature)
                    .map(InstalledFeature::name)
                    .collect(toImmutableList());
        }

        public static Builder builder(Class<? extends Plugin> pluginClass)
        {
            return new Builder(pluginClass);
        }

        public static class Builder
        {
            private final Class<? extends Plugin> pluginClass;
            private Duration loadingTime = Duration.succinctNanos(0);
            private final ImmutableList.Builder<InstalledFeature> features = ImmutableList.builder();

            private Builder(Class<? extends Plugin> pluginClass)
            {
                this.pluginClass = requireNonNull(pluginClass, "pluginClass is null");
            }

            public Builder withFeature(Feature feature, String name)
            {
                features.add(new InstalledFeature(feature, name));
                return this;
            }

            public Builder withLoadingTime(Duration duration)
            {
                this.loadingTime = duration;
                return this;
            }

            public InstalledFeatures build()
            {
                return new InstalledFeatures(pluginClass, loadingTime, features.build());
            }
        }
    }

    record InstalledFeature(Feature type, String name)
    {
        public InstalledFeature
        {
            requireNonNull(type, "type is null");
            requireNonNull(name, "name is null");
        }
    }

    enum Feature
    {
        ACCESS_CONTROL("access controls"),
        BLOCK_ENCODING("block encodings"),
        CATALOG_STORE("catalog stores"),
        CERTIFICATE_AUTHENTICATOR("certificate authenticators"),
        CONNECTOR("connectors"),
        EVENT_LISTENER("event listeners"),
        EXCHANGE_MANAGER("exchange managers"),
        FUNCTION("functions"),
        GROUP_PROVIDER("group providers"),
        HEADER_AUTHENTICATOR("header authenticators"),
        LANGUAGE_FUNCTION("language functions engines"),
        PARAMETRIC_TYPE("parametric types"),
        PASSWORD_AUTHENTICATOR("password authenticators"),
        RESOURCE_GROUP_CONFIGURATION_MANAGER("resource group managers"),
        SESSION_PROPERTY_CONFIGURATION_MANAGER("session property managers"),
        SPOOLING_MANAGER("spooling managers"),
        TYPE("types"),
        /**/;

        private final String description;

        Feature(String description)
        {
            this.description = requireNonNull(description, "description is null");
        }

        public String getDescription()
        {
            return description;
        }
    }
}
