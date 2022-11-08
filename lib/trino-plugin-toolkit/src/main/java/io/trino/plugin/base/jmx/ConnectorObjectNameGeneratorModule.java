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
package io.trino.plugin.base.jmx;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.trino.plugin.base.CatalogName;
import org.weakref.jmx.ObjectNameBuilder;
import org.weakref.jmx.ObjectNameGenerator;

import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class ConnectorObjectNameGeneratorModule
        implements Module
{
    private final String packageName;
    private final String defaultDomainBase;

    public ConnectorObjectNameGeneratorModule(String packageName, String defaultDomainBase)
    {
        this.packageName = requireNonNull(packageName, "packageName is null");
        this.defaultDomainBase = requireNonNull(defaultDomainBase, "defaultDomainBase is null");
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(ObjectNameGeneratorConfig.class);
    }

    @Provides
    ObjectNameGenerator createPrefixObjectNameGenerator(CatalogName catalogName, ObjectNameGeneratorConfig config)
    {
        String domainBase = firstNonNull(config.getDomainBase(), defaultDomainBase);
        return new ConnectorObjectNameGenerator(packageName, domainBase, catalogName.toString());
    }

    public static final class ConnectorObjectNameGenerator
            implements ObjectNameGenerator
    {
        private final String packageName;
        private final String domainBase;
        private final String catalogName;

        public ConnectorObjectNameGenerator(String packageName, String domainBase, String catalogName)
        {
            this.packageName = packageName;
            this.domainBase = domainBase;
            this.catalogName = catalogName;
        }

        @Override
        public String generatedNameOf(Class<?> type)
        {
            return new ObjectNameBuilder(toDomain(type))
                    .withProperties(ImmutableMap.<String, String>builder()
                            .put("type", type.getSimpleName())
                            .put("name", catalogName)
                            .buildOrThrow())
                    .build();
        }

        @Override
        public String generatedNameOf(Class<?> type, Map<String, String> properties)
        {
            return new ObjectNameBuilder(toDomain(type))
                    .withProperties(ImmutableMap.<String, String>builder()
                            .putAll(properties)
                            .put("catalog", catalogName)
                            .buildOrThrow())
                    .build();
        }

        private String toDomain(Class<?> type)
        {
            String domain = type.getPackage().getName();
            if (domain.startsWith(packageName)) {
                domain = domainBase + domain.substring(packageName.length());
            }
            return domain;
        }
    }
}
