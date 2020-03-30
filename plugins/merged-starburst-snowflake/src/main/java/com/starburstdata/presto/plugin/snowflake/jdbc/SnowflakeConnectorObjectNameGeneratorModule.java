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
package com.starburstdata.presto.plugin.snowflake.jdbc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.airlift.configuration.Config;
import org.weakref.jmx.ObjectNameBuilder;
import org.weakref.jmx.ObjectNameGenerator;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.MoreObjects.firstNonNull;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

// TODO: move to presto-base-jdbc and use in other connectors
public class SnowflakeConnectorObjectNameGeneratorModule
        implements Module
{
    private static final Set<String> CONNECTOR_PACKAGE_NAMES = ImmutableSet.of("io.prestosql.plugin", "com.starburstdata.presto.plugin.snowflake");
    private static final String DEFAULT_DOMAIN_BASE = "presto.plugin.snowflake";

    private final String catalogName;

    public SnowflakeConnectorObjectNameGeneratorModule(String catalogName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(ConnectorObjectNameGeneratorConfig.class);
    }

    @Provides
    ObjectNameGenerator createPrefixObjectNameGenerator(ConnectorObjectNameGeneratorConfig config)
    {
        String domainBase = firstNonNull(config.getDomainBase(), DEFAULT_DOMAIN_BASE);
        return new ConnectorObjectNameGenerator(domainBase, catalogName);
    }

    public static class ConnectorObjectNameGeneratorConfig
    {
        private String domainBase;

        public String getDomainBase()
        {
            return domainBase;
        }

        @Config("jmx.base-name")
        public ConnectorObjectNameGeneratorConfig setDomainBase(String domainBase)
        {
            this.domainBase = domainBase;
            return this;
        }
    }

    public static final class ConnectorObjectNameGenerator
            implements ObjectNameGenerator
    {
        private final String domainBase;
        private final String catalogName;

        public ConnectorObjectNameGenerator(String domainBase, String catalogName)
        {
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
                            .build())
                    .build();
        }

        @Override
        public String generatedNameOf(Class<?> type, Map<String, String> properties)
        {
            return new ObjectNameBuilder(toDomain(type))
                    .withProperties(ImmutableMap.<String, String>builder()
                            .putAll(properties)
                            .put("catalog", catalogName)
                            .build())
                    .build();
        }

        private String toDomain(Class<?> type)
        {
            String domain = type.getPackage().getName();
            for (String packageName : CONNECTOR_PACKAGE_NAMES) {
                if (domain.startsWith(packageName)) {
                    return domainBase + domain.substring(packageName.length());
                }
            }
            return domain;
        }
    }
}
