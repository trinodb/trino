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
package io.trino.execution.admission;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.spi.admission.AdmissionPolicy;
import io.trino.spi.admission.AdmissionPolicyFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Guice wiring for the {@link AdmissionPolicy} SPI.
 *
 * <p>Owns the {@code Multibinder<AdmissionPolicyFactory>} that holds the bundled
 * default factory ({@link MinWorkersAdmissionPolicy.Factory}) and any
 * plugin-supplied factories registered by {@code PluginManager}. Provides the
 * single bound {@link AdmissionPolicy} as a Guice singleton, resolved at
 * provision time from {@link AdmissionPolicyConfig}.
 *
 * <p>All resolution failures (unknown name, duplicate factory names, factory
 * returning {@code null}, factory throwing) are surfaced as runtime exceptions
 * during Guice provision so the coordinator fails before the HTTP server
 * accepts traffic.
 */
public class AdmissionPolicyModule
        extends AbstractModule
{
    private static final Logger log = Logger.get(AdmissionPolicyModule.class);

    private static final File CONFIG_FILE = new File("etc/admission-policy.properties");

    @Override
    protected void configure()
    {
        Binder binder = binder();
        configBinder(binder).bindConfig(AdmissionPolicyConfig.class);

        // The bundled default factory is registered directly via the multibinder.
        // Plugin-supplied factories are registered at plugin-load time through
        // AdmissionPolicyManager (populated by PluginManager). The @Provides
        // method below merges both sources and validates uniqueness across them.
        newSetBinder(binder, AdmissionPolicyFactory.class)
                .addBinding()
                .to(MinWorkersAdmissionPolicy.Factory.class)
                .in(Scopes.SINGLETON);
        binder.bind(AdmissionPolicyManager.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public AdmissionPolicy createAdmissionPolicy(
            Set<AdmissionPolicyFactory> defaultFactories,
            AdmissionPolicyManager pluginFactories,
            AdmissionPolicyConfig config)
    {
        requireNonNull(defaultFactories, "defaultFactories is null");
        requireNonNull(pluginFactories, "pluginFactories is null");
        requireNonNull(config, "config is null");

        ImmutableList.Builder<AdmissionPolicyFactory> merged = ImmutableList.builder();
        merged.addAll(defaultFactories);
        merged.addAll(pluginFactories.getAdmissionPolicyFactories());
        Map<String, AdmissionPolicyFactory> byName = indexUnique(merged.build());

        String requestedName = config.getName();
        AdmissionPolicyFactory factory = byName.get(requestedName);
        if (factory == null) {
            throw new RuntimeException(format(
                    "No admission policy factory registered for name '%s'; registered: %s",
                    requestedName,
                    byName.keySet()));
        }

        Map<String, String> properties = loadProperties();

        log.info("-- Loading admission policy '%s' from %s --", requestedName, factory.getClass().getName());

        AdmissionPolicy policy;
        try {
            policy = factory.create(ImmutableMap.copyOf(properties));
        }
        catch (Throwable t) {
            throw new RuntimeException(format(
                    "Admission policy factory '%s' (%s) threw while creating policy",
                    requestedName,
                    factory.getClass().getName()), t);
        }
        if (policy == null) {
            throw new RuntimeException(format(
                    "Admission policy factory '%s' (%s) returned null",
                    requestedName,
                    factory.getClass().getName()));
        }

        log.info("-- Loaded admission policy '%s' --", requestedName);
        return policy;
    }

    /**
     * Validates that no two registered factories share a name (case-sensitive)
     * and returns a map keyed by {@code getName()}.
     */
    private static Map<String, AdmissionPolicyFactory> indexUnique(List<AdmissionPolicyFactory> factories)
    {
        Map<String, AdmissionPolicyFactory> byName = new LinkedHashMap<>();
        for (AdmissionPolicyFactory factory : factories) {
            String name = factory.getName();
            if (name == null || name.isEmpty()) {
                throw new RuntimeException(format(
                        "Admission policy factory %s returned a null or empty name",
                        factory.getClass().getName()));
            }
            AdmissionPolicyFactory existing = byName.putIfAbsent(name, factory);
            if (existing != null) {
                throw new RuntimeException(format(
                        "Duplicate admission policy factory name '%s' from %s and %s",
                        name,
                        existing.getClass().getName(),
                        factory.getClass().getName()));
            }
        }
        return byName;
    }

    /**
     * Loads {@code etc/admission-policy.properties} into a mutable map. Returns
     * an empty map when the file is absent so vanilla operators are not required
     * to create it.
     */
    private static Map<String, String> loadProperties()
    {
        File configFile = CONFIG_FILE.getAbsoluteFile();
        if (!configFile.exists()) {
            return new HashMap<>();
        }
        try {
            return new HashMap<>(loadPropertiesFrom(configFile.getPath()));
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read admission policy configuration file: " + configFile, e);
        }
    }
}
