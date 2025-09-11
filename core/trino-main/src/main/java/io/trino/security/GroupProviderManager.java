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
package io.trino.security;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.log.Logger;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.security.GroupProvider;
import io.trino.spi.security.GroupProviderFactory;
import io.trino.util.Case;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.trino.util.Case.KEEP;
import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class GroupProviderManager
        implements GroupProvider
{
    private static final Logger log = Logger.get(GroupProviderManager.class);
    private static final File GROUP_PROVIDER_CONFIGURATION = new File("etc/group-provider.properties");
    private static final String GROUP_PROVIDER_PROPERTY_NAME = "group-provider.name";
    private static final String GROUP_PROVIDER_PROPERTY_GROUP_CASE = "group-provider.group-case";
    private final Map<String, GroupProviderFactory> groupProviderFactories = new ConcurrentHashMap<>();
    private final AtomicReference<Optional<GroupProvider>> configuredGroupProvider = new AtomicReference<>(Optional.empty());
    private final SecretsResolver secretsResolver;
    private Case groupCase = KEEP;

    @Inject
    public GroupProviderManager(SecretsResolver secretsResolver)
    {
        this.secretsResolver = requireNonNull(secretsResolver, "secretsResolver is null");
    }

    public void addGroupProviderFactory(GroupProviderFactory groupProviderFactory)
    {
        requireNonNull(groupProviderFactory, "groupProviderFactory is null");

        if (groupProviderFactories.putIfAbsent(groupProviderFactory.getName(), groupProviderFactory) != null) {
            throw new IllegalArgumentException(format("Group provider '%s' is already registered", groupProviderFactory.getName()));
        }
    }

    public void loadConfiguredGroupProvider()
            throws IOException
    {
        loadConfiguredGroupProvider(GROUP_PROVIDER_CONFIGURATION);
    }

    @VisibleForTesting
    void loadConfiguredGroupProvider(File groupProviderFile)
            throws IOException
    {
        if (configuredGroupProvider.get().isPresent() || !groupProviderFile.exists()) {
            return;
        }
        Map<String, String> properties = new HashMap<>(loadPropertiesFrom(groupProviderFile.getPath()));

        String groupProviderName = properties.remove(GROUP_PROVIDER_PROPERTY_NAME);
        checkArgument(!isNullOrEmpty(groupProviderName),
                "Group provider configuration %s does not contain %s", groupProviderFile.getAbsoluteFile(), GROUP_PROVIDER_PROPERTY_NAME);

        String groupCase = properties.remove(GROUP_PROVIDER_PROPERTY_GROUP_CASE);
        if (groupCase != null) {
            this.groupCase = stream(Case.values())
                    .map(Case::toString)
                    .filter(groupCase::equalsIgnoreCase)
                    .map(Case::valueOf)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(format("Group provider configuration %s does not contain valid %s. Expected one of: %s",
                            groupProviderFile.getAbsoluteFile(),
                            GROUP_PROVIDER_PROPERTY_GROUP_CASE,
                            stream(Case.values()).map(Case::toString).collect(joining(", ", "[", "]")))));
        }

        setConfiguredGroupProvider(groupProviderName, properties);
    }

    @VisibleForTesting
    protected void setConfiguredGroupProvider(String name, Map<String, String> properties)
    {
        requireNonNull(name, "name is null");
        requireNonNull(properties, "properties is null");

        log.info("-- Loading group provider %s --", name);

        GroupProviderFactory factory = groupProviderFactories.get(name);
        checkState(factory != null, "Group provider %s is not registered", name);

        GroupProvider groupProvider;
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            groupProvider = factory.create(ImmutableMap.copyOf(secretsResolver.getResolvedConfiguration(properties)));
        }

        setConfiguredGroupProvider(groupProvider);

        log.info("-- Loaded group provider %s --", name);
    }

    @VisibleForTesting
    protected void setConfiguredGroupProvider(GroupProvider groupProvider)
    {
        checkState(configuredGroupProvider.compareAndSet(Optional.empty(), Optional.of(groupProvider)), "groupProvider is already set");
    }

    @Override
    public Set<String> getGroups(String user)
    {
        requireNonNull(user, "user is null");
        return configuredGroupProvider.get()
                .map(provider -> provider.getGroups(user))
                .map(groups -> groups.stream().map(this.groupCase::transform).collect(toImmutableSet()))
                .orElse(ImmutableSet.of());
    }
}
