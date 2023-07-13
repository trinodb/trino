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
package io.trino.plugin.resourcegroups;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.units.Duration;
import io.trino.spi.memory.ClusterMemoryPoolManager;
import io.trino.spi.resourcegroups.ResourceGroup;
import io.trino.spi.resourcegroups.SelectionContext;
import io.trino.spi.resourcegroups.SelectionCriteria;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FileResourceGroupConfigurationManager
        extends AbstractResourceConfigurationManager
{
    private static final JsonCodec<ManagerSpec> CODEC = new JsonCodecFactory(
            () -> new ObjectMapperProvider().get().enable(FAIL_ON_UNKNOWN_PROPERTIES))
            .jsonCodec(ManagerSpec.class);

    private final List<ResourceGroupSpec> rootGroups;
    private final List<ResourceGroupSelector> selectors;
    private final Optional<Duration> cpuQuotaPeriod;

    @Inject
    public FileResourceGroupConfigurationManager(ClusterMemoryPoolManager memoryPoolManager, FileResourceGroupConfig config)
    {
        this(memoryPoolManager, parseManagerSpec(config));
    }

    @VisibleForTesting
    FileResourceGroupConfigurationManager(ClusterMemoryPoolManager memoryPoolManager, ManagerSpec managerSpec)
    {
        super(memoryPoolManager);
        requireNonNull(managerSpec, "managerSpec is null");

        this.rootGroups = ImmutableList.copyOf(managerSpec.getRootGroups());
        this.cpuQuotaPeriod = managerSpec.getCpuQuotaPeriod();
        validateRootGroups(managerSpec);
        this.selectors = buildSelectors(managerSpec);
    }

    @VisibleForTesting
    static ManagerSpec parseManagerSpec(FileResourceGroupConfig config)
    {
        ManagerSpec managerSpec;
        try {
            managerSpec = CODEC.fromJson(Files.readAllBytes(Paths.get(config.getConfigFile())));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (IllegalArgumentException e) {
            Throwable cause = e.getCause();
            if (cause instanceof UnrecognizedPropertyException ex) {
                String message = format("Unknown property at line %s:%s: %s",
                        ex.getLocation().getLineNr(),
                        ex.getLocation().getColumnNr(),
                        ex.getPropertyName());
                throw new IllegalArgumentException(message, e);
            }
            if (cause instanceof JsonMappingException) {
                // remove the extra "through reference chain" message
                if (cause.getCause() != null) {
                    cause = cause.getCause();
                }
                throw new IllegalArgumentException(cause.getMessage(), e);
            }
            throw e;
        }
        return managerSpec;
    }

    @Override
    protected Optional<Duration> getCpuQuotaPeriod()
    {
        return cpuQuotaPeriod;
    }

    @Override
    protected List<ResourceGroupSpec> getRootGroups()
    {
        return rootGroups;
    }

    @Override
    public void configure(ResourceGroup group, SelectionContext<ResourceGroupIdTemplate> context)
    {
        configureGroup(group, getMatchingSpec(group, context));
    }

    @Override
    public Optional<SelectionContext<ResourceGroupIdTemplate>> match(SelectionCriteria criteria)
    {
        return selectors.stream()
                .map(s -> s.match(criteria))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }
}
