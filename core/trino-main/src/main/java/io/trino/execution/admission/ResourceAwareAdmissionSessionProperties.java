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
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.SystemSessionPropertiesProvider;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.plugin.base.session.PropertyMetadataUtil.dataSizeProperty;
import static io.trino.plugin.base.session.PropertyMetadataUtil.durationProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;

/**
 * Session-level overrides for resource-aware query admission thresholds, registered as a
 * {@link SystemSessionPropertiesProvider}. Defaults are sourced from
 * {@link ResourceAwareAdmissionConfig}, so an operator who sets only the cluster-wide config gets
 * consistent behavior without anyone setting a session property.
 */
public class ResourceAwareAdmissionSessionProperties
        implements SystemSessionPropertiesProvider
{
    public static final String REQUIRED_FREE_CLUSTER_MEMORY = "required_free_cluster_memory";
    public static final String REQUIRED_FREE_CLUSTER_VCPU = "required_free_cluster_vcpu";
    public static final String QUERY_ADMISSION_MAX_WAIT = "query_admission_max_wait";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public ResourceAwareAdmissionSessionProperties(ResourceAwareAdmissionConfig config)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(dataSizeProperty(
                        REQUIRED_FREE_CLUSTER_MEMORY,
                        "Minimum cluster-wide free memory required before this query is admitted",
                        config.getRequiredFreeMemory(),
                        false))
                .add(integerProperty(
                        REQUIRED_FREE_CLUSTER_VCPU,
                        "Minimum cluster-wide free vCPU required before this query is admitted",
                        config.getRequiredFreeVcpu(),
                        false))
                .add(durationProperty(
                        QUERY_ADMISSION_MAX_WAIT,
                        "Maximum time this query is held waiting for capacity before failing",
                        config.getMaxWait(),
                        false))
                .build();
    }

    public static DataSize getRequiredFreeClusterMemory(Session session)
    {
        return session.getSystemProperty(REQUIRED_FREE_CLUSTER_MEMORY, DataSize.class);
    }

    public static int getRequiredFreeClusterVcpu(Session session)
    {
        return session.getSystemProperty(REQUIRED_FREE_CLUSTER_VCPU, Integer.class);
    }

    public static Duration getQueryAdmissionMaxWait(Session session)
    {
        return session.getSystemProperty(QUERY_ADMISSION_MAX_WAIT, Duration.class);
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}
