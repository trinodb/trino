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
package io.trino.node;

import com.google.inject.Inject;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceType;
import io.trino.failuredetector.FailureDetector;
import io.trino.server.InternalCommunicationConfig;

import java.net.URI;
import java.util.Objects;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.not;

public class AirliftNodeInventory
        implements NodeInventory
{
    private final ServiceSelector serviceSelector;
    private final FailureDetector failureDetector;
    private final boolean httpsRequired;

    @Inject
    public AirliftNodeInventory(
            @ServiceType("trino") ServiceSelector serviceSelector,
            FailureDetector failureDetector,
            InternalCommunicationConfig internalCommunicationConfig)
    {
        this.serviceSelector = requireNonNull(serviceSelector, "serviceSelector is null");
        this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
        this.httpsRequired = internalCommunicationConfig.isHttpsRequired();
    }

    @Override
    public Set<URI> getNodes()
    {
        // This is a deny-list.
        Set<ServiceDescriptor> failed = failureDetector.getFailed();
        return serviceSelector.selectAllServices().stream()
                .filter(not(failed::contains))
                .map(this::getHttpUri)
                .filter(Objects::nonNull)
                .collect(toImmutableSet());
    }

    private URI getHttpUri(ServiceDescriptor descriptor)
    {
        String url = descriptor.getProperties().get(httpsRequired ? "https" : "http");
        if (url != null) {
            return URI.create(url);
        }
        return null;
    }
}
