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
import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.Inject;
import io.trino.spi.admission.AdmissionPolicyFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Objects.requireNonNull;

/**
 * Runtime registry for plugin-supplied {@link AdmissionPolicyFactory} instances.
 *
 * <p>{@code PluginManager} populates this registry as plugins are loaded by
 * iterating {@code Plugin.getAdmissionPolicyFactories()} and calling
 * {@link #addAdmissionPolicyFactory(AdmissionPolicyFactory)}. The registry
 * itself does not enforce name uniqueness; per the design, duplicate-name
 * validation lives in {@link AdmissionPolicyModule}'s
 * {@code @Provides AdmissionPolicy} provider, which merges the multibinder
 * set (containing the bundled default factory) with the factories registered
 * here and validates uniqueness across both sources at provision time.
 *
 * <p>The bundled default factory ({@link MinWorkersAdmissionPolicy.Factory})
 * is bound directly via the {@code Multibinder<AdmissionPolicyFactory>} in
 * {@link AdmissionPolicyModule} and is therefore never registered through this
 * manager.
 */
@ThreadSafe
public class AdmissionPolicyManager
{
    private final List<AdmissionPolicyFactory> factories = new CopyOnWriteArrayList<>();

    @Inject
    public AdmissionPolicyManager() {}

    public void addAdmissionPolicyFactory(AdmissionPolicyFactory factory)
    {
        requireNonNull(factory, "factory is null");
        factories.add(factory);
    }

    public Collection<AdmissionPolicyFactory> getAdmissionPolicyFactories()
    {
        return ImmutableList.copyOf(factories);
    }
}
