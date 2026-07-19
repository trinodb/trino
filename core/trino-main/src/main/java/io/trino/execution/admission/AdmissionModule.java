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

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.SystemSessionPropertiesProvider;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

/**
 * Wires the resource-aware query admission feature, installed from {@code CoordinatorModule}.
 * The feature is self-contained in this package; the dispatch path consults the controller
 * through {@link ResourceAwareAdmissionController}.
 */
public class AdmissionModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(ResourceAwareAdmissionConfig.class);
        binder.bind(ResourceAwareAdmissionController.class).in(Scopes.SINGLETON);
        newSetBinder(binder, SystemSessionPropertiesProvider.class).addBinding().to(ResourceAwareAdmissionSessionProperties.class).in(Scopes.SINGLETON);
    }
}
