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
package io.trino.server.tracing;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.tracing.TracingEnabledConfig;
import io.trino.server.ServerConfig;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.trino.server.InternalCommunicationHttpClientModule.internalHttpClientModule;

public class DynamicTracingModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        if (!buildConfigObject(TracingEnabledConfig.class).isEnabled()) {
            return;
        }

        if (buildConfigObject(ServerConfig.class).isCoordinator()) {
            jaxrsBinder(binder).bind(CoordinatorDynamicTracingResource.class);
            jsonCodecBinder(binder).bindJsonCodec(TracingStatus.class);
            install(internalHttpClientModule("dynamic-tracing", ForDynamicTracing.class)
                    .build());
            binder.bind(CoordinatorDynamicTracingController.class).asEagerSingleton();
        }
        else {
            jaxrsBinder(binder).bind(WorkerDynamicTracingResource.class);
            binder.bind(DynamicTracingController.class).to(WorkerDynamicTracingController.class).asEagerSingleton();
        }

        configBinder(binder).bindConfig(DynamicTracingConfig.class);
    }
}
