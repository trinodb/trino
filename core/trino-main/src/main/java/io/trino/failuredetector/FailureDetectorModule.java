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
package io.trino.failuredetector;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import org.weakref.jmx.guice.ExportBinder;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.server.InternalCommunicationHttpClientModule.internalHttpClientModule;

public class FailureDetectorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(internalHttpClientModule("failure-detector", ForFailureDetector.class)
                .withTracing()
                .build());

        configBinder(binder).bindConfig(FailureDetectorConfig.class);

        binder.bind(HeartbeatFailureDetector.class).in(Scopes.SINGLETON);

        binder.bind(FailureDetector.class)
                .to(HeartbeatFailureDetector.class)
                .in(Scopes.SINGLETON);

        ExportBinder.newExporter(binder)
                .export(HeartbeatFailureDetector.class)
                .withGeneratedName();
    }
}
