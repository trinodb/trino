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
package io.trino.jmxutils;

import com.google.inject.Inject;
import com.google.inject.Module;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;
import org.weakref.jmx.ObjectNameGenerator;

import java.util.Optional;

import static io.airlift.configuration.ConfigurationAwareModule.combine;

public final class MBeanModule
{
    private MBeanModule() {}

    public static Module forMainServer()
    {
        return createMBeanModule();
    }

    public static Module forConnector()
    {
        return combine(
                createMBeanModule(),
                binder -> {
                    // require binding for ObjectNameGenerator
                    binder.bind(RequireObjectNameGenerator.class).asEagerSingleton();
                });
    }

    @SuppressModernizer // MBeanModule::new is forbidden, advising to use this class as a safety-adding wrapper.
    private static Module createMBeanModule()
    {
        return new org.weakref.jmx.guice.MBeanModule();
    }

    static class RequireObjectNameGenerator
    {
        @Inject
        RequireObjectNameGenerator(Optional<ObjectNameGenerator> objectNameGenerator)
        {
            if (objectNameGenerator.isEmpty()) {
                throw new IllegalStateException("ObjectNameGenerator must be explicitly bound");
            }
        }
    }
}
