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
package io.trino.plugin.jdbc.logging;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class RemoteQueryModifierModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(FormatBasedRemoteQueryModifierConfig.class);

        if (buildConfigObject(FormatBasedRemoteQueryModifierConfig.class).getFormat().isBlank()) {
            binder.bind(RemoteQueryModifier.class).toInstance(RemoteQueryModifier.NONE);
        }
        else {
            binder.bind(RemoteQueryModifier.class).to(FormatBasedRemoteQueryModifier.class).in(SINGLETON);
        }
    }
}
