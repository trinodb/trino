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
package io.trino.filesystem.gcs;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.configuration.SwitchModule.switchModule;

public class GcsFileSystemModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(GcsFileSystemConfig.class);
        binder.bind(GcsStorageFactory.class).in(Scopes.SINGLETON);
        binder.bind(GcsFileSystemFactory.class).in(Scopes.SINGLETON);

        install(switchModule(
                GcsFileSystemConfig.class,
                GcsFileSystemConfig::getAuthType,
                type -> switch (type) {
                    case ACCESS_TOKEN -> _ -> binder.bind(GcsAuth.class).to(GcsAccessTokenAuth.class).in(Scopes.SINGLETON);
                    case SERVICE_ACCOUNT -> new GcsServiceAccountModule();
                    case APPLICATION_DEFAULT -> _ -> binder.bind(GcsAuth.class).to(ApplicationDefaultAuth.class).in(Scopes.SINGLETON);
                }));
    }
}
