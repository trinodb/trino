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
package io.trino.plugin.iceberg;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.security.AllowAllSecurityModule;
import io.trino.plugin.base.security.ConnectorAccessControlModule;
import io.trino.plugin.base.security.FileBasedAccessControlModule;
import io.trino.plugin.base.security.ReadOnlySecurityModule;
import io.trino.plugin.hive.security.UsingSystemSecurity;

import static io.airlift.configuration.ConfigurationAwareModule.combine;

public class IcebergSecurityModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(new ConnectorAccessControlModule());
        install(switch (buildConfigObject(IcebergSecurityConfig.class).getSecuritySystem()) {
            case ALLOW_ALL -> combine(new AllowAllSecurityModule(), usingSystemSecurity(false));
            case READ_ONLY -> combine(new ReadOnlySecurityModule(), usingSystemSecurity(false));
            case FILE -> combine(new FileBasedAccessControlModule(), usingSystemSecurity(false));
            // do not bind a ConnectorAccessControl so the engine will use system security with system roles
            case SYSTEM -> usingSystemSecurity(true);
        });
    }

    private static Module usingSystemSecurity(boolean system)
    {
        return binder -> binder.bind(boolean.class).annotatedWith(UsingSystemSecurity.class).toInstance(system);
    }
}
