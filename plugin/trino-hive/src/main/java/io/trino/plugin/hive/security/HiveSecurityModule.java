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
package io.trino.plugin.hive.security;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.security.AllowAllSecurityModule;
import io.trino.plugin.base.security.ConnectorAccessControlModule;
import io.trino.plugin.base.security.FileBasedAccessControlModule;
import io.trino.plugin.base.security.ReadOnlySecurityModule;

import static io.airlift.configuration.ConfigurationAwareModule.combine;

public class HiveSecurityModule
        extends AbstractConfigurationAwareModule
{
    public enum HiveSecurity
    {
        ALLOW_ALL,
        READ_ONLY,
        FILE,
        SQL_STANDARD,
        SYSTEM,
        /**/
    }

    @Override
    protected void setup(Binder binder)
    {
        install(new ConnectorAccessControlModule());
        install(switch (buildConfigObject(SecurityConfig.class).getSecuritySystem()) {
            case ALLOW_ALL -> combine(new AllowAllSecurityModule(), new StaticAccessControlMetadataModule(), usingSystemSecurity(false));
            case READ_ONLY -> combine(new ReadOnlySecurityModule(), new StaticAccessControlMetadataModule(), usingSystemSecurity(false));
            case FILE -> combine(new FileBasedAccessControlModule(), new StaticAccessControlMetadataModule(), usingSystemSecurity(false));
            case SQL_STANDARD -> combine(new SqlStandardSecurityModule(), usingSystemSecurity(false));
            // do not bind a ConnectorAccessControl so the engine will use system security with system roles
            case SYSTEM -> combine(new StaticAccessControlMetadataModule(), usingSystemSecurity(true));
        });
    }

    private static Module usingSystemSecurity(boolean system)
    {
        return binder -> binder.bind(boolean.class).annotatedWith(UsingSystemSecurity.class).toInstance(system);
    }

    private static class StaticAccessControlMetadataModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(AccessControlMetadataFactory.class).toInstance(_ -> new AccessControlMetadata() {});
        }
    }
}
