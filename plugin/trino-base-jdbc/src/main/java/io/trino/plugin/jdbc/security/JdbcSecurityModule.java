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
package io.trino.plugin.jdbc.security;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.security.AllowAllAccessControlModule;
import io.trino.plugin.base.security.FileBasedAccessControlModule;
import io.trino.plugin.base.security.ReadOnlySecurityModule;
import io.trino.plugin.jdbc.security.JdbcSecurityConfig.SecuritySystem;

import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.trino.plugin.jdbc.security.JdbcSecurityConfig.SecuritySystem.ALLOW_ALL;
import static io.trino.plugin.jdbc.security.JdbcSecurityConfig.SecuritySystem.FILE;
import static io.trino.plugin.jdbc.security.JdbcSecurityConfig.SecuritySystem.READ_ONLY;

public class JdbcSecurityModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        bindSecurityModule(ALLOW_ALL, new AllowAllAccessControlModule());
        bindSecurityModule(READ_ONLY, new ReadOnlySecurityModule());
        bindSecurityModule(FILE, new FileBasedAccessControlModule());
    }

    private void bindSecurityModule(SecuritySystem type, Module module)
    {
        install(installModuleIf(
                JdbcSecurityConfig.class,
                security -> type == security.getSecuritySystem(),
                module));
    }
}
