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
package io.trino.plugin.deltalake;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.security.FileBasedAccessControlModule;
import io.trino.plugin.base.security.ReadOnlySecurityModule;
import io.trino.plugin.hive.security.AllowAllSecurityModule;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static io.trino.plugin.deltalake.DeltaLakeAccessControlMetadataFactory.DEFAULT;

public class DeltaLakeSecurityModule
        extends AbstractConfigurationAwareModule
{
    public static final String FILE = "file";
    public static final String READ_ONLY = "read_only";
    public static final String ALLOW_ALL = "allow_all";

    @Override
    protected void setup(Binder binder)
    {
        bindSecurityModule(ALLOW_ALL, combine(
                new AllowAllSecurityModule(),
                new StaticAccessControlMetadataModule()));
        bindSecurityModule(READ_ONLY, combine(
                new ReadOnlySecurityModule(),
                new StaticAccessControlMetadataModule()));
        bindSecurityModule(FILE, combine(
                new FileBasedAccessControlModule(),
                new StaticAccessControlMetadataModule()));
        // SYSTEM: do not bind an ConnectorAccessControl so the engine will use system security with system roles
    }

    protected void bindSecurityModule(String name, Module module)
    {
        install(conditionalModule(
                DeltaLakeSecurityConfig.class,
                // imitate Airlift's enum matching
                security -> name.equalsIgnoreCase(security.getSecuritySystem().replace("-", "_")),
                module));
    }

    private static class StaticAccessControlMetadataModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            newOptionalBinder(binder, DeltaLakeAccessControlMetadataFactory.class).setBinding().toInstance(DEFAULT);
        }
    }
}
