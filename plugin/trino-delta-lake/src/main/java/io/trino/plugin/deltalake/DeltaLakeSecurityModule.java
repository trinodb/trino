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
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static io.trino.plugin.deltalake.DeltaLakeAccessControlMetadataFactory.DEFAULT;

public class DeltaLakeSecurityModule
        extends AbstractConfigurationAwareModule
{
    public enum DeltaLakeSecurity
    {
        ALLOW_ALL,
        READ_ONLY,
        FILE,
        SYSTEM,
        /**/
    }

    @Override
    protected void setup(Binder binder)
    {
        install(switch (buildConfigObject(DeltaLakeSecurityConfig.class).getSecuritySystem()) {
            case ALLOW_ALL -> combine(new AllowAllSecurityModule(), new StaticAccessControlMetadataModule());
            case READ_ONLY -> combine(new ReadOnlySecurityModule(), new StaticAccessControlMetadataModule());
            case FILE -> combine(new FileBasedAccessControlModule(), new StaticAccessControlMetadataModule());
            // do not bind a ConnectorAccessControl so the engine will use system security with system roles
            case SYSTEM -> EMPTY_MODULE;
        });
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
