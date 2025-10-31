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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergFileSystemFactory;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.Security;
import io.trino.spi.TrinoException;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public class IcebergRestCatalogModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(IcebergRestCatalogConfig.class);
        install(conditionalModule(
                IcebergRestCatalogConfig.class,
                config -> config.getSecurity() == Security.OAUTH2,
                new OAuth2SecurityModule()));
        install(conditionalModule(
                IcebergRestCatalogConfig.class,
                config -> config.getSecurity() == Security.SIGV4,
                new SigV4SecurityModule()));
        install(conditionalModule(
                IcebergRestCatalogConfig.class,
                config -> config.getSecurity() == Security.GOOGLE,
                new GoogleSecurityModule()));
        install(conditionalModule(
                IcebergRestCatalogConfig.class,
                config -> config.getSecurity() == Security.NONE,
                new NoneSecurityModule()));

        binder.bind(TrinoCatalogFactory.class).to(TrinoIcebergRestCatalogFactory.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, IcebergFileSystemFactory.class).setBinding().to(IcebergRestCatalogFileSystemFactory.class).in(Scopes.SINGLETON);

        IcebergConfig icebergConfig = buildConfigObject(IcebergConfig.class);
        IcebergRestCatalogConfig restCatalogConfig = buildConfigObject(IcebergRestCatalogConfig.class);
        if (restCatalogConfig.isVendedCredentialsEnabled() && icebergConfig.isRegisterTableProcedureEnabled()) {
            throw new TrinoException(NOT_SUPPORTED, "Using the `register_table` procedure with vended credentials is currently not supported");
        }
    }
}
