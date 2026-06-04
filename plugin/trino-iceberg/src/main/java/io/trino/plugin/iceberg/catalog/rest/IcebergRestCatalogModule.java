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
import io.trino.spi.TrinoException;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.bootstrap.ClosingBinder.closingBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.Security.OAUTH2;
import static io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.SessionType.USER;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public class IcebergRestCatalogModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(IcebergRestCatalogConfig.class);
        install(switch (buildConfigObject(IcebergRestCatalogConfig.class).getSecurity()) {
            case OAUTH2 -> new OAuth2SecurityModule();
            case SIGV4 -> new SigV4SecurityModule();
            case GOOGLE -> new GoogleSecurityModule();
            case NONE -> new NoneSecurityModule();
        });

        binder.bind(IcebergRestCatalogPropertiesProvider.class).in(Scopes.SINGLETON);
        binder.bind(TrinoIcebergRestCatalogFactory.class).in(Scopes.SINGLETON);
        binder.bind(TrinoCatalogFactory.class).to(TrinoIcebergRestCatalogFactory.class);
        closingBinder(binder).registerCloseable(TrinoIcebergRestCatalogFactory.class);
        newOptionalBinder(binder, IcebergFileSystemFactory.class).setBinding().to(IcebergRestCatalogFileSystemFactory.class).in(Scopes.SINGLETON);

        IcebergConfig icebergConfig = buildConfigObject(IcebergConfig.class);
        IcebergRestCatalogConfig restCatalogConfig = buildConfigObject(IcebergRestCatalogConfig.class);
        if (restCatalogConfig.getSecurity() == OAUTH2 &&
                restCatalogConfig.getSessionType() != USER) {
            OAuth2SecurityConfig oauth2SecurityConfig = buildConfigObject(OAuth2SecurityConfig.class);
            if (oauth2SecurityConfig.getCredential().isEmpty() && oauth2SecurityConfig.getToken().isEmpty()) {
                throw new TrinoException(CONFIGURATION_INVALID, "OAuth2 REST catalog requires iceberg.rest-catalog.oauth2.credential or iceberg.rest-catalog.oauth2.token when iceberg.rest-catalog.session is not USER");
            }
        }
        if (restCatalogConfig.getSessionType() == USER && restCatalogConfig.getSecurity() != OAUTH2) {
            throw new TrinoException(CONFIGURATION_INVALID, "iceberg.rest-catalog.session=USER requires iceberg.rest-catalog.security=OAUTH2");
        }
        if (restCatalogConfig.isVendedCredentialsEnabled() && icebergConfig.isRegisterTableProcedureEnabled()) {
            throw new TrinoException(NOT_SUPPORTED, "Using the `register_table` procedure with vended credentials is currently not supported");
        }
    }
}
