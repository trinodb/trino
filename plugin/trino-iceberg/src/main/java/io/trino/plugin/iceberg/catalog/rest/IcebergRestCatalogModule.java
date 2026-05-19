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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.ConfigPropertyMetadata;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergFileSystemFactory;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.SessionType;
import io.trino.spi.TrinoException;

import java.lang.reflect.Constructor;
import java.util.Map;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public class IcebergRestCatalogModule
        extends AbstractConfigurationAwareModule
{
    private static final String HEADER_PROPERTY_PREFIX = "iceberg.rest-catalog.header.";
    private static final String TOKEN_EXCHANGE_EXTRA_PREFIX = "iceberg.rest-catalog.token-exchange.extra.";

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(IcebergRestCatalogConfig.class);
        configBinder(binder).bindConfig(IcebergRestCatalogTokenExchangeConfig.class);

        install(switch (buildConfigObject(IcebergRestCatalogConfig.class).getSecurity()) {
            case OAUTH2 -> new OAuth2SecurityModule();
            case SIGV4 -> new SigV4SecurityModule();
            case GOOGLE -> new GoogleSecurityModule();
            case NONE -> new NoneSecurityModule();
        });

        binder.bind(IcebergRestCatalogPropertiesProvider.class).in(Scopes.SINGLETON);

        ImmutableMap.Builder<String, String> headers = ImmutableMap.builder();
        ImmutableMap.Builder<String, String> tokenExchangeExtra = ImmutableMap.builder();
        getProperties().forEach((name, value) -> {
            if (name.startsWith(HEADER_PROPERTY_PREFIX)) {
                consumeProperty(new ConfigPropertyMetadata(name, false));
                headers.put(name, value);
            }
            else if (name.startsWith(TOKEN_EXCHANGE_EXTRA_PREFIX)) {
                consumeProperty(new ConfigPropertyMetadata(name, false));
                tokenExchangeExtra.put(name.substring(TOKEN_EXCHANGE_EXTRA_PREFIX.length()), value);
            }
        });
        binder.bind(new TypeLiteral<Map<String, String>>() {}).annotatedWith(Names.named("restCatalogHeaders")).toInstance(headers.buildOrThrow());
        binder.bind(new TypeLiteral<Map<String, String>>() {}).annotatedWith(Names.named("tokenExchangeExtra")).toInstance(tokenExchangeExtra.buildOrThrow());

        newOptionalBinder(binder, OidcStsCredentialExchanger.class);
        newOptionalBinder(binder, OidcTokenExchanger.class);
        binder.bind(UserTokenProvider.class).in(Scopes.SINGLETON);

        binder.bind(TrinoCatalogFactory.class).to(TrinoIcebergRestCatalogFactory.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, IcebergFileSystemFactory.class).setBinding().to(IcebergRestCatalogFileSystemFactory.class).in(Scopes.SINGLETON);

        IcebergConfig icebergConfig = buildConfigObject(IcebergConfig.class);
        IcebergRestCatalogConfig restCatalogConfig = buildConfigObject(IcebergRestCatalogConfig.class);
        if (restCatalogConfig.isVendedCredentialsEnabled() && icebergConfig.isRegisterTableProcedureEnabled()) {
            throw new TrinoException(NOT_SUPPORTED, "Using the `register_table` procedure with vended credentials is currently not supported");
        }
        if (restCatalogConfig.isTokenDelegation() && restCatalogConfig.getSessionType() != SessionType.USER) {
            throw new TrinoException(NOT_SUPPORTED, "iceberg.rest-catalog.token-delegation requires iceberg.rest-catalog.session=user");
        }
        IcebergRestCatalogSigV4Config sigV4Config = buildConfigObject(IcebergRestCatalogSigV4Config.class);
        if (sigV4Config.isStsWebIdentity() && restCatalogConfig.getSessionType() != SessionType.USER) {
            throw new TrinoException(NOT_SUPPORTED, "iceberg.rest-catalog.sigv4.sts-web-identity requires iceberg.rest-catalog.session=user");
        }
        IcebergRestCatalogTokenExchangeConfig tokenExchangeConfig = buildConfigObject(IcebergRestCatalogTokenExchangeConfig.class);
        if (tokenExchangeConfig.isEnabled() && restCatalogConfig.getSessionType() != SessionType.USER) {
            throw new TrinoException(NOT_SUPPORTED, "iceberg.rest-catalog.token-exchange-enabled requires iceberg.rest-catalog.session=user");
        }
        if (tokenExchangeConfig.isEnabled()) {
            try {
                Constructor<OidcTokenExchanger> ctor = OidcTokenExchanger.class
                        .getConstructor(IcebergRestCatalogTokenExchangeConfig.class, Map.class);
                newOptionalBinder(binder, OidcTokenExchanger.class)
                        .setBinding()
                        .toConstructor(ctor)
                        .in(Scopes.SINGLETON);
            }
            catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
