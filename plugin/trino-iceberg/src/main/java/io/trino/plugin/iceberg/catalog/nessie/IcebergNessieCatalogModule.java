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
package io.trino.plugin.iceberg.catalog.nessie;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import org.apache.iceberg.nessie.NessieIcebergClient;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.auth.BearerAuthenticationProvider;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.base.ClosingBinder.closingBinder;
import static java.lang.Math.toIntExact;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class IcebergNessieCatalogModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(IcebergNessieCatalogConfig.class);
        binder.bind(IcebergTableOperationsProvider.class).to(IcebergNessieTableOperationsProvider.class).in(Scopes.SINGLETON);
        newExporter(binder).export(IcebergTableOperationsProvider.class).withGeneratedName();
        binder.bind(TrinoCatalogFactory.class).to(TrinoNessieCatalogFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(TrinoCatalogFactory.class).withGeneratedName();
        closingBinder(binder).registerCloseable(NessieIcebergClient.class);
    }

    @Provides
    @Singleton
    public static NessieIcebergClient createNessieIcebergClient(IcebergNessieCatalogConfig icebergNessieCatalogConfig)
    {
        NessieClientBuilder builder = NessieClientBuilder.createClientBuilderFromSystemSettings()
                .withUri(icebergNessieCatalogConfig.getServerUri())
                .withDisableCompression(!icebergNessieCatalogConfig.isCompressionEnabled())
                .withReadTimeout(toIntExact(icebergNessieCatalogConfig.getReadTimeout().toMillis()))
                .withConnectionTimeout(toIntExact(icebergNessieCatalogConfig.getConnectionTimeout().toMillis()));

        icebergNessieCatalogConfig.getBearerToken()
                .ifPresent(token -> builder.withAuthentication(BearerAuthenticationProvider.create(token)));

        IcebergNessieCatalogConfig.ClientApiVersion clientApiVersion = icebergNessieCatalogConfig.getClientAPIVersion()
                .orElseGet(icebergNessieCatalogConfig::inferVersionFromURI);
        NessieApiV1 api = switch (clientApiVersion) {
            case V1 -> builder.build(NessieApiV1.class);
            case V2 -> builder.build(NessieApiV2.class);
        };

        return new NessieIcebergClient(api,
                icebergNessieCatalogConfig.getDefaultReferenceName(),
                null,
                ImmutableMap.of());
    }
}
