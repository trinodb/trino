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
package io.trino.plugin.iceberg.catalog.bigquery;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import org.apache.iceberg.gcp.bigquery.BigQueryMetastoreClientImpl;

import javax.security.auth.login.CredentialException;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class IcebergBigQueryMetastoreModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(IcebergBigQueryMetastoreCatalogConfig.class);
        binder.bind(IcebergTableOperationsProvider.class).to(BigQueryMetastoreIcebergTableOperationsProvider.class).in(Scopes.SINGLETON);
        newExporter(binder).export(IcebergTableOperationsProvider.class).withGeneratedName();
        binder.bind(TrinoCatalogFactory.class).to(TrinoBigQueryMetastoreCatalogFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(TrinoCatalogFactory.class).withGeneratedName();
    }

    @Provides
    @Singleton
    public static BigQueryMetastoreClientImpl createBigQueryMetastoreClient(IcebergBigQueryMetastoreCatalogConfig config)
            throws GeneralSecurityException, IOException
    {
        BigQueryOptions.Builder optionsBuilder = BigQueryOptions.newBuilder();

        if (config.getProjectID() != null) {
            optionsBuilder.setProjectId(config.getProjectID());
        }
        if (config.getLocation() != null) {
            optionsBuilder.setLocation(config.getLocation());
        }
        if (config.getJsonKeyFilePath() != null) {
            try (FileInputStream fs = new FileInputStream(config.getJsonKeyFilePath())) {
                optionsBuilder.setCredentials(ServiceAccountCredentials.fromStream(fs));
            }
            catch (Exception e) {
                throw new CredentialException("Unable to locate GCP Service Account JSON file");
            }
        }
        return new BigQueryMetastoreClientImpl(optionsBuilder.build());
    }
}
