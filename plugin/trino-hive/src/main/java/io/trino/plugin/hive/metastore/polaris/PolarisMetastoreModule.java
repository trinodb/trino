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
package io.trino.plugin.hive.metastore.polaris;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.RawHiveMetastoreFactory;
import io.trino.plugin.hive.AllowHiveTableRename;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static java.util.Objects.requireNonNull;

public class PolarisMetastoreModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        PolarisMetastoreConfig polarisConfig = buildConfigObject(PolarisMetastoreConfig.class);
        requireNonNull(polarisConfig.getUri(), "hive.metastore.polaris.uri is required");

        configBinder(binder).bindConfig(PolarisMetastoreConfig.class);

        switch (polarisConfig.getSecurity()) {
            case OAUTH2 -> {
                configBinder(binder).bindConfig(OAuth2SecurityConfig.class);
                install(new OAuth2SecurityModule());
            }
            case NONE -> install(new NoneSecurityModule());
        }

        binder.bind(AwsProperties.class).to(DefaultAwsProperties.class).in(Scopes.SINGLETON);
        httpClientBinder(binder).bindHttpClient("polaris", ForPolarisClient.class);
        binder.bind(PolarisRestClient.class).in(Scopes.SINGLETON);
        binder.bind(PolarisMetastoreStats.class).in(Scopes.SINGLETON);

        binder.bind(HiveMetastoreFactory.class)
                .annotatedWith(RawHiveMetastoreFactory.class)
                .to(PolarisHiveMetastoreFactory.class)
                .in(Scopes.SINGLETON);

        binder.bind(Key.get(boolean.class, AllowHiveTableRename.class)).toInstance(true);
    }
}
