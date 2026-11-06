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
package io.trino.plugin.starrocks;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class StarRocksModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(StarRocksConnector.class).in(Scopes.SINGLETON);
        binder.bind(StarRocksJdbcConnectionFactory.class).in(Scopes.SINGLETON);
        binder.bind(StarRocksMetadataClient.class).to(JdbcStarRocksMetadataClient.class).in(Scopes.SINGLETON);
        binder.bind(StarRocksMetadata.class).in(Scopes.SINGLETON);
        binder.bind(StarRocksTypeMapper.class).in(Scopes.SINGLETON);
        binder.bind(StarRocksQueryBuilder.class).in(Scopes.SINGLETON);
        binder.bind(AdbcStarRocksFlightSqlClient.class).in(Scopes.SINGLETON);
        binder.bind(StarRocksFlightSqlClient.class).to(AdbcStarRocksFlightSqlClient.class);
        binder.bind(StarRocksArrowToPageConverter.class).in(Scopes.SINGLETON);
        binder.bind(StarRocksSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(StarRocksPageSourceProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(StarRocksConfig.class);
    }
}
