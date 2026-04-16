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
package io.trino.plugin.doris;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;

public class DorisModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        Multibinder.newSetBinder(binder, DorisQueryEventListener.class);

        binder.bind(DorisConnector.class).in(Scopes.SINGLETON);
        binder.bind(DorisJdbcConnectionFactory.class).in(Scopes.SINGLETON);
        binder.bind(DorisMetadataClient.class).to(JdbcDorisMetadataClient.class).in(Scopes.SINGLETON);
        binder.bind(DorisFlightSqlPortResolver.class).to(JdbcDorisFlightSqlPortResolver.class).in(Scopes.SINGLETON);
        binder.bind(DorisMetadata.class).in(Scopes.SINGLETON);
        binder.bind(DorisQueryBuilder.class).in(Scopes.SINGLETON);
        httpClientBinder(binder).bindHttpClient("doris", ForDoris.class);
        binder.bind(DorisSplitPlanner.class).to(FeDorisSplitPlanner.class).in(Scopes.SINGLETON);
        binder.bind(AdbcDorisFlightSqlClient.class).in(Scopes.SINGLETON);
        binder.bind(DorisFlightSqlClient.class).to(AdbcDorisFlightSqlClient.class);
        binder.bind(DorisArrowToPageConverter.class).in(Scopes.SINGLETON);
        binder.bind(DorisTypeMapper.class).in(Scopes.SINGLETON);
        binder.bind(DorisSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(DorisPageSourceProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(DorisConfig.class);
    }
}
