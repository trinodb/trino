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
package io.prestosql.plugin.genericjdbc;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.ForBaseJdbc;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.credential.CredentialProvider;
import io.prestosql.spi.PrestoException;

import java.lang.reflect.InvocationTargetException;
import java.sql.Driver;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.DRIVER_NOT_FOUND;

public class GenericJdbcClientModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(Key.get(JdbcClient.class, ForBaseJdbc.class)).to(GenericJdbcClient.class)
                 .in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public ConnectionFactory getConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider)
    {
        try {
            Class.forName(config.getDriverClass());
        }
        catch (ClassNotFoundException e) {
            throw new PrestoException(DRIVER_NOT_FOUND, config.getDriverClass() + " not found");
        }
        try {
            try {
                return new DriverConnectionFactory((Driver) Class.forName(config.getDriverClass()).getConstructor().newInstance(), config, credentialProvider);
            }
            catch (InvocationTargetException e) {
                e.printStackTrace();
            }
            catch (NoSuchMethodException e) {
                e.printStackTrace();
            }
        }
        catch (InstantiationException e) {
            e.printStackTrace();
        }
        catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }
}
