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
package io.trino.plugin.metastore.jdbc;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.filesystem.HetuFileSystemClient;
import io.trino.spi.metastore.HetuMetaStoreFactory;
import io.trino.spi.metastore.HetuMetastore;
import io.trino.spi.statestore.StateStore;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class JdbcHetuMetastoreFactory
        implements HetuMetaStoreFactory
{
    private static final String FACTORY_TYPE = "jdbc";
    private static final String HETU_METASTORE_CACHE_TYPE_DEFAULT = "local";
    private ClassLoader classLoader;

    public JdbcHetuMetastoreFactory(ClassLoader classLoader)
    {
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public HetuMetastore create(String name, Map<String, String> config, HetuFileSystemClient client,
            StateStore stateStore, String inputType)
    {
        String type = inputType;
        requireNonNull(config, "config is null");
        Bootstrap app;
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            if (stateStore == null) {
                type = HETU_METASTORE_CACHE_TYPE_DEFAULT;
                app = new Bootstrap(new JdbcMetastoreModule(type));
            }
            else {
                app = new Bootstrap(new JdbcMetastoreModule(stateStore, type));
            }

            Injector injector =
                    app.doNotInitializeLogging().setRequiredConfigurationProperties(config).initialize();

            return injector.getInstance(HetuMetastore.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new ExceptionInInitializerError();
        }
    }

    @Override
    public String getName()
    {
        return FACTORY_TYPE;
    }
}
