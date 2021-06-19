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
package io.trino.tests.product.hive;

import com.google.inject.Module;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.initialization.SuiteModuleProvider;

import javax.inject.Singleton;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Optional;

import static io.trino.tests.product.utils.QueryExecutors.onHive;

public class HiveVersionProvider
{
    private Optional<HiveVersion> hiveVersion = Optional.empty();

    public synchronized HiveVersion getHiveVersion()
    {
        hiveVersion = hiveVersion.or(() -> Optional.of(detectHiveVersion()));
        return hiveVersion.get();
    }

    private static HiveVersion detectHiveVersion()
    {
        try {
            DatabaseMetaData metaData = onHive().getConnection().getMetaData();
            return HiveVersion.createFromString(metaData.getDatabaseProductVersion());
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static class ModuleProvider
            implements SuiteModuleProvider
    {
        @Override
        public Module getModule(Configuration configuration)
        {
            return binder -> binder.bind(HiveVersionProvider.class).in(Singleton.class);
        }
    }
}
