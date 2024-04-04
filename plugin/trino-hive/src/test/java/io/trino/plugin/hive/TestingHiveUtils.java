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
package io.trino.plugin.hive;

import com.google.inject.Injector;
import com.google.inject.Key;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.QueryRunner;

import static io.trino.plugin.hive.HiveQueryRunner.HIVE_CATALOG;

public final class TestingHiveUtils
{
    private TestingHiveUtils() {}

    public static <T> T getConnectorService(QueryRunner queryRunner, Class<T> clazz)
    {
        return getConnectorInjector(queryRunner).getInstance(clazz);
    }

    public static <T> T getConnectorService(QueryRunner queryRunner, Key<T> key)
    {
        return getConnectorInjector(queryRunner).getInstance(key);
    }

    private static Injector getConnectorInjector(QueryRunner queryRunner)
    {
        if (queryRunner instanceof DistributedQueryRunner) {
            return ((HiveConnector) ((DistributedQueryRunner) queryRunner).getCoordinator().getConnector(HIVE_CATALOG)).getInjector();
        }
        return ((HiveConnector) ((LocalQueryRunner) queryRunner).getConnector(HIVE_CATALOG)).getInjector();
    }
}
