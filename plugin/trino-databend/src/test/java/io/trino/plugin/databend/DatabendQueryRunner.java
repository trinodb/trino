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
package io.trino.plugin.databend;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.HashMap;
import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;

public final class DatabendQueryRunner
{
    private DatabendQueryRunner() {}

    public static QueryRunner createDatabendQueryRunner(
            TestingDatabendServer server,
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties)
            throws Exception
    {
        QueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog("databend")
                                .setSchema("default")
                                .build())
                .setExtraProperties(extraProperties)
                .build();

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            Map<String, String> properties = new HashMap<>(ImmutableMap.<String, String>builder()
                    .put("connection-url", server.getJdbcUrl())
                    .put("connection-user", server.getUser())
                    .put("connection-password", server.getPassword())
                    .buildOrThrow());
            properties.putAll(connectorProperties);

            queryRunner.installPlugin(new DatabendPlugin());
            queryRunner.createCatalog("databend", "databend", properties);

            return queryRunner;
        }
        catch (Throwable e) {
            queryRunner.close();
            throw e;
        }
    }
}
