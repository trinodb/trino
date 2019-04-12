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
package io.prestosql.plugin.druid;

import io.prestosql.Session;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tests.DistributedQueryRunner;

import java.util.HashMap;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.prestosql.testing.TestingSession.testSessionBuilder;

public class DruidQueryRunner
{
    private DruidQueryRunner() {}

    private static final String DRUID = "druid";

    public static QueryRunner createDruidQueryRunner(TestingDruidServer testingDruidServer)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(createSession()).build();
            Map<String, String> connectorProperties = new HashMap<>();
            connectorProperties.putIfAbsent("connection-url", testingDruidServer.getJdbcUrl());
            queryRunner.installPlugin(new DruidJdbcPlugin());
            queryRunner.createCatalog(DRUID, DRUID, connectorProperties);
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog(DRUID)
                .setSchema(DRUID)
                .build();
    }
}
