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
package io.trino.plugin.mongodb;

import io.trino.testing.QueryRunner;

public class TestMongo4LatestConnectorSmokeTest
        extends BaseMongoConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        MongoServer server = closeAfterClass(new MongoServer("4.4.1"));
        return MongoQueryRunner.builder(server)
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }
}
