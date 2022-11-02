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
package io.trino.plugin.resourcegroups.db;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.Test;

@Test(singleThreaded = true)
public class TestDbResourceGroupsPostgresqlFlywayMigration
        extends BaseTestDbResourceGroupsFlywayMigration
{
    @Override
    protected final JdbcDatabaseContainer<?> startContainer()
    {
        JdbcDatabaseContainer<?> container = new PostgreSQLContainer<>("postgres:10.20");
        container.start();
        return container;
    }
}
