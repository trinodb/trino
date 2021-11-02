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

import io.airlift.log.Logger;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;

public class FlywayMigration
{
    private static final Logger log = Logger.get(FlywayMigration.class);

    private FlywayMigration()
    {
    }

    public static void migrate(DbResourceGroupConfig config)
    {
        log.info("Performing migrations...");
        Flyway flyway = Flyway.configure()
                .dataSource(config.getConfigDbUrl(), config.getConfigDbUser(), config.getConfigDbPassword())
                .locations("/db/migration/mysql")
                .baselineOnMigrate(true)
                .baselineVersion("0")
                .load();

        MigrateResult migrations = flyway.migrate();
        log.info("Performed %s migrations", migrations.migrationsExecuted);
    }
}
