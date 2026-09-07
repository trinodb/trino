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
package io.trino.plugin.session.db;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;

import static java.lang.String.format;

public class FlywayMigration
{
    private static final Logger log = Logger.get(FlywayMigration.class);

    private final Flyway flyway;
    private final boolean runMigrations;

    @Inject
    public FlywayMigration(DbSessionPropertyManagerConfig config)
    {
        // Resolve (and validate) the dialect from the URL first, so an unsupported JDBC URL fails with a clear
        // message rather than Flyway's generic "no driver found" error.
        String location = getLocation(config.getConfigDbUrl());
        flyway = Flyway.configure()
                .dataSource(config.getConfigDbUrl(), config.getConfigDbUser(), config.getConfigDbPassword())
                .locations(location)
                .baselineOnMigrate(true)
                .baselineVersion("0")
                .load();
        runMigrations = config.isRunMigrationsEnabled();
    }

    public void migrate()
    {
        if (!runMigrations) {
            log.info("Skipping migrations");
            return;
        }
        log.info("Performing migrations...");
        MigrateResult migrations = flyway.migrate();
        log.info("Performed %s migrations", migrations.migrationsExecuted);
    }

    private static String getLocation(String configDbUrl)
    {
        if (configDbUrl.startsWith("jdbc:postgresql")) {
            return "/db/migration/postgresql";
        }
        if (configDbUrl.startsWith("jdbc:mysql")) {
            return "/db/migration/mysql";
        }
        throw new IllegalArgumentException(format("Invalid JDBC URL: %s. Only PostgreSQL and MySQL are supported.", configDbUrl));
    }
}
