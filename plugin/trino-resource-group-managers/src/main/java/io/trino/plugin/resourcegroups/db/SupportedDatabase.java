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

import java.util.Arrays;

import static java.lang.String.format;

enum SupportedDatabase
{
    MYSQL("jdbc:mysql:", "/db/migration/mysql"),
    POSTGRESQL("jdbc:postgresql:", "/db/migration/postgresql"),
    ORACLE("jdbc:oracle:", "/db/migration/oracle");

    private final String urlPrefix;
    private final String migrationLocation;

    SupportedDatabase(String urlPrefix, String migrationLocation)
    {
        this.urlPrefix = urlPrefix;
        this.migrationLocation = migrationLocation;
    }

    static SupportedDatabase requireSupported(String url)
    {
        return Arrays.stream(values())
                .filter(db -> url.startsWith(db.urlPrefix))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        format("Invalid JDBC URL: %s. Only PostgreSQL, MySQL, and Oracle are supported.", url)));
    }

    String getMigrationLocation()
    {
        return migrationLocation;
    }
}
