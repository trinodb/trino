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
package io.trino.plugin.mariadb;

import io.trino.plugin.jdbc.BaseJdbcConfig;
import jakarta.validation.constraints.AssertTrue;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.Driver;

import java.sql.SQLException;

public class MariaDbJdbcConfig
        extends BaseJdbcConfig
{
    @AssertTrue(message = "Invalid JDBC URL for MariaDB connector")
    public boolean isUrlValid()
    {
        Driver driver = new Driver();
        return driver.acceptsURL(getConnectionUrl());
    }

    @AssertTrue(message = "Database (catalog) must not be specified in JDBC URL for MariaDB connector")
    public boolean isUrlWithoutDatabase()
    {
        try {
            Configuration conf = Configuration.parse(getConnectionUrl());
            if (conf == null) {
                return false;
            }
            return conf.database() == null;
        }
        catch (SQLException e) {
            return false;
        }
    }
}
