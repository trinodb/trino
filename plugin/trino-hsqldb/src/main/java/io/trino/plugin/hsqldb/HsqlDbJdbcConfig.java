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
package io.trino.plugin.hsqldb;

import io.trino.plugin.jdbc.BaseJdbcConfig;
import jakarta.validation.constraints.AssertTrue;
import org.hsqldb.jdbcDriver;

public class HsqlDbJdbcConfig
        extends BaseJdbcConfig
{
    @AssertTrue(message = "Invalid JDBC URL for HsqlDB connector")
    public boolean isUrlValid()
    {
        jdbcDriver driver = new jdbcDriver();
        return driver.acceptsURL(getConnectionUrl());
    }

    @AssertTrue(message = "Database must not be specified in JDBC URL for HsqlDB connector")
    public boolean isUrlWithoutDatabase()
    {
        jdbcDriver driver = new jdbcDriver();
        return driver.acceptsURL(getConnectionUrl());
    }
}
