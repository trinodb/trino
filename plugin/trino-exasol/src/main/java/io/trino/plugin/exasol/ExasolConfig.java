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
package io.trino.plugin.exasol;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import java.nio.file.Path;
import java.util.Optional;

public class ExasolConfig
{
    private Path jdbcDriverLogDir;

    public Optional<Path> getJdbcDriverLogDir()
    {
        return Optional.ofNullable(jdbcDriverLogDir);
    }

    @ConfigDescription("Set a log directory to enable logging for the Exasol JDBC driver")
    @Config("exasol.jdbc-driver.log-dir")
    public void setJdbcDriverLogDir(Path jdbcDriverLogDir)
    {
        this.jdbcDriverLogDir = jdbcDriverLogDir;
    }
}
