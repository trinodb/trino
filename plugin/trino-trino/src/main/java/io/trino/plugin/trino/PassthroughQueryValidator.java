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
package io.trino.plugin.trino;

import io.trino.spi.TrinoException;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Query;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

/**
 * Enforces the upstream {@code system.query} contract: only top-level
 * row-returning queries (SELECT/WITH/VALUES/TABLE) are executed remotely.
 * Other JDBC connectors get this for free because their drivers report no
 * result set metadata for DML/DDL statements; a remote Trino reports a result
 * shape for those too, so the contract needs an explicit check here. No
 * further validation or security checks are performed — the execution
 * boundary is remote access control.
 */
final class PassthroughQueryValidator
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    private PassthroughQueryValidator() {}

    static void validate(String sql)
    {
        if (!(SQL_PARSER.createStatement(sql) instanceof Query)) {
            throw new TrinoException(NOT_SUPPORTED, "system.query only supports row-returning read queries");
        }
    }
}
