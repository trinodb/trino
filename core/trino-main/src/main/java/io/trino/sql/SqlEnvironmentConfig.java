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
package io.trino.sql;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.trino.spi.type.TimeZoneKey;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.tree.Identifier;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;

import java.util.List;
import java.util.Optional;

public class SqlEnvironmentConfig
{
    private String path = "";
    private Optional<String> defaultCatalog = Optional.empty();
    private Optional<String> defaultSchema = Optional.empty();
    private Optional<String> defaultFunctionCatalog = Optional.empty();
    private Optional<String> defaultFunctionSchema = Optional.empty();
    private Optional<TimeZoneKey> forcedSessionTimeZone = Optional.empty();

    @NotNull
    public String getPath()
    {
        return path;
    }

    @Config("sql.path")
    public SqlEnvironmentConfig setPath(String path)
    {
        this.path = path;
        return this;
    }

    @NotNull
    public Optional<String> getDefaultCatalog()
    {
        return defaultCatalog;
    }

    @Config("sql.default-catalog")
    public SqlEnvironmentConfig setDefaultCatalog(String catalog)
    {
        this.defaultCatalog = Optional.ofNullable(catalog);
        return this;
    }

    @NotNull
    public Optional<String> getDefaultSchema()
    {
        return defaultSchema;
    }

    @Config("sql.default-schema")
    public SqlEnvironmentConfig setDefaultSchema(String schema)
    {
        this.defaultSchema = Optional.ofNullable(schema);
        return this;
    }

    @NotNull
    public Optional<String> getDefaultFunctionCatalog()
    {
        return defaultFunctionCatalog;
    }

    @Config("sql.default-function-catalog")
    public SqlEnvironmentConfig setDefaultFunctionCatalog(String catalog)
    {
        this.defaultFunctionCatalog = Optional.ofNullable(catalog);
        return this;
    }

    @NotNull
    public Optional<String> getDefaultFunctionSchema()
    {
        return defaultFunctionSchema;
    }

    @Config("sql.default-function-schema")
    public SqlEnvironmentConfig setDefaultFunctionSchema(String schema)
    {
        this.defaultFunctionSchema = Optional.ofNullable(schema);
        return this;
    }

    @NotNull
    public Optional<TimeZoneKey> getForcedSessionTimeZone()
    {
        return forcedSessionTimeZone;
    }

    @Config("sql.forced-session-time-zone")
    @ConfigDescription("User session time zone overriding value sent by client")
    public SqlEnvironmentConfig setForcedSessionTimeZone(@Nullable String timeZoneId)
    {
        this.forcedSessionTimeZone = Optional.ofNullable(timeZoneId)
                .map(TimeZoneKey::getTimeZoneKey);
        return this;
    }

    @AssertTrue(message = "sql.path must be a valid SQL path")
    public boolean isSqlPathValid()
    {
        return path.isEmpty() || validParsedSqlPath().isPresent();
    }

    @AssertTrue(message = "sql.default-function-catalog and sql.default-function-schema must be set together")
    public boolean isBothFunctionCatalogAndSchemaSet()
    {
        return defaultFunctionCatalog.isPresent() == defaultFunctionSchema.isPresent();
    }

    @AssertTrue(message = "default function schema must be in the default SQL path")
    public boolean isFunctionSchemaInSqlPath()
    {
        if (defaultFunctionCatalog.isEmpty() || defaultFunctionSchema.isEmpty()) {
            return true;
        }

        SqlPathElement function = new SqlPathElement(
                defaultFunctionCatalog.map(Identifier::new),
                defaultFunctionSchema.map(Identifier::new).orElseThrow());
        return validParsedSqlPath().map(path -> path.contains(function)).orElse(false);
    }

    private Optional<List<SqlPathElement>> validParsedSqlPath()
    {
        try {
            return Optional.of(SqlPath.parsePath(path));
        }
        catch (ParsingException e) {
            return Optional.empty();
        }
    }
}
