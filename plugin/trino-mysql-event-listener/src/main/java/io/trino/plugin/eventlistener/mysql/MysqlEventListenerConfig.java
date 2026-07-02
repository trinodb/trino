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
package io.trino.plugin.eventlistener.mysql;

import com.google.common.collect.ImmutableSet;
import com.mysql.cj.jdbc.Driver;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;

import java.sql.SQLException;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class MysqlEventListenerConfig
{
    private String url;
    private Set<String> excludedColumns = ImmutableSet.of();
    private boolean terminateOnInitializationFailure = true;

    @NotNull
    public String getUrl()
    {
        return url;
    }

    @ConfigSecuritySensitive
    @Config("mysql-event-listener.db.url")
    public MysqlEventListenerConfig setUrl(String url)
    {
        this.url = url;
        return this;
    }

    @AssertTrue(message = "Invalid JDBC URL for MySQL event listener")
    public boolean isValidUrl()
    {
        try {
            return new Driver().acceptsURL(getUrl());
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Set<String> getExcludedColumns()
    {
        return excludedColumns;
    }

    @Config("mysql-event-listener.excluded-columns")
    @ConfigDescription("Comma-separated list of trino_queries text columns to exclude from stored query events")
    public MysqlEventListenerConfig setExcludedColumns(Set<String> excludedColumns)
    {
        this.excludedColumns = requireNonNull(excludedColumns, "excludedColumns is null").stream()
                .filter(column -> !column.isBlank())
                .map(ExcludableColumn::normalize)
                .collect(toImmutableSet());
        return this;
    }

    @AssertTrue(message = "Only supported MySQL event listener text columns can be excluded")
    public boolean isValidExcludedColumns()
    {
        return excludedColumns.stream().allMatch(ExcludableColumn::isSupported);
    }

    public boolean getTerminateOnInitializationFailure()
    {
        return terminateOnInitializationFailure;
    }

    @Config("mysql-event-listener.terminate-on-initialization-failure")
    @ConfigDescription("The MySQL event listener initialization may fail if the database is unavailable. This flag determines whether an exception should be thrown in such cases.")
    public MysqlEventListenerConfig setTerminateOnInitializationFailure(boolean terminateOnInitializationFailure)
    {
        this.terminateOnInitializationFailure = terminateOnInitializationFailure;
        return this;
    }
}
