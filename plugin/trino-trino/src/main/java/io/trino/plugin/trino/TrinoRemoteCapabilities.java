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

import io.trino.spi.connector.ConnectorSession;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

record TrinoRemoteCapabilities(
        Optional<String> version,
        Optional<Set<String>> functions,
        Optional<String> timeZone,
        Optional<CharToVarcharCastSemantics> charToVarcharCastSemantics)
{
    TrinoRemoteCapabilities
    {
        requireNonNull(version, "version is null");
        functions = requireNonNull(functions, "functions is null").map(Set::copyOf);
        requireNonNull(timeZone, "timeZone is null");
        requireNonNull(charToVarcharCastSemantics, "charToVarcharCastSemantics is null");
    }

    static TrinoRemoteCapabilities load(Connection connection)
            throws SQLException
    {
        requireNonNull(connection, "connection is null");
        return new TrinoRemoteCapabilities(
                Optional.ofNullable(connection.getMetaData().getDatabaseProductVersion()),
                Optional.of(loadFunctionNames(connection)),
                loadCurrentTimeZone(connection),
                loadCharToVarcharCastSemantics(connection));
    }

    static TrinoRemoteCapabilities unavailable()
    {
        return new TrinoRemoteCapabilities(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    static TrinoRemoteCapabilities forTesting(Set<String> functions)
    {
        return new TrinoRemoteCapabilities(
                Optional.of("477"),
                Optional.of(functions),
                Optional.of("UTC"),
                Optional.of(CharToVarcharCastSemantics.TRIMS_TRAILING_SPACES));
    }

    static TrinoRemoteCapabilities forTestingLegacyCharToVarcharCast(Set<String> functions)
    {
        return new TrinoRemoteCapabilities(
                Optional.of("477"),
                Optional.of(functions),
                Optional.of("UTC"),
                Optional.of(CharToVarcharCastSemantics.RETAINS_TRAILING_SPACES));
    }

    boolean hasFunction(String name)
    {
        requireNonNull(name, "name is null");
        return functions
                .map(values -> values.contains(name.toLowerCase(Locale.ENGLISH)))
                .orElse(false);
    }

    boolean hasSameTimeZone(ConnectorSession session)
    {
        requireNonNull(session, "session is null");
        return timeZone
                .map(remoteTimeZone -> remoteTimeZone.equalsIgnoreCase(session.getTimeZoneKey().getId()))
                .orElse(false);
    }

    enum CharToVarcharCastSemantics
    {
        TRIMS_TRAILING_SPACES,
        RETAINS_TRAILING_SPACES,
    }

    private static Set<String> loadFunctionNames(Connection connection)
            throws SQLException
    {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SHOW FUNCTIONS")) {
            Set<String> names = new HashSet<>();
            while (resultSet.next()) {
                names.add(resultSet.getString(1).toLowerCase(Locale.ENGLISH));
            }
            return Set.copyOf(names);
        }
    }

    private static Optional<String> loadCurrentTimeZone(Connection connection)
            throws SQLException
    {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT current_timezone()")) {
            if (resultSet.next()) {
                return Optional.ofNullable(resultSet.getString(1));
            }
        }
        return Optional.empty();
    }

    private static Optional<CharToVarcharCastSemantics> loadCharToVarcharCastSemantics(Connection connection)
    {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT length(CAST(CAST('a' AS CHAR(3)) AS VARCHAR))")) {
            if (!resultSet.next()) {
                return Optional.empty();
            }
            return switch (resultSet.getInt(1)) {
                case 1 -> Optional.of(CharToVarcharCastSemantics.TRIMS_TRAILING_SPACES);
                case 3 -> Optional.of(CharToVarcharCastSemantics.RETAINS_TRAILING_SPACES);
                default -> Optional.empty();
            };
        }
        catch (SQLException ignored) {
            return Optional.empty();
        }
    }
}
