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
package io.trino;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;

public final class JdbcDriverCapabilities
{
    private JdbcDriverCapabilities() {}

    public static final int VERSION_HEAD = 0;

    public static Optional<Integer> testedVersion()
    {
        return Optional.ofNullable(System.getenv("TRINO_JDBC_VERSION_UNDER_TEST")).map(Integer::valueOf);
    }

    public static int driverVersion()
    {
        return jdbcDriver().getMajorVersion();
    }

    public static Driver jdbcDriver()
    {
        return getDriver("jdbc:trino:")
                .or(() -> getDriver("jdbc:presto:"))
                .orElseThrow(() -> new IllegalStateException("JDBC driver not available"));
    }

    private static Optional<Driver> getDriver(String url)
    {
        try {
            return Optional.of(DriverManager.getDriver(url));
        }
        catch (SQLException ignored) {
            return Optional.empty();
        }
    }

    public static boolean supportsSessionPropertiesViaConnectionUri()
    {
        return driverVersion() == VERSION_HEAD || driverVersion() >= 330;
    }

    public static boolean supportsParametricTimestamp()
    {
        return driverVersion() == VERSION_HEAD || driverVersion() >= 335;
    }

    public static boolean supportsParametricTimestampWithTimeZone()
    {
        return driverVersion() == VERSION_HEAD || driverVersion() >= 337;
    }

    public static boolean correctlyReportsTimestampWithTimeZone()
    {
        return driverVersion() == VERSION_HEAD || driverVersion() >= 348;
    }

    public static boolean supportsTimestampObjectRepresentationInCollections()
    {
        return driverVersion() == VERSION_HEAD || driverVersion() >= 348;
    }

    public static boolean hasBrokenParametricTimestampWithTimeZoneSupport()
    {
        // Since 335 JDBC client reports ClientCapabilities.PARAMETRIC_DATETIME which is used server side to determine whether
        // timestamp with time zone can be returned with given precision instead of default one (3).
        // JDBC client 335 and 336 are broken in regard to handling timestamp with time zone correctly.
        return driverVersion() == 335 || driverVersion() == 336;
    }
}
