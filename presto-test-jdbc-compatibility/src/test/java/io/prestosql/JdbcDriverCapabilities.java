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
package io.prestosql;

import io.prestosql.jdbc.PrestoDriver;

import java.util.Optional;

public final class JdbcDriverCapabilities
{
    private JdbcDriverCapabilities() {}

    public static final int VERSION_HEAD = 0;

    public static Optional<Integer> testedVersion()
    {
        return Optional.ofNullable(System.getenv("PRESTO_JDBC_VERSION_UNDER_TEST")).map(Integer::valueOf);
    }

    public static int driverVersion()
    {
        try (PrestoDriver driver = new PrestoDriver()) {
            return driver.getMajorVersion();
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

    public static boolean hasBrokenParametricTimestampWithTimeZoneSupport()
    {
        // Since 335 JDBC client reports ClientCapabilities.PARAMETRIC_DATETIME which is used server side to determine whether
        // timestamp with time zone can be returned with given precision instead of default one (3).
        // JDBC client 335 and 336 are broken in regard to handling timestamp with time zone correctly.
        return driverVersion() == 335 || driverVersion() == 336;
    }

    public static boolean hasBrokenParametricTimeSupport()
    {
        return driverVersion() >= 337 && driverVersion() <= 340;
    }

    public static boolean hasBrokenTimeWithTimeZoneSupport()
    {
        return driverVersion() <= 340;
    }
}
