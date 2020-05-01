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
package io.prestosql.tests.hive;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.prestosql.tempto.ProductTest;

import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.tests.utils.QueryExecutors.onHive;

public class HiveProductTest
        extends ProductTest
{
    @Inject
    @Named("databases.hive.major_version")
    private int hiveVersionMajor;
    private boolean hiveVersionMajorVerified;

    @Inject
    @Named("databases.hive.minor_version")
    private int hiveVersionMinor;
    private boolean hiveVersionMinorVerified;

    protected int getHiveVersionMajor()
    {
        checkState(hiveVersionMajor > 0, "hiveVersionMajor not set");
        if (!hiveVersionMajorVerified) {
            int detected = detectHiveVersionMajor();
            checkState(hiveVersionMajor == detected, "Hive version major expected: %s, but was detected as: %s", hiveVersionMajor, detected);
            hiveVersionMajorVerified = true;
        }
        return hiveVersionMajor;
    }

    private static int detectHiveVersionMajor()
    {
        try {
            return onHive().getConnection().getMetaData().getDatabaseMajorVersion();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected int getHiveVersionMinor()
    {
        checkState(hiveVersionMinor > 0, "hiveVersionMinor not set");
        if (!hiveVersionMinorVerified) {
            int detected = detectHiveVersionMinor();
            checkState(hiveVersionMinor == detected, "Hive version minor expected: %s, but was detected as: %s", hiveVersionMinor, detected);
            hiveVersionMinorVerified = true;
        }
        return hiveVersionMinor;
    }

    private static int detectHiveVersionMinor()
    {
        try {
            return onHive().getConnection().getMetaData().getDatabaseMinorVersion();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected boolean isHiveVersionBefore12()
    {
        return getHiveVersionMajor() == 0
                || (getHiveVersionMajor() == 1 && getHiveVersionMinor() < 2);
    }
}
