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

import io.airlift.log.Logger;
import io.prestosql.tempto.ProductTest;
import io.prestosql.tempto.query.QueryExecutionException;

import javax.annotation.concurrent.GuardedBy;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.tests.utils.QueryExecutors.onHive;
import static java.lang.Integer.parseInt;

public class HiveProductTest
        extends ProductTest
{
    private static final Logger log = Logger.get(HiveProductTest.class);

    @GuardedBy("this")
    private Integer hiveVersionMajor;

    protected synchronized int getHiveVersionMajor()
    {
        if (hiveVersionMajor == null) {
            hiveVersionMajor = readVersionFromHive()
                    .orElseGet(HiveProductTest::readVersionFromHiveCommandLine);
        }

        return hiveVersionMajor;
    }

    private static Optional<Integer> readVersionFromHive()
    {
        String hiveVersion;
        try {
            // version() is available in e.g. CDH 5 (Hive 1.1) and HDP 3.1 (Hive 3.1)
            hiveVersion = getOnlyElement(onHive().executeQuery("SELECT version()").column(1));
        }
        catch (QueryExecutionException e) {
            if (nullToEmpty(e.getMessage()).contains("Invalid function 'version'")) {
                log.info("version() function not found in Hive");
                return Optional.empty();
            }
            throw e;
        }
        Matcher matcher = Pattern.compile("^(\\d+)\\.").matcher(hiveVersion);
        checkState(matcher.lookingAt(), "Invalid Hive version: %s", hiveVersion);
        int hiveVersionMajor = parseInt(matcher.group(1));
        log.info("Found Hive version major %s from Hive version '%s'", hiveVersionMajor, hiveVersion);
        return Optional.of(hiveVersionMajor);
    }

    // version() is not available in e.g. HDP 2.6 (Hive 1.2)
    private static int readVersionFromHiveCommandLine()
    {
        String hiveServerCommand = getOnlyElement(onHive().executeQuery("SET system:sun.java.command").column(1));
        Matcher matcher = Pattern.compile("/hive-service-(\\d+)\\.[-0-9a-zA-Z.]+\\.jar").matcher(hiveServerCommand);
        checkState(matcher.find(), "Cannot find Hive version in Hive command line: %s", hiveServerCommand);
        int hiveVersionMajor = parseInt(matcher.group(1));
        log.info("Found Hive version major %s from Hive command line '%s'", hiveVersionMajor, hiveServerCommand);
        return hiveVersionMajor;
    }
}
