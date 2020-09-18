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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.prestosql.jdbc.BaseTestJdbcResultSet;
import io.prestosql.jdbc.PrestoDriver;
import org.testcontainers.containers.PrestoContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static io.prestosql.testing.DataProviders.toDataProvider;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestJdbcResultSetCompatibilityOldServer
        extends BaseTestJdbcResultSet
{
    private static final int NUMBER_OF_TESTED_VERSIONS = 5;
    private static final int TESTED_VERSIONS_GRANULARITY = 3;
    private static final String TESTED_VERSIONS_ENV_KEY = "TESTED_PRESTO_SERVER_VERSIONS";

    /**
     * Empty means that we could not obtain current Presto version and tests defined here will be marked as failed.
     */
    private final Optional<String> testedPrestoVersion;
    private PrestoContainer<?> prestoContainer;

    @Factory(dataProvider = "testedPrestoVersions")
    public TestJdbcResultSetCompatibilityOldServer(Optional<String> testedPrestoVersion)
    {
        this.testedPrestoVersion = requireNonNull(testedPrestoVersion, "testedPrestoVersion is null");
    }

    @DataProvider
    public static Object[][] testedPrestoVersions()
    {
        try {
            if (System.getenv(TESTED_VERSIONS_ENV_KEY) != null) {
                // This is currently needed so that the test is runnable from IDE
                // TODO use filtered resource to provide current version instead.
                return Splitter.on(",").trimResults().splitToList(System.getenv(TESTED_VERSIONS_ENV_KEY)).stream()
                        .map(Optional::of)
                        .collect(toDataProvider());
            }

            String currentVersionString = requireNonNull(PrestoDriver.class.getPackage().getImplementationVersion());
            // this should match git-describe output which is used as version stored in metadata
            Matcher matcher = Pattern.compile("(\\d+)(?:-\\d+-[a-z0-9]+)?").matcher(currentVersionString);
            checkState(matcher.matches());
            int currentVersion = Integer.parseInt(matcher.group(1));
            ImmutableList.Builder<String> testedPrestoVersions = ImmutableList.builder();
            int lastReleasedVersion = currentVersion - 1;
            for (int i = 0; i < NUMBER_OF_TESTED_VERSIONS; i++) {
                testedPrestoVersions.add(String.valueOf(lastReleasedVersion - TESTED_VERSIONS_GRANULARITY * i));
            }

            return testedPrestoVersions.build().stream()
                    .map(Optional::of)
                    .collect(toDataProvider());
        }
        catch (Throwable e) {
            // We cannot throw here because TestNG does not handle exceptions coming out from @DataProvider used with @Factory well.
            // Instead we return marker Option.empty() as only parametrization. Then we will fail test run in setupPrestoContainer().
            System.err.println("Could not determine Presto versions to test; " + e.getMessage() + "\n" + getStackTraceAsString(e));
            return new Object[][] {
                    {Optional.empty()}
            };
        }
    }

    @BeforeClass
    public void setupPrestoContainer()
    {
        if (testedPrestoVersion.isEmpty()) {
            throw new AssertionError("Could not determine current Presto version");
        }

        prestoContainer = new PrestoContainer<>("prestosql/presto:" + testedPrestoVersion.get());
        prestoContainer.start();

        // verify that version reported by Presto server matches requested one.
        try (ConnectedStatement statementWrapper = newStatement()) {
            try (ResultSet rs = statementWrapper.getStatement().executeQuery("SELECT node_version FROM system.runtime.nodes")) {
                assertTrue(rs.next());
                String actualPrestoVersion = rs.getString(1);
                assertEquals(actualPrestoVersion, testedPrestoVersion.get(), "Presto server version reported by container does not match expected one");
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Could not get version from Presto server", e);
        }
    }

    @AfterClass(alwaysRun = true)
    public void tearDownPrestoContainer()
    {
        if (prestoContainer != null) {
            prestoContainer.stop();
            prestoContainer = null;
        }
    }

    @Override
    protected Connection createConnection()
            throws SQLException
    {
        return DriverManager.getConnection(prestoContainer.getJdbcUrl(), "test", null);
    }

    @Override
    protected int getTestedPrestoServerVersion()
    {
        assertTrue(testedPrestoVersion.isPresent());
        return Integer.parseInt(testedPrestoVersion.get());
    }

    @Override
    public String toString()
    {
        // This allows distinguishing tests run against different Presto server version from each other.
        // It is included in tests report and maven output.
        return "TestJdbcResultSetCompatibility[" + testedPrestoVersion.orElse("unknown") + "]";
    }
}
