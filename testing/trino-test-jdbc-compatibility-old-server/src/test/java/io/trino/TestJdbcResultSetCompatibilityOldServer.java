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

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import io.trino.jdbc.BaseTestJdbcResultSet;
import org.testcontainers.containers.TrinoContainer;
import org.testcontainers.utility.DockerImageName;
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
import static io.trino.testing.DataProviders.toDataProvider;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestJdbcResultSetCompatibilityOldServer
        extends BaseTestJdbcResultSet
{
    private static final int FIRST_VERSION = 351;
    private static final int NUMBER_OF_TESTED_VERSIONS = 5;
    private static final int TESTED_VERSIONS_GRANULARITY = 3;

    /**
     * Empty means that we could not obtain current Trino version and tests defined here will be marked as failed.
     */
    private final Optional<String> testedTrinoVersion;
    private TrinoContainer trinoContainer;

    @Factory(dataProvider = "testedTrinoVersions")
    public TestJdbcResultSetCompatibilityOldServer(Optional<String> testedTrinoVersion)
    {
        this.testedTrinoVersion = requireNonNull(testedTrinoVersion, "testedTrinoVersion is null");
    }

    @DataProvider
    public static Object[][] testedTrinoVersions()
    {
        try {
            String currentVersionString = Resources.toString(Resources.getResource("trino-test-jdbc-compatibility-old-server-version.txt"), UTF_8).trim();
            Matcher matcher = Pattern.compile("(\\d+)(?:-SNAPSHOT)?").matcher(currentVersionString);
            checkState(matcher.matches());
            int currentVersion = parseInt(matcher.group(1));
            ImmutableList.Builder<String> testedTrinoVersions = ImmutableList.builder();
            int testVersion = currentVersion - 1; // last release version
            for (int i = 0; i < NUMBER_OF_TESTED_VERSIONS; i++) {
                if (testVersion == 404) {
                    // 404 release was skipped.
                    testVersion--;
                }
                if (testVersion < FIRST_VERSION) {
                    break;
                }
                testedTrinoVersions.add(String.valueOf(testVersion));
                testVersion -= TESTED_VERSIONS_GRANULARITY;
            }

            return testedTrinoVersions.build().stream()
                    .map(Optional::of)
                    .collect(toDataProvider());
        }
        catch (Throwable e) {
            // We cannot throw here because TestNG does not handle exceptions coming out from @DataProvider used with @Factory well.
            // Instead we return marker Option.empty() as only parameterization. Then we will fail test run in setupTrinoContainer().
            System.err.println("Could not determine Trino versions to test; " + e.getMessage() + "\n" + getStackTraceAsString(e));
            return new Object[][] {
                    {Optional.empty()}
            };
        }
    }

    @BeforeClass
    public void setupTrinoContainer()
    {
        DockerImageName image = DockerImageName.parse("trinodb/trino").withTag(getTestedTrinoVersion());
        trinoContainer = new TrinoContainer(image);
        trinoContainer.start();

        // verify that version reported by Trino server matches requested one.
        try (ConnectedStatement statementWrapper = newStatement()) {
            try (ResultSet rs = statementWrapper.getStatement().executeQuery("SELECT node_version FROM system.runtime.nodes")) {
                assertTrue(rs.next());
                String actualTrinoVersion = rs.getString(1);
                assertEquals(actualTrinoVersion, getTestedTrinoVersion(), "Trino server version reported by container does not match expected one");
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Could not get version from Trino server", e);
        }
    }

    @AfterClass(alwaysRun = true)
    public void tearDownTrinoContainer()
    {
        if (trinoContainer != null) {
            trinoContainer.stop();
            trinoContainer = null;
        }
    }

    @Override
    protected Connection createConnection()
            throws SQLException
    {
        return DriverManager.getConnection(trinoContainer.getJdbcUrl(), "test", null);
    }

    @Override
    protected int getTestedServerVersion()
    {
        return parseInt(getTestedTrinoVersion());
    }

    @Override
    public String toString()
    {
        // This allows distinguishing tests run against different Trino server version from each other.
        // It is included in tests report and maven output.
        return format("TestJdbcResultSetCompatibility[%s]", testedTrinoVersion.orElse("unknown"));
    }

    protected String getTestedTrinoVersion()
    {
        return testedTrinoVersion.orElseThrow(() -> new IllegalStateException("Trino version not set"));
    }
}
