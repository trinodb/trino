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
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.AfterParameterizedClassInvocation;
import org.junit.jupiter.params.BeforeParameterizedClassInvocation;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.trino.TrinoContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Integer.parseInt;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@ParameterizedClass
@MethodSource("testedTrinoVersions")
@TestInstance(PER_CLASS)
public class TestJdbcResultSetCompatibilityOldServer
        extends BaseTestJdbcResultSet
{
    private static final int FIRST_VERSION = 351;
    private static final int NUMBER_OF_TESTED_VERSIONS = 5;
    private static final int TESTED_VERSIONS_GRANULARITY = 3;

    @Parameter
    String testedTrinoVersion;
    private static TrinoContainer trinoContainer;

    public static List<String> testedTrinoVersions()
    {
        try {
            String currentVersionString = Resources.toString(Resources.getResource("trino-test-jdbc-compatibility-old-server-version.txt"), UTF_8).trim();
            Matcher matcher = Pattern.compile("(\\d+)(?:-SNAPSHOT)?").matcher(currentVersionString);
            checkState(matcher.matches());
            int currentVersion = parseInt(matcher.group(1));
            ImmutableList.Builder<String> testedTrinoVersions = ImmutableList.builder();
            int testVersion = currentVersion - 1; // last release version
            for (int i = 0; i < NUMBER_OF_TESTED_VERSIONS; i++) {
                if (testVersion == 456) {
                    // 456 is invalid - release process errors resulted in invalid artifacts.
                    testVersion--;
                }
                if (testVersion < FIRST_VERSION) {
                    break;
                }
                testedTrinoVersions.add(String.valueOf(testVersion));
                testVersion -= TESTED_VERSIONS_GRANULARITY;
            }

            return testedTrinoVersions.build();
        }
        catch (Throwable e) {
            throw new RuntimeException("Could not determine Trino versions to test", e);
        }
    }

    @BeforeParameterizedClassInvocation
    public void setupTrinoContainer()
    {
        DockerImageName image = DockerImageName.parse("trinodb/trino").withTag(testedTrinoVersion);
        trinoContainer = new TrinoContainer(image);
        trinoContainer.start();

        // verify that version reported by Trino server matches requested one.
        try (ConnectedStatement statementWrapper = newStatement()) {
            try (ResultSet rs = statementWrapper.getStatement().executeQuery("SELECT node_version FROM system.runtime.nodes")) {
                assertThat(rs.next()).isTrue();
                String actualTrinoVersion = rs.getString(1);
                assertThat(actualTrinoVersion)
                        .describedAs("Trino server version reported by container does not match expected one")
                        .isEqualTo(testedTrinoVersion);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Could not get version from Trino server", e);
        }
    }

    @AfterParameterizedClassInvocation
    public void tearDownTrinoContainer()
    {
        if (trinoContainer != null) {
            trinoContainer.stop();
            String imageName = trinoContainer.getDockerImageName();
            trinoContainer = null;

            removeDockerImage(imageName);
        }
    }

    @Override
    protected Connection createConnection()
            throws SQLException
    {
        return DriverManager.getConnection(trinoContainer.getJdbcUrl(), "test", null);
    }

    private static void removeDockerImage(String imageName)
    {
        DockerClientFactory.lazyClient().removeImageCmd(imageName).exec();
    }
}
