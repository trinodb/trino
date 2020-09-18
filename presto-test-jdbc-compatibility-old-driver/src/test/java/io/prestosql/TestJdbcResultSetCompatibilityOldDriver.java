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

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logging;
import io.prestosql.jdbc.TestJdbcResultSet;
import io.prestosql.server.testing.TestingPrestoServer;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Optional;

import static io.prestosql.JdbcDriverCapabilities.hasBrokenParametricTimeSupport;
import static io.prestosql.JdbcDriverCapabilities.hasBrokenParametricTimestampWithTimeZoneSupport;
import static io.prestosql.JdbcDriverCapabilities.hasBrokenTimeWithTimeZoneSupport;
import static io.prestosql.JdbcDriverCapabilities.testedVersion;
import static io.prestosql.TestingServerUtils.setTestingServer;
import static org.assertj.core.api.Assertions.assertThat;

// We need to keep @Test(singleThreaded = true) even though current version of superclass does not require it.
// This class is compiled against older versions of TestJdbcResultSet which require single threaded execution of tests.
@Test(singleThreaded = true)
public class TestJdbcResultSetCompatibilityOldDriver
        extends TestJdbcResultSet
{
    private static final Optional<Integer> VERSION_UNDER_TEST = testedVersion();

    @Test
    public void ensureProperTestVersionLoaded()
    {
        if (VERSION_UNDER_TEST.isEmpty()) {
            throw new SkipException("Information about JDBC version under test is missing");
        }

        assertThat(TestJdbcResultSet.class.getPackage().getImplementationVersion())
                .isEqualTo(VERSION_UNDER_TEST.get().toString());
    }

    @BeforeClass
    @Override
    public void setupServer()
    {
        Logging.initialize();

        // Instantiate server and set it via reflection.
        // This is required due to TestingPrestoServer API changes.
        setTestingServer(this, TestingPrestoServer
                .builder()
                .setProperties(ImmutableMap.of("deprecated.omit-datetime-type-precision", "true"))
                .build());
    }

    @BeforeMethod
    public void skipBrokenTests(Method method)
    {
        if (hasBrokenParametricTimestampWithTimeZoneSupport() && method.getName().equals("testObjectTypes")) {
            throw new SkipException("This version reports PARAMETRIC_DATETIME client capability but TIMESTAMP WITH TIME ZONE is not supported");
        }

        if (hasBrokenParametricTimeSupport() && (method.getName().equals("testTime") || method.getName().equals("testObjectTypes"))) {
            throw new SkipException("This version reports PARAMETRIC_DATETIME client capability but TIMESTAMP WITH TIME ZONE is not supported");
        }

        if (hasBrokenTimeWithTimeZoneSupport() && method.getName().equals("testObjectTypes")) {
            throw new SkipException("TIME WITH TIME ZONE TYPE is not supported properly");
        }
    }

    @Test
    // We add the extra public method to enforce the class to be run single-thread.
    // See TestNG bug https://github.com/cbeust/testng/issues/2361#issuecomment-688393166
    // We need to run this class single-threaded where we compile against older version of JDBC tests.
    public void forceTestNgToRespectSingleThreaded() {}
}
