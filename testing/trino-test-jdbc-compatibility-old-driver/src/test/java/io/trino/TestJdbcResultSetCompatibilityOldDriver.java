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

import io.trino.jdbc.TestJdbcResultSet;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.JdbcDriverCapabilities.testedVersion;
import static org.assertj.core.api.Assertions.assertThat;

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
}
