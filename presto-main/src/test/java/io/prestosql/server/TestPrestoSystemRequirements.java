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
package io.prestosql.server;

import com.google.common.base.StandardSystemProperty;
import org.testng.SkipException;
import org.testng.annotations.Test;

import static io.prestosql.server.PrestoSystemRequirements.verifyJvmRequirements;
import static io.prestosql.server.PrestoSystemRequirements.verifySystemTimeIsReasonable;

public class TestPrestoSystemRequirements
{
    @Test
    public void testVerifyJvmRequirements()
    {
        // TODO: remove when we stop running tests with Java 8 on CI
        String javaVersion = StandardSystemProperty.JAVA_VERSION.value();
        JavaVersion version = JavaVersion.parse(javaVersion);
        if (version.getMajor() < 11) {
            throw new SkipException("Incompatible Java version: " + javaVersion);
        }

        verifyJvmRequirements();
    }

    @Test
    public void testSystemTimeSanityCheck()
    {
        verifySystemTimeIsReasonable();
    }
}
