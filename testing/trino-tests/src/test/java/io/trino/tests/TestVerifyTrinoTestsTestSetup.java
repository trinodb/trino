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
package io.trino.tests;

import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

public class TestVerifyTrinoTestsTestSetup
{
    @Test
    public void testJvmZone()
    {
        // Ensure that the zone defined in the POM is correctly set in the test JVM
        assertThat(ZoneId.systemDefault().getId()).isEqualTo("America/Bahia_Banderas");
    }

    @Test
    public void testJvmLocale()
    {
        // Ensure that locale defined in the POM is correctly set in the test JVM
        assertThat(Locale.getDefault()).isEqualTo(Locale.US);
    }
}
