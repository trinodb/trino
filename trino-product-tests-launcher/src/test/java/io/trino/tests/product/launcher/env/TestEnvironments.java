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
package io.prestosql.tests.product.launcher.env;

import org.testng.annotations.Test;

import static io.prestosql.tests.product.launcher.env.Environments.canonicalName;
import static org.assertj.core.api.Assertions.assertThat;

public class TestEnvironments
{
    @Test
    public void testCanonicalName()
    {
        assertThat(canonicalName("ala")).isEqualTo("ala");
        assertThat(canonicalName("Ala")).isEqualTo("ala");
        assertThat(canonicalName("DuzaAla")).isEqualTo("duza-ala");
        assertThat(canonicalName("duza-ala")).isEqualTo("duza-ala");
        assertThat(canonicalName("duza-Ala")).isEqualTo("duza-ala");
        assertThat(canonicalName("duza----Ala")).isEqualTo("duza-ala");
    }
}
