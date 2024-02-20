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
package io.trino.verifier;

import org.junit.jupiter.api.Test;

import static io.trino.verifier.Validator.precisionCompare;
import static java.lang.Double.NaN;
import static org.assertj.core.api.Assertions.assertThat;

public class TestValidator
{
    @Test
    public void testDoubleComparison()
    {
        assertThat(precisionCompare(0.9045, 0.9045000000000001, 3)).isEqualTo(0);
        assertThat(precisionCompare(0.9045, 0.9045000000000001, 2)).isEqualTo(0);
        assertThat(precisionCompare(0.9041, 0.9042, 3)).isEqualTo(0);
        assertThat(precisionCompare(0.9041, 0.9042, 4)).isEqualTo(0);
        assertThat(precisionCompare(0.9042, 0.9041, 4)).isEqualTo(0);
        assertThat(precisionCompare(-0.9042, -0.9041, 4)).isEqualTo(0);
        assertThat(precisionCompare(-0.9042, -0.9041, 3)).isEqualTo(0);
        assertThat(precisionCompare(0.899, 0.901, 3)).isEqualTo(0);
        assertThat(precisionCompare(NaN, NaN, 4)).isEqualTo(Double.compare(NaN, NaN));
    }
}
