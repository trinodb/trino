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
package io.trino.plugin.base.logging;

import org.testng.annotations.Test;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestFormatInterpolator
{
    @Test
    public void testNullInterpolation()
    {
        FormatInterpolator<String> interpolator = new FormatInterpolator<>(null, SingleTestValue.values());
        assertThat(interpolator.interpolate("!")).isEqualTo("");
    }

    @Test
    public void testSingleValueInterpolation()
    {
        FormatInterpolator<String> interpolator = new FormatInterpolator<>("TEST_VALUE is $TEST_VALUE", SingleTestValue.values());
        assertThat(interpolator.interpolate("!")).isEqualTo("TEST_VALUE is singleValue!");
    }

    @Test
    public void testMultipleValueInterpolation()
    {
        FormatInterpolator<String> interpolator = new FormatInterpolator<>("TEST_VALUE is $TEST_VALUE and ANOTHER_VALUE is $ANOTHER_VALUE", MultipleTestValues.values());
        assertThat(interpolator.interpolate("!")).isEqualTo("TEST_VALUE is first! and ANOTHER_VALUE is second!");
    }

    @Test
    public void testUnknownValueInterpolation()
    {
        FormatInterpolator<String> interpolator = new FormatInterpolator<>("UNKNOWN_VALUE is $UNKNOWN_VALUE", MultipleTestValues.values());
        assertThat(interpolator.interpolate("!")).isEqualTo("UNKNOWN_VALUE is $UNKNOWN_VALUE");
    }

    @Test
    public void testValidation()
    {
        assertFalse(FormatInterpolator.hasValidPlaceholders("$UNKNOWN_VALUE", MultipleTestValues.values()));
        assertTrue(FormatInterpolator.hasValidPlaceholders("$TEST_VALUE", MultipleTestValues.values()));
        assertFalse(FormatInterpolator.hasValidPlaceholders("$TEST_VALUE and $UNKNOWN_VALUE", MultipleTestValues.values()));
        assertTrue(FormatInterpolator.hasValidPlaceholders("$TEST_VALUE and $ANOTHER_VALUE", MultipleTestValues.values()));
        assertTrue(FormatInterpolator.hasValidPlaceholders("$TEST_VALUE and $TEST_VALUE", MultipleTestValues.values()));
    }

    public enum SingleTestValue
            implements FormatInterpolator.InterpolatedValue<String>
    {
        TEST_VALUE;

        @Override
        public String value(String value)
        {
            return "singleValue" + value;
        }
    }

    public enum MultipleTestValues
            implements FormatInterpolator.InterpolatedValue<String>
    {
        TEST_VALUE("first"),
        ANOTHER_VALUE("second");

        private final String value;

        MultipleTestValues(String value)
        {
            this.value = value;
        }

        @Override
        public String value(String value)
        {
            return this.value + value;
        }
    }
}
