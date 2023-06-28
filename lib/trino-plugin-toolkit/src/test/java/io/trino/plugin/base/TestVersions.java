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
package io.trino.plugin.base;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestVersions
{
    @Test
    public void testExactMatch()
    {
        assertTrue(Versions.checkMatch("420", "420"));
    }

    @Test
    public void testExactMatchWithSuffix()
    {
        assertTrue(Versions.checkMatch("420-SNAPSHOT", "420-SNAPSHOT"));
    }

    @Test
    public void testDifferentSuffix()
    {
        assertTrue(Versions.checkMatch("420-rc1", "420-SNAPSHOT"));
    }

    @Test
    public void testBaseVsSuffix()
    {
        assertTrue(Versions.checkMatch("420", "420-SNAPSHOT"));
        assertTrue(Versions.checkMatch("420-SNAPSHOT", "420"));
    }

    @Test
    public void testFailDifferentBase()
    {
        assertFalse(Versions.checkMatch("420", "419"));
    }

    @Test
    public void testFailDifferentBaseWithSuffix()
    {
        assertFalse(Versions.checkMatch("420-SNAPSHOT", "421-SNAPSHOT"));
        assertFalse(Versions.checkMatch("420", "421-SNAPSHOT"));
        assertFalse(Versions.checkMatch("421-SNAPSHOT", "420"));
    }
}
