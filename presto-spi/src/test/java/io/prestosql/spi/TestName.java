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
package io.prestosql.spi;

import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import static io.prestosql.spi.Name.equivalentNames;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestName
{
    @Test
    public void testLegacyName()
    {
        assertEquals(new Name("test", true).getLegacyName(), "test");
        assertEquals(new Name("test", false).getLegacyName(), "test");
        assertEquals(new Name("Test", true).getLegacyName(), "test");
        assertEquals(new Name("Test", false).getLegacyName(), "test");
    }

    @Test
    public void testNormalizedName()
    {
        assertEquals(new Name("test", true).getNormalizedName(), "test");
        assertEquals(new Name("test", false).getNormalizedName(), "TEST");
        assertEquals(new Name("Test", true).getNormalizedName(), "Test");
        assertEquals(new Name("Test", false).getNormalizedName(), "TEST");
    }

    @Test
    public void assertJsonRoundTrip()
    {
        JsonCodec<Name> codec = JsonCodec.jsonCodec(Name.class);
        Name delimitedName = new Name("test", true);
        assertEquals(codec.fromJson(codec.toJson(delimitedName)), delimitedName);
        Name nonDelimitedName = new Name("test", false);
        assertEquals(codec.fromJson(codec.toJson(nonDelimitedName)), nonDelimitedName);
    }

    @Test
    public void testNameEquivalence()
    {
        assertTrue(equivalentNames(new Name("test", false), new Name("Test", false)));
        assertFalse(equivalentNames(new Name("test", true), new Name("Test", false)));
        assertFalse(equivalentNames(new Name("test", false), new Name("Test", true)));
        assertTrue(equivalentNames(new Name("test", true), new Name("test", true)));
    }
}
