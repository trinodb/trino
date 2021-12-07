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
package io.trino.plugin.elasticsearch.utility;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static io.trino.plugin.elasticsearch.utility.ElasticsearchTypeCoercionHierarchy.getWiderDataType;
import static io.trino.testing.assertions.Assert.assertEquals;

public class TestElasticsearchTypeCoercionHierarchy
{
    @Test
    public void testTypeCoercionWholeNumbers()
    {
        // Whole numbers
        assertEquals(getWiderDataType(ImmutableList.of("byte")), "BYTE");
        assertEquals(getWiderDataType(ImmutableList.of("short")), "SHORT");
        assertEquals(getWiderDataType(ImmutableList.of("integer")), "INTEGER");
        assertEquals(getWiderDataType(ImmutableList.of("long")), "LONG");
        assertEquals(getWiderDataType(ImmutableList.of("byte", "short")), "SHORT");
        assertEquals(getWiderDataType(ImmutableList.of("byte", "short", "integer")), "INTEGER");
        assertEquals(getWiderDataType(ImmutableList.of("byte", "short", "long", "integer")), "LONG");
        assertEquals(getWiderDataType(ImmutableList.of("short", "integer")), "INTEGER");
        assertEquals(getWiderDataType(ImmutableList.of("short", "integer", "long")), "LONG");
        assertEquals(getWiderDataType(ImmutableList.of("integer", "long")), "LONG");
    }

    @Test
    public void testTypeCoercionDecimalNumbers()
    {
        // Decimal numbers
        assertEquals(getWiderDataType(ImmutableList.of("half_float")), "HALF_FLOAT");
        assertEquals(getWiderDataType(ImmutableList.of("float")), "FLOAT");
        assertEquals(getWiderDataType(ImmutableList.of("double")), "DOUBLE");
        assertEquals(getWiderDataType(ImmutableList.of("half_float", "float")), "FLOAT");
        assertEquals(getWiderDataType(ImmutableList.of("half_float", "float", "double")), "DOUBLE");
        assertEquals(getWiderDataType(ImmutableList.of("float", "double")), "DOUBLE");
    }

    @Test
    public void testTypeCoercionFromDifferentBuckets()
    {
        assertEquals(getWiderDataType(ImmutableList.of("byte", "half_float")), "text");
        assertEquals(getWiderDataType(ImmutableList.of("short", "float")), "text");
        assertEquals(getWiderDataType(ImmutableList.of("short", "long", "float")), "text");
        assertEquals(getWiderDataType(ImmutableList.of("double", "long")), "text");
    }
}
