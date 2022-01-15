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
    public void testTypeCoercionIntegerVariants()
    {
        // Integer variants
        assertEquals(getWiderDataType(ImmutableList.of("byte")), "byte");
        assertEquals(getWiderDataType(ImmutableList.of("short")), "short");
        assertEquals(getWiderDataType(ImmutableList.of("integer")), "integer");
        assertEquals(getWiderDataType(ImmutableList.of("long")), "long");
        assertEquals(getWiderDataType(ImmutableList.of("byte", "short")), "short");
        assertEquals(getWiderDataType(ImmutableList.of("byte", "short", "integer")), "integer");
        assertEquals(getWiderDataType(ImmutableList.of("byte", "short", "long", "integer")), "long");
        assertEquals(getWiderDataType(ImmutableList.of("short", "integer")), "integer");
        assertEquals(getWiderDataType(ImmutableList.of("short", "integer", "long")), "long");
        assertEquals(getWiderDataType(ImmutableList.of("integer", "long")), "long");
    }

    @Test
    public void testTypeCoercionFloatVariants()
    {
        // Float variants
        assertEquals(getWiderDataType(ImmutableList.of("half_float")), "half_float");
        assertEquals(getWiderDataType(ImmutableList.of("float")), "float");
        assertEquals(getWiderDataType(ImmutableList.of("double")), "double");
        assertEquals(getWiderDataType(ImmutableList.of("half_float", "float")), "float");
        assertEquals(getWiderDataType(ImmutableList.of("half_float", "float", "double")), "double");
        assertEquals(getWiderDataType(ImmutableList.of("float", "double")), "double");
    }

    @Test
    public void testTypeCoercionFromDifferentVariants()
    {
        assertEquals(getWiderDataType(ImmutableList.of("byte", "half_float")), "text");
        assertEquals(getWiderDataType(ImmutableList.of("short", "float")), "text");
        assertEquals(getWiderDataType(ImmutableList.of("short", "long", "float")), "text");
        assertEquals(getWiderDataType(ImmutableList.of("double", "long")), "text");
    }

    @Test
    public void testTypeCoercionFromUndefinedVariants()
    {
        // Complex data types not defined in the coercion hierarchy.
        assertEquals(getWiderDataType(ImmutableList.of("date", "ip")), "text");
        assertEquals(getWiderDataType(ImmutableList.of("scaled_float", "float")), "text");
        assertEquals(getWiderDataType(ImmutableList.of("short", "long", "scaled_float")), "text");
        assertEquals(getWiderDataType(ImmutableList.of("double", "ip")), "text");
        assertEquals(getWiderDataType(ImmutableList.of("date", "ip", "scaled_float")), "text");
    }
}
