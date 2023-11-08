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
package io.trino.plugin.thrift.api.valuesets;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.thrift.api.datatypes.TrinoThriftJson;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.thrift.api.TrinoThriftBlock.jsonData;
import static io.trino.plugin.thrift.api.valuesets.TrinoThriftValueSet.fromValueSet;
import static io.trino.type.JsonType.JSON;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestTrinoThriftEquatableValueSet
{
    private static final String JSON1 = "\"key1\":\"value1\"";
    private static final String JSON2 = "\"key2\":\"value2\"";

    @Test
    public void testFromValueSetAll()
    {
        TrinoThriftValueSet thriftValueSet = fromValueSet(ValueSet.all(JSON));
        assertNotNull(thriftValueSet.getEquatableValueSet());
        assertFalse(thriftValueSet.getEquatableValueSet().isInclusive());
        assertTrue(thriftValueSet.getEquatableValueSet().getValues().isEmpty());
    }

    @Test
    public void testFromValueSetNone()
    {
        TrinoThriftValueSet thriftValueSet = fromValueSet(ValueSet.none(JSON));
        assertNotNull(thriftValueSet.getEquatableValueSet());
        assertTrue(thriftValueSet.getEquatableValueSet().isInclusive());
        assertTrue(thriftValueSet.getEquatableValueSet().getValues().isEmpty());
    }

    @Test
    public void testFromValueSetOf()
    {
        TrinoThriftValueSet thriftValueSet = fromValueSet(ValueSet.of(JSON, utf8Slice(JSON1), utf8Slice(JSON2)));
        assertNotNull(thriftValueSet.getEquatableValueSet());
        assertTrue(thriftValueSet.getEquatableValueSet().isInclusive());
        assertEquals(thriftValueSet.getEquatableValueSet().getValues(), ImmutableList.of(
                jsonData(new TrinoThriftJson(null, new int[] {JSON1.length()}, JSON1.getBytes(UTF_8))),
                jsonData(new TrinoThriftJson(null, new int[] {JSON2.length()}, JSON2.getBytes(UTF_8)))));
    }
}
