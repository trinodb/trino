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
package io.trino.spi.type;

import org.testng.annotations.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.testng.Assert.assertEquals;

public class TestMapType
{
    @Test
    public void testMapDisplayName()
    {
        TypeOperators typeOperators = new TypeOperators();
        MapType mapType = new MapType(BIGINT, createVarcharType(42), typeOperators);
        assertEquals(mapType.getDisplayName(), "map(bigint, varchar(42))");

        mapType = new MapType(BIGINT, VARCHAR, typeOperators);
        assertEquals(mapType.getDisplayName(), "map(bigint, varchar)");
    }
}
