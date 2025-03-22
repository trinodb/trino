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
package io.trino.plugin.starrocks;

import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StarrocksTypeMapperTest
{
    @Test
    public void testToTrinoType()
    {
        String statement = "struct<row_type struct<id bigint(20), name varchar(65533)>, time_type bigint(20), array_type array<struct<id bigint(20), name varchar(65533)>>>";
        try {
            Type trinoType = StarrocksTypeMapper.mappingSemiStructure(statement);
            assertTrue(true);
            assertTrue("Expected RowType as result", trinoType instanceof RowType);
        }
        catch (Exception e) {
            fail("Should not throw any exception, but got: " + e.getMessage());
        }
    }
}
