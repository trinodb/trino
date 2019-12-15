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
package io.prestosql.plugin.salesforce.driver.metadata;

import org.junit.Test;

import java.sql.Types;

import static org.junit.Assert.assertEquals;

public class ForceDatabaseMetaDataTest
{
    @Test
    public void testLookupTypeInfo()
    {
        ForceDatabaseMetaData.TypeInfo actual = ForceDatabaseMetaData.lookupTypeInfo("int");

        assertEquals("int", actual.typeName);
        assertEquals(Types.INTEGER, actual.sqlDataType);
    }

    @Test
    public void testLookupTypeInfo_IfTypeUnknown()
    {
        ForceDatabaseMetaData.TypeInfo actual = ForceDatabaseMetaData.lookupTypeInfo("my strange type");

        assertEquals("other", actual.typeName);
        assertEquals(Types.OTHER, actual.sqlDataType);
    }
}
