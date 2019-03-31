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
package io.prestosql.util;

import io.prestosql.spi.Name;
import io.prestosql.sql.tree.Identifier;
import org.testng.annotations.Test;

import static io.prestosql.testing.assertions.Assert.assertEquals;
import static io.prestosql.util.NameUtil.createName;

public class TestNameUtil
{
    @Test
    public void testCreatingNameFromIdentifier()
    {
        Identifier identifier = new Identifier("identifier", true);
        assertEquals(createName(identifier), new Name("identifier", true));
        identifier = new Identifier("identifier", false);
        assertEquals(createName(identifier), new Name("identifier", false));
    }

    @Test
    public void testCreatingNameFromString()
    {
        assertEquals(createName("identifier"), new Name("identifier", false));
        assertEquals(createName("\"idenTifier\""), new Name("idenTifier", true));
        assertEquals(createName("\"iden\"\"Tifier\""), new Name("iden\"Tifier", true));
        assertEquals(createName("sf3000.0"), new Name("sf3000.0", false));
        assertEquals(createName("\"sf3000.0\""), new Name("sf3000.0", true));
        assertEquals(createName("ident.ifer"), new Name("ident.ifer", false));
        assertEquals(createName("\"ident.ifer\""), new Name("ident.ifer", true));
    }
}
