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
package io.prestosql.spi.security;

import io.airlift.json.JsonCodec;
import io.prestosql.spi.Name;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.prestosql.spi.Name.createDelimitedName;
import static io.prestosql.spi.Name.createNonDelimitedName;
import static org.testng.Assert.assertEquals;

public class TestSelectedRole
{
    private static final JsonCodec<SelectedRole> SELECTED_ROLE_JSON_CODEC = jsonCodec(SelectedRole.class);
    private static final Pattern ROLE_PATTERN = Pattern.compile("(ROLE|ALL|NONE)(\\{(.+?)\\})?");

    @Test
    public void testJsonSerialization()
            throws Exception
    {
        assertJsonRoundTrip(new SelectedRole(SelectedRole.Type.ALL, Optional.empty()));
        assertJsonRoundTrip(new SelectedRole(SelectedRole.Type.NONE, Optional.empty()));
        assertJsonRoundTrip(new SelectedRole(SelectedRole.Type.ROLE, Optional.of(createNonDelimitedName("role"))));
        assertJsonRoundTrip(new SelectedRole(SelectedRole.Type.ROLE, Optional.of(createDelimitedName("role"))));
    }

    private static void assertJsonRoundTrip(SelectedRole expected)
    {
        assertEquals(SELECTED_ROLE_JSON_CODEC.fromJson(SELECTED_ROLE_JSON_CODEC.toJson(expected)), expected);
    }

    @Test
    public void testToStringSerialization()
            throws Exception
    {
        assertToStringRoundTrip(new SelectedRole(SelectedRole.Type.ALL, Optional.empty()));
        assertToStringRoundTrip(new SelectedRole(SelectedRole.Type.NONE, Optional.empty()));
        assertToStringRoundTrip(new SelectedRole(SelectedRole.Type.ROLE, Optional.of(createNonDelimitedName("role"))));
        assertToStringRoundTrip(new SelectedRole(SelectedRole.Type.ROLE, Optional.of(createDelimitedName("role"))));
    }

    private static void assertToStringRoundTrip(SelectedRole expected)
    {
        Matcher m = ROLE_PATTERN.matcher(expected.toString());
        SelectedRole role = null;
        if (m.matches()) {
            SelectedRole.Type type = SelectedRole.Type.valueOf(m.group(1));
            Optional<Name> roleName = Optional.ofNullable(m.group(3)).map(TestSelectedRole::createNamePart);
            role = new SelectedRole(type, roleName);
        }
        assertEquals(role, expected);
    }

    private static Name createNamePart(String name)
    {
        if (name.startsWith("\"") && name.endsWith(("\""))) {
            return new Name(name.substring(1, name.length() - 1).replace("\"\"", "\""), true);
        }
        return new Name(name.replace("\"\"", "\""), false);
    }
}
