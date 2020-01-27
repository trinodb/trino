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
package io.prestosql.server.security;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import io.prestosql.server.security.UserMapping.Rule;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Optional;

import static io.prestosql.server.security.UserMapping.createUserMapping;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestUserMapping
{
    private File testFile;

    @BeforeClass
    public void setUp()
            throws URISyntaxException
    {
        testFile = new File(Resources.getResource("user-mapping.json").toURI());
    }

    @Test
    public void testStaticFactory()
            throws Exception
    {
        UserMapping defaultUserMapping = createUserMapping(Optional.empty(), Optional.empty());
        assertEquals(defaultUserMapping.mapUser("test@example.com"), "test@example.com");

        UserMapping singlePatternUserMapping = createUserMapping(Optional.of("(.*?)@.*"), Optional.empty());
        assertEquals(singlePatternUserMapping.mapUser("test@example.com"), "test");

        UserMapping fileUserMapping = createUserMapping(Optional.empty(), Optional.of(testFile));
        assertEquals(fileUserMapping.mapUser("test@example.com"), "test_file");
        assertEquals(fileUserMapping.mapUser("user"), "user");
        assertThrows(UserMappingException.class, () -> fileUserMapping.mapUser("test"));

        assertThrows(IllegalArgumentException.class, () -> createUserMapping(Optional.of("(.*?)@.*"), Optional.of(testFile)));
    }

    @Test
    public void testSimplePatternRule()
            throws Exception
    {
        UserMapping userMapping = new UserMapping(ImmutableList.of(new Rule("(.*?)@.*")));
        assertEquals(userMapping.mapUser("test@example.com"), "test");
        assertThrows(UserMappingException.class, () -> userMapping.mapUser("no at sign"));
        assertThrows(UserMappingException.class, () -> userMapping.mapUser("@no user string"));
    }

    @Test
    public void testReplacePatternRule()
            throws Exception
    {
        UserMapping userMapping = new UserMapping(ImmutableList.of(new Rule("(.*?)@.*", "$1 ^ $1", true)));
        assertEquals(userMapping.mapUser("test@example.com"), "test ^ test");
        assertThrows(UserMappingException.class, () -> userMapping.mapUser("no at sign"));

        UserMapping emptyMapping = new UserMapping(ImmutableList.of(new Rule("(.*?)@.*", "  ", true)));
        assertThrows(UserMappingException.class, () -> emptyMapping.mapUser("test@example.com"));
    }

    @Test
    public void testNotAllowedRule()
    {
        UserMapping userMapping = new UserMapping(ImmutableList.of(new Rule("(.*?)@.*", "$1", false)));
        assertThrows(UserMappingException.class, () -> userMapping.mapUser("test@example.com"));

        UserMapping emptyMapping = new UserMapping(ImmutableList.of(new Rule("(.*?)@.*", "", false)));
        assertThrows(UserMappingException.class, () -> emptyMapping.mapUser("test@example.com"));
    }

    @Test
    public void testMultipleRule()
            throws Exception
    {
        UserMapping userMapping = new UserMapping(ImmutableList.of(new Rule("test@example.com", "", false), new Rule("(.*?)@example.com")));
        assertEquals(userMapping.mapUser("apple@example.com"), "apple");
        assertThrows(UserMappingException.class, () -> userMapping.mapUser("test@example.com"));
        assertThrows(UserMappingException.class, () -> userMapping.mapUser("apple@other.example.com"));
    }
}
