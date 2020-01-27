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
import io.prestosql.server.security.UserExtraction.Rule;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;

import static io.prestosql.server.security.UserExtraction.createUserExtraction;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestUserExtraction
{
    private static final File TEST_FILE = new File(Resources.getResource("user-extraction.json").getPath());

    @Test
    public void testStaticFactory()
            throws Exception
    {
        UserExtraction defaultUserExtraction = createUserExtraction(Optional.empty(), Optional.empty());
        assertEquals(defaultUserExtraction.extractUser("test@example.com"), "test@example.com");

        UserExtraction singlePatternUserExtraction = createUserExtraction(Optional.of("(.*?)@.*"), Optional.empty());
        assertEquals(singlePatternUserExtraction.extractUser("test@example.com"), "test");

        UserExtraction fileUserExtraction = createUserExtraction(Optional.empty(), Optional.of(TEST_FILE));
        assertEquals(fileUserExtraction.extractUser("test@example.com"), "test_file");
        assertEquals(fileUserExtraction.extractUser("user"), "user");
        assertThrows(UserExtractionException.class, () -> fileUserExtraction.extractUser("test"));

        assertThrows(IllegalArgumentException.class, () -> createUserExtraction(Optional.of("(.*?)@.*"), Optional.of(TEST_FILE)));
    }

    @Test
    public void testSimplePatternRule()
            throws Exception
    {
        UserExtraction userExtraction = new UserExtraction(ImmutableList.of(new Rule("(.*?)@.*")));
        assertEquals(userExtraction.extractUser("test@example.com"), "test");
        assertThrows(UserExtractionException.class, () -> userExtraction.extractUser("no at sign"));
        assertThrows(UserExtractionException.class, () -> userExtraction.extractUser("@no user string"));
    }

    @Test
    public void testReplacePatternRule()
            throws Exception
    {
        UserExtraction userExtraction = new UserExtraction(ImmutableList.of(new Rule("(.*?)@.*", "$1 ^ $1", true)));
        assertEquals(userExtraction.extractUser("test@example.com"), "test ^ test");
        assertThrows(UserExtractionException.class, () -> userExtraction.extractUser("no at sign"));

        UserExtraction emptyExtraction = new UserExtraction(ImmutableList.of(new Rule("(.*?)@.*", "  ", true)));
        assertThrows(UserExtractionException.class, () -> emptyExtraction.extractUser("test@example.com"));
    }

    @Test
    public void testNotAllowedRule()
    {
        UserExtraction userExtraction = new UserExtraction(ImmutableList.of(new Rule("(.*?)@.*", "$1", false)));
        assertThrows(UserExtractionException.class, () -> userExtraction.extractUser("test@example.com"));

        UserExtraction emptyExtraction = new UserExtraction(ImmutableList.of(new Rule("(.*?)@.*", "", false)));
        assertThrows(UserExtractionException.class, () -> emptyExtraction.extractUser("test@example.com"));
    }

    @Test
    public void testMultipleRule()
            throws Exception
    {
        UserExtraction userExtraction = new UserExtraction(ImmutableList.of(new Rule("test@example.com", "", false), new Rule("(.*?)@example.com")));
        assertEquals(userExtraction.extractUser("apple@example.com"), "apple");
        assertThrows(UserExtractionException.class, () -> userExtraction.extractUser("test@example.com"));
        assertThrows(UserExtractionException.class, () -> userExtraction.extractUser("apple@other.example.com"));
    }
}
