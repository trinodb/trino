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
package io.trino.server.security;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import io.trino.server.security.UserMapping.Rule;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Optional;

import static io.trino.server.security.UserMapping.Case.KEEP;
import static io.trino.server.security.UserMapping.createUserMapping;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

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
        assertThatThrownBy(() -> fileUserMapping.mapUser("test"))
                .isInstanceOf(UserMappingException.class)
                .hasMessage("Principal is not allowed");

        assertThatThrownBy(() -> createUserMapping(Optional.of("(.*?)@.*"), Optional.of(testFile)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("user mapping pattern and file can not both be set");
    }

    @Test
    public void testSimplePatternRule()
            throws Exception
    {
        UserMapping userMapping = new UserMapping(ImmutableList.of(new Rule("(.*?)@.*")));
        assertEquals(userMapping.mapUser("test@example.com"), "test");
        assertThatThrownBy(() -> userMapping.mapUser("no at sign"))
                .isInstanceOf(UserMappingException.class)
                .hasMessage("No user mapping patterns match the principal");
        assertThatThrownBy(() -> userMapping.mapUser("@no user string"))
                .isInstanceOf(UserMappingException.class)
                .hasMessage("Principal matched, but mapped user is empty");
    }

    @Test
    public void testReplacePatternRule()
            throws Exception
    {
        UserMapping userMapping = new UserMapping(ImmutableList.of(new Rule("(.*?)@.*", "$1 ^ $1", true, KEEP)));
        assertEquals(userMapping.mapUser("test@example.com"), "test ^ test");
        assertThatThrownBy(() -> userMapping.mapUser("no at sign"))
                .isInstanceOf(UserMappingException.class)
                .hasMessage("No user mapping patterns match the principal");

        UserMapping emptyMapping = new UserMapping(ImmutableList.of(new Rule("(.*?)@.*", "  ", true, KEEP)));
        assertThatThrownBy(() -> emptyMapping.mapUser("test@example.com"))
                .isInstanceOf(UserMappingException.class)
                .hasMessage("Principal matched, but mapped user is empty");
    }

    @Test
    public void testNotAllowedRule()
    {
        UserMapping userMapping = new UserMapping(ImmutableList.of(new Rule("(.*?)@.*", "$1", false, KEEP)));
        assertThatThrownBy(() -> userMapping.mapUser("test@example.com"))
                .isInstanceOf(UserMappingException.class)
                .hasMessage("Principal is not allowed");

        UserMapping emptyMapping = new UserMapping(ImmutableList.of(new Rule("(.*?)@.*", "", false, KEEP)));
        assertThatThrownBy(() -> emptyMapping.mapUser("test@example.com"))
                .isInstanceOf(UserMappingException.class)
                .hasMessage("Principal is not allowed");
    }

    @Test
    public void testMultipleRule()
            throws Exception
    {
        UserMapping userMapping = new UserMapping(ImmutableList.of(new Rule("test@example.com", "", false, KEEP), new Rule("(.*?)@example.com")));
        assertEquals(userMapping.mapUser("apple@example.com"), "apple");
        assertThatThrownBy(() -> userMapping.mapUser("test@example.com"))
                .isInstanceOf(UserMappingException.class)
                .hasMessage("Principal is not allowed");
        assertThatThrownBy(() -> userMapping.mapUser("apple@other.example.com"))
                .isInstanceOf(UserMappingException.class)
                .hasMessage("No user mapping patterns match the principal");
    }

    @Test
    public void testLowercaseUsernameRule()
            throws UserMappingException
    {
        UserMapping userMapping = new UserMapping(ImmutableList.of(new Rule("(.*)@EXAMPLE\\.COM", "$1", true, UserMapping.Case.LOWER)));
        assertEquals(userMapping.mapUser("TEST@EXAMPLE.COM"), "test");
    }

    @Test
    public void testUppercaseUsernameRule()
            throws UserMappingException
    {
        UserMapping userMapping = new UserMapping(ImmutableList.of(new Rule("(.*)@example\\.com", "$1", true, UserMapping.Case.UPPER)));
        assertEquals(userMapping.mapUser("test@example.com"), "TEST");
    }

    @Test
    public void testDocsExample()
            throws Exception
    {
        // TODO: figure out a better way to validate documentation
        File docExample = new File("../../docs/src/main/sphinx/security/user-mapping.json");
        UserMapping userMapping = createUserMapping(Optional.empty(), Optional.of(docExample));

        assertEquals(userMapping.mapUser("apple@example.com"), "apple");
        assertEquals(userMapping.mapUser("apple@uk.example.com"), "apple_uk");
        assertEquals(userMapping.mapUser("apple@de.example.com"), "apple_de");
        assertThatThrownBy(() -> userMapping.mapUser("apple@unknown.com"))
                .isInstanceOf(UserMappingException.class)
                .hasMessage("No user mapping patterns match the principal");
        assertThatThrownBy(() -> userMapping.mapUser("test@example.com"))
                .isInstanceOf(UserMappingException.class)
                .hasMessage("Principal is not allowed");
        assertEquals(userMapping.mapUser("test@uppercase.com"), "TEST");
    }
}
