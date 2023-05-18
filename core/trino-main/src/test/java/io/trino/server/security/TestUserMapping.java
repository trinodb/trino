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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        assertThat(defaultUserMapping.mapUser("test@example.com")).isEqualTo("test@example.com");

        UserMapping singlePatternUserMapping = createUserMapping(Optional.of("(.*?)@.*"), Optional.empty());
        assertThat(singlePatternUserMapping.mapUser("test@example.com")).isEqualTo("test");

        UserMapping fileUserMapping = createUserMapping(Optional.empty(), Optional.of(testFile));
        assertThat(fileUserMapping.mapUser("test@example.com")).isEqualTo("test_file");
        assertThat(fileUserMapping.mapUser("user")).isEqualTo("user");
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
        assertThat(userMapping.mapUser("test@example.com")).isEqualTo("test");
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
        assertThat(userMapping.mapUser("test@example.com")).isEqualTo("test ^ test");
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
        assertThat(userMapping.mapUser("apple@example.com")).isEqualTo("apple");
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
        assertThat(userMapping.mapUser("TEST@EXAMPLE.COM")).isEqualTo("test");
    }

    @Test
    public void testUppercaseUsernameRule()
            throws UserMappingException
    {
        UserMapping userMapping = new UserMapping(ImmutableList.of(new Rule("(.*)@example\\.com", "$1", true, UserMapping.Case.UPPER)));
        assertThat(userMapping.mapUser("test@example.com")).isEqualTo("TEST");
    }

    @Test
    public void testDocsExample()
            throws Exception
    {
        // TODO: figure out a better way to validate documentation
        File docExample = new File("../../docs/src/main/sphinx/security/user-mapping.json");
        UserMapping userMapping = createUserMapping(Optional.empty(), Optional.of(docExample));

        assertThat(userMapping.mapUser("apple@example.com")).isEqualTo("apple");
        assertThat(userMapping.mapUser("apple@uk.example.com")).isEqualTo("apple_uk");
        assertThat(userMapping.mapUser("apple@de.example.com")).isEqualTo("apple_de");
        assertThatThrownBy(() -> userMapping.mapUser("apple@unknown.com"))
                .isInstanceOf(UserMappingException.class)
                .hasMessage("No user mapping patterns match the principal");
        assertThatThrownBy(() -> userMapping.mapUser("test@example.com"))
                .isInstanceOf(UserMappingException.class)
                .hasMessage("Principal is not allowed");
        assertThat(userMapping.mapUser("test@uppercase.com")).isEqualTo("TEST");
    }
}
