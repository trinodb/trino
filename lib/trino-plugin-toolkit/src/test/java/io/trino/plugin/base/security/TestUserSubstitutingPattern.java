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
package io.trino.plugin.base.security;

import org.junit.jupiter.api.Test;

import java.util.regex.PatternSyntaxException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestUserSubstitutingPattern
{
    @Test
    void testPatternWithoutPlaceholder()
    {
        UserSubstitutingPattern pattern = UserSubstitutingPattern.of("bob-schema");
        assertThat(pattern.matches("alice", "bob-schema")).isTrue();
        assertThat(pattern.matches("bob", "bob-schema")).isTrue();
        assertThat(pattern.matches("alice", "alice-schema")).isFalse();
    }

    @Test
    void testUserPlaceholder()
    {
        UserSubstitutingPattern pattern = UserSubstitutingPattern.of("^{user}$");
        assertThat(pattern.matches("alice", "alice")).isTrue();
        assertThat(pattern.matches("alice", "bob")).isFalse();
        assertThat(pattern.matches("bob", "bob")).isTrue();
        assertThat(pattern.matches("bob", "alice")).isFalse();
    }

    @Test
    void testUserNameIsMatchedLiterally()
    {
        UserSubstitutingPattern pattern = UserSubstitutingPattern.of("^{user}$");
        assertThat(pattern.matches("a-user", "a-user")).isTrue();
        assertThat(pattern.matches("a.user", "a.user")).isTrue();
        // an unquoted user name would make "." match any character
        assertThat(pattern.matches("a.user", "aXuser")).isFalse();
        assertThat(pattern.matches(".*", ".*")).isTrue();
        assertThat(pattern.matches(".*", "anything")).isFalse();
        assertThat(pattern.matches("\\E", "\\E")).isTrue();
    }

    @Test
    void testPlaceholderWithinPattern()
    {
        UserSubstitutingPattern pattern = UserSubstitutingPattern.of("^{user}_(sandbox|scratch)$");
        assertThat(pattern.matches("alice", "alice_sandbox")).isTrue();
        assertThat(pattern.matches("alice", "alice_scratch")).isTrue();
        assertThat(pattern.matches("alice", "bob_sandbox")).isFalse();
    }

    @Test
    void testMultiplePlaceholders()
    {
        UserSubstitutingPattern pattern = UserSubstitutingPattern.of("^({user}|shared_{user})$");
        assertThat(pattern.matches("alice", "alice")).isTrue();
        assertThat(pattern.matches("alice", "shared_alice")).isTrue();
        assertThat(pattern.matches("alice", "shared_bob")).isFalse();
    }

    @Test
    void testEscapedBracesAreNotPlaceholder()
    {
        UserSubstitutingPattern pattern = UserSubstitutingPattern.of("^\\{user\\}$");
        assertThat(pattern.matches("alice", "{user}")).isTrue();
        assertThat(pattern.matches("alice", "alice")).isFalse();
    }

    @Test
    void testInvalidPatternWithoutPlaceholder()
    {
        assertThatThrownBy(() -> UserSubstitutingPattern.of("^{admin}$"))
                .isInstanceOf(PatternSyntaxException.class)
                .hasMessageContaining("Illegal repetition");
        assertThatThrownBy(() -> UserSubstitutingPattern.of("unclosed["))
                .isInstanceOf(PatternSyntaxException.class)
                .hasMessageContaining("Unclosed character class");
    }

    @Test
    void testInvalidPatternWithPlaceholder()
    {
        assertThatThrownBy(() -> UserSubstitutingPattern.of("^{user}[$"))
                .isInstanceOf(PatternSyntaxException.class)
                .hasMessageContaining("Unclosed character class");
    }

    @Test
    void testEquality()
    {
        assertThat(UserSubstitutingPattern.of("^{user}$")).isEqualTo(UserSubstitutingPattern.of("^{user}$"));
        assertThat(UserSubstitutingPattern.of("^{user}$")).isNotEqualTo(UserSubstitutingPattern.of("^{user}x$"));
        assertThat(UserSubstitutingPattern.of("schema")).isEqualTo(UserSubstitutingPattern.of("schema"));
    }
}
