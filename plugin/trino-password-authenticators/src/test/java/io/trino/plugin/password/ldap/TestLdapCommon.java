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
package io.trino.plugin.password.ldap;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestLdapCommon
{
    @Test
    public void testContainsSpecialCharacters()
    {
        assertThat(LdapCommon.containsSpecialCharacters("The quick brown fox jumped over the lazy dogs"))
                .as("English pangram")
                .isEqualTo(false);
        assertThat(LdapCommon.containsSpecialCharacters("Pchnąć w tę łódź jeża lub ośm skrzyń fig"))
                .as("Perfect polish pangram")
                .isEqualTo(false);
        assertThat(LdapCommon.containsSpecialCharacters("いろはにほへと ちりぬるを わかよたれそ つねならむ うゐのおくやま けふこえて あさきゆめみし ゑひもせす（ん）"))
                .as("Japanese hiragana pangram - Iroha")
                .isEqualTo(false);
        assertThat(LdapCommon.containsSpecialCharacters("*"))
                .as("LDAP wildcard")
                .isEqualTo(true);
        assertThat(LdapCommon.containsSpecialCharacters("   John Doe"))
                .as("Beginning with whitespace")
                .isEqualTo(true);
        assertThat(LdapCommon.containsSpecialCharacters("John Doe  \r"))
                .as("Ending with whitespace")
                .isEqualTo(true);
        assertThat(LdapCommon.containsSpecialCharacters("Hi (This) = is * a \\ test # ç à ô"))
                .as("Multiple special characters")
                .isEqualTo(true);
        assertThat(LdapCommon.containsSpecialCharacters("John\u0000Doe"))
                .as("NULL character")
                .isEqualTo(true);
        assertThat(LdapCommon.containsSpecialCharacters("John Doe <john.doe@company.com>"))
                .as("Angle brackets")
                .isEqualTo(true);
    }
}
