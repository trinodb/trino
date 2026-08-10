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
package io.trino.util;

import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestSoundex
{
    // Expected values produced by commons-codec Soundex.US_ENGLISH, which this implementation replaces
    @Test
    void testSoundex()
    {
        assertThat(soundex("Robert")).isEqualTo("R163");
        assertThat(soundex("Rupert")).isEqualTo("R163");
        // H and W are silent and do not separate consonants with the same code
        assertThat(soundex("Ashcraft")).isEqualTo("A261");
        assertThat(soundex("Ashcroft")).isEqualTo("A261");
        assertThat(soundex("Tymczak")).isEqualTo("T522");
        assertThat(soundex("Pfister")).isEqualTo("P236");
        assertThat(soundex("Honeyman")).isEqualTo("H555");
        assertThat(soundex("Trino")).isEqualTo("T650");
        assertThat(soundex("jim")).isEqualTo("J500");
    }

    @Test
    void testNonLetterCharactersAreIgnored()
    {
        assertThat(soundex("")).isEmpty();
        assertThat(soundex("123")).isEmpty();
        assertThat(soundex("🚀")).isEmpty();
        assertThat(soundex("j~im")).isEqualTo("J500");
        assertThat(soundex("x123")).isEqualTo("X000");
    }

    @Test
    void testNonAsciiInput()
    {
        // LATIN SMALL LETTER SHARP S upper-cases to "SS"
        assertThat(soundex("ß")).isEqualTo("S000");
        assertThat(soundex("Roßert")).isEqualTo("R263");
        // LATIN SMALL LIGATURE FI upper-cases to "FI"
        assertThat(soundex("ﬁm")).isEqualTo("F500");
        // unmappable letters beyond the fourth code position are never reached
        assertThat(soundex("Robertą")).isEqualTo("R163");
    }

    @Test
    void testUnmappableCharacter()
    {
        assertThatThrownBy(() -> soundex("jąmes"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The character is not mapped: Ą (index=195)");
    }

    private static String soundex(String input)
    {
        return Soundex.soundex(utf8Slice(input)).toStringUtf8();
    }
}
