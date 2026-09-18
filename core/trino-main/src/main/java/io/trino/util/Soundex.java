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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Locale;

/**
 * The classic American Soundex algorithm, where H and W are treated as silent letters
 * that do not act as separators between consonants with the same code.
 * <p>
 * Derived from {@code org.apache.commons.codec.language.Soundex#US_ENGLISH} (Apache License 2.0),
 * preserving its exact behavior, including the messages of exceptions thrown for unmappable input.
 */
public final class Soundex
{
    /**
     * Mapping of the 26 letters used in US English. A value of {@code 0} for a letter position
     * means do not encode, but treat as a separator when it occurs between consonants with the same code.
     */
    private static final String US_ENGLISH_MAPPING = "01230120022455012623010202";

    private Soundex() {}

    public static Slice soundex(Slice input)
    {
        for (int i = 0; i < input.length(); i++) {
            if (input.getByte(i) < 0) {
                // Non-ASCII input needs full Unicode letter detection and String upper-casing
                // with one-to-many mappings (e.g. LATIN SMALL LETTER SHARP S upper-cases to "SS")
                return Slices.utf8Slice(soundexUnicode(input.toStringUtf8()));
            }
        }

        byte[] out = {'0', '0', '0', '0'};
        int count = 0;
        char lastDigit = 0;
        for (int i = 0; i < input.length() && count < out.length; i++) {
            byte value = input.getByte(i);
            char letter;
            if (value >= 'a' && value <= 'z') {
                letter = (char) (value - ('a' - 'A'));
            }
            else if (value >= 'A' && value <= 'Z') {
                letter = (char) value;
            }
            else {
                continue;
            }
            if (count == 0) {
                out[count] = (byte) letter;
                count++;
                lastDigit = map(letter);
                continue;
            }
            if (letter == 'H' || letter == 'W') {
                // these are ignored completely
                continue;
            }
            char digit = map(letter);
            if (digit != '0' && digit != lastDigit) {
                // don't store vowels or repeats
                out[count] = (byte) digit;
                count++;
            }
            lastDigit = digit;
        }
        if (count == 0) {
            return Slices.EMPTY_SLICE;
        }
        return Slices.wrappedBuffer(out);
    }

    private static String soundexUnicode(String input)
    {
        String cleaned = clean(input);
        if (cleaned.isEmpty()) {
            return cleaned;
        }
        char[] out = {'0', '0', '0', '0'};
        int count = 0;
        char first = cleaned.charAt(0);
        out[count] = first;
        count++;
        char lastDigit = map(first);
        for (int i = 1; i < cleaned.length() && count < out.length; i++) {
            char character = cleaned.charAt(i);
            if (character == 'H' || character == 'W') {
                // these are ignored completely
                continue;
            }
            char digit = map(character);
            if (digit != '0' && digit != lastDigit) {
                // don't store vowels or repeats
                out[count] = digit;
                count++;
            }
            lastDigit = digit;
        }
        return new String(out);
    }

    private static char map(char character)
    {
        int index = character - 'A';
        if (index < 0 || index >= US_ENGLISH_MAPPING.length()) {
            throw new IllegalArgumentException("The character is not mapped: " + character + " (index=" + index + ")");
        }
        return US_ENGLISH_MAPPING.charAt(index);
    }

    private static String clean(String input)
    {
        if (input.isEmpty()) {
            return input;
        }
        int length = input.length();
        char[] letters = new char[length];
        int count = 0;
        for (int i = 0; i < length; i++) {
            if (Character.isLetter(input.charAt(i))) {
                letters[count] = input.charAt(i);
                count++;
            }
        }
        if (count == length) {
            return input.toUpperCase(Locale.ENGLISH);
        }
        return new String(letters, 0, count).toUpperCase(Locale.ENGLISH);
    }
}
