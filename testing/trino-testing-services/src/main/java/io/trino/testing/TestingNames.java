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
package io.trino.testing;

import java.security.SecureRandom;

public final class TestingNames
{
    private TestingNames() {}

    private static final char[] ALPHABET = new char[36];

    static {
        for (int digit = 0; digit < 10; digit++) {
            ALPHABET[digit] = (char) ('0' + digit);
        }

        for (int letter = 0; letter < 26; letter++) {
            ALPHABET[10 + letter] = (char) ('a' + letter);
        }
    }

    private static final SecureRandom random = new SecureRandom();

    // The suffix needs to be long enough to "prevent" collisions in practice.
    // The length of 5 was proven not to be long enough for tables names in tests.
    private static final int RANDOM_SUFFIX_LENGTH = 10;

    public static String randomNameSuffix()
    {
        char[] chars = new char[RANDOM_SUFFIX_LENGTH];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = ALPHABET[random.nextInt(0, ALPHABET.length)];
        }
        return new String(chars);
    }
}
