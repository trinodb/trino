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

import java.util.Arrays;

/**
 * The Jaro-Winkler similarity metric, designed and best suited for short strings
 * such as identifiers, and to detect typos. Returns a value in the [0, 1] range,
 * where 1 means equal.
 * <p>
 * Derived from {@code info.debatty.java.stringsimilarity.JaroWinkler} (MIT License),
 * preserving its exact arithmetic, including the uncapped common prefix bonus.
 */
public final class JaroWinkler
{
    private static final double THRESHOLD = 0.7;
    private static final double WINKLER_BONUS_COEFFICIENT = 0.1;

    private JaroWinkler() {}

    public static double similarity(String first, String second)
    {
        if (first.equals(second)) {
            return 1;
        }

        int[] matchProfile = matches(first, second);
        // float arithmetic retained from the original implementation to produce identical results
        float matchCount = matchProfile[0];
        if (matchCount == 0) {
            return 0;
        }
        double jaro = (matchCount / first.length() + matchCount / second.length() + (matchCount - matchProfile[1]) / matchCount) / 3;
        if (jaro <= THRESHOLD) {
            return jaro;
        }
        return jaro + Math.min(WINKLER_BONUS_COEFFICIENT, 1.0 / matchProfile[3]) * matchProfile[2] * (1 - jaro);
    }

    // returns [matches, transpositions, common prefix length, longer string length]
    private static int[] matches(String first, String second)
    {
        String max;
        String min;
        if (first.length() > second.length()) {
            max = first;
            min = second;
        }
        else {
            max = second;
            min = first;
        }
        int range = Math.max(max.length() / 2 - 1, 0);
        int[] matchIndexes = new int[min.length()];
        Arrays.fill(matchIndexes, -1);
        boolean[] matchFlags = new boolean[max.length()];
        int matches = 0;
        for (int minIndex = 0; minIndex < min.length(); minIndex++) {
            char character = min.charAt(minIndex);
            for (int maxIndex = Math.max(minIndex - range, 0), limit = Math.min(minIndex + range + 1, max.length()); maxIndex < limit; maxIndex++) {
                if (!matchFlags[maxIndex] && character == max.charAt(maxIndex)) {
                    matchIndexes[minIndex] = maxIndex;
                    matchFlags[maxIndex] = true;
                    matches++;
                    break;
                }
            }
        }
        char[] minMatches = new char[matches];
        char[] maxMatches = new char[matches];
        for (int i = 0, matchIndex = 0; i < min.length(); i++) {
            if (matchIndexes[i] != -1) {
                minMatches[matchIndex] = min.charAt(i);
                matchIndex++;
            }
        }
        for (int i = 0, matchIndex = 0; i < max.length(); i++) {
            if (matchFlags[i]) {
                maxMatches[matchIndex] = max.charAt(i);
                matchIndex++;
            }
        }
        int transpositions = 0;
        for (int i = 0; i < minMatches.length; i++) {
            if (minMatches[i] != maxMatches[i]) {
                transpositions++;
            }
        }
        int prefix = 0;
        for (int i = 0; i < min.length(); i++) {
            if (first.charAt(i) != second.charAt(i)) {
                break;
            }
            prefix++;
        }
        return new int[] {matches, transpositions / 2, prefix, max.length()};
    }
}
