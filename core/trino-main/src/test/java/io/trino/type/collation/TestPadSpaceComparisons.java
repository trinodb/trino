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
package io.trino.type.collation;

import com.google.common.collect.ImmutableList;
import com.ibm.icu.text.CollationElementIterator;
import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.type.CollationComparison;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.Collation.UCS_BASIC;
import static io.trino.type.collation.CollationRegistry.UNICODE_CI_NAME;
import static io.trino.type.collation.CollationRegistry.UNICODE_NAME;
import static io.trino.type.collation.CollationRegistry.collationRegistry;
import static io.trino.type.collation.CollationRegistry.rootCollator;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class TestPadSpaceComparisons
{
    private static final List<String> PAD_SPACE_COLLATIONS = ImmutableList.of(UCS_BASIC.name(), UNICODE_NAME, UNICODE_CI_NAME);
    private static final List<String> ICU_COLLATIONS = ImmutableList.of(UNICODE_NAME, UNICODE_CI_NAME);

    private static final String ZERO_WIDTH_SPACE = Character.toString(0x200B);
    private static final String NO_BREAK_SPACE = Character.toString(0x00A0);

    // Characters with exactly one weight at each level of the root collation. For strings of
    // them, padding weights is padding characters, so the collator compares the strings padded
    // with spaces exactly as the PAD SPACE collation should; that is the reference. Includes
    // white space below the space (tab, line feed, carriage return, next line, line separator)
    // and characters with the space's primary weight (no-break, en, and ideographic spaces).
    private static final int[] SINGLE_WEIGHT = {'a', 'A', 'b', 'z', 'Z', '0', '!', '~', ' ', '\t', '\n', '\r', 0x0085, 0x00A0, 0x2002, 0x2028, 0x3000, 0x4E00};
    // characters the root collation ignores at every level
    private static final int[] IGNORABLE = {0x0001, 0x00AD, 0x200B, 0xFEFF};
    // characters with several weights at a level (expansions, Hangul syllables) or weights at
    // only some levels (combining marks)
    private static final int[] MULTIPLE_WEIGHTS = {0x00DF, 0x00E6, 0x00E9, 0x0301, 0x0323, 0xAC00, 0xFB01};
    // contractions, scripts with reordering, noncharacters, the replacement character, and
    // supplementary characters, including ones with long primary weights
    private static final int[] OTHER = {0x00BD, 0x05D0, 0x0627, 0x0E01, 0x0E40, 0x1100, 0x1161, 0x2460, 0x3042, 0x30A2, 0x9FFF, 0xFFFD, 0xFFFE, 0xFFFF, 0x1F600, 0x20000, 0xE0001, 0x10FFFD};
    private static final int[] ALL = Stream.of(SINGLE_WEIGHT, IGNORABLE, MULTIPLE_WEIGHTS, OTHER).flatMapToInt(IntStream::of).toArray();

    // strings for which the collator padded with spaces is the reference: only characters with
    // one weight at each level, with white space around the space at the ends
    private static final Alphabet REFERENCE = new Alphabet(SINGLE_WEIGHT, new int[] {' ', '\t', '\n', 0x00A0, 0x3000});
    // every kind of character, with ignorable characters among the white space at the ends
    private static final Alphabet ADVERSARIAL = new Alphabet(ALL, new int[] {' ', '\t', '\n', 0x00A0, 0x3000, 0x200B, 0x0001});

    @Test
    public void testShorterStringIsPaddedWithSpaces()
    {
        for (String name : PAD_SPACE_COLLATIONS) {
            CollationComparison comparison = padSpace(name);
            // a tab sorts below a space, so 'a' followed by a tab sorts below 'a' padded with a space
            assertOrder(comparison, name, "a\t", "a");
            assertOrder(comparison, name, "a \t", "a");
            assertOrder(comparison, name, "a\t", "a \t");
            assertOrder(comparison, name, "a\n", "a ");
            // a character above a space sorts above the padding
            assertOrder(comparison, name, "a", "a!");
            assertOrder(comparison, name, "a", "a b");
            assertOrder(comparison, name, "a  ", "a!");
            // trailing spaces are padding
            assertEqual(comparison, name, "a", "a   ");
            assertEqual(comparison, name, "", "  ");
            // leading and inner spaces are not
            assertOrder(comparison, name, " a", "a");
            assertOrder(comparison, name, "a b", "ab");
            // the padding extends the shorter string only as far as the longer one
            assertOrder(comparison, name, "a" + " ".repeat(1_000) + "\t", "a");
            assertOrder(comparison, name, "a", "a" + " ".repeat(1_000) + "!");
            assertEqual(comparison, name, "a" + " ".repeat(1_000), "a");
        }

        // ICU collations pad weights: an ignorable character contributes no weight, and a no-break
        // space has the space's weights when case and width differences are disregarded
        assertEqual(padSpace(UNICODE_NAME), UNICODE_NAME, "a" + ZERO_WIDTH_SPACE, "a ");
        assertEqual(padSpace(UNICODE_CI_NAME), UNICODE_CI_NAME, "a " + ZERO_WIDTH_SPACE, "a");
        assertEqual(padSpace(UNICODE_CI_NAME), UNICODE_CI_NAME, "a" + NO_BREAK_SPACE, "a");
        assertOrder(padSpace(UNICODE_NAME), UNICODE_NAME, "a", "a" + NO_BREAK_SPACE);
        assertEqual(padSpace(UNICODE_CI_NAME), UNICODE_CI_NAME, "ABC  ", "abc");
        assertOrder(padSpace(UNICODE_NAME), UNICODE_NAME, "abc  ", "ABC");
        // a combining mark on a trailing space gives the space a secondary weight, so it is not padding
        assertOrder(padSpace(UNICODE_CI_NAME), UNICODE_CI_NAME, "a", "a " + Character.toString(0x0301));
        // the binary collation compares code points, so these are significant
        assertOrder(padSpace(UCS_BASIC.name()), UCS_BASIC.name(), "a", "a" + ZERO_WIDTH_SPACE);
        assertOrder(padSpace(UCS_BASIC.name()), UCS_BASIC.name(), "a", "a" + NO_BREAK_SPACE);
    }

    @Test
    public void testOrderIsPaddedOrder()
    {
        Random random = new Random(1);
        for (String name : PAD_SPACE_COLLATIONS) {
            CollationComparison padSpace = padSpace(name);
            CollationComparison noPad = collationRegistry().comparison(collationRegistry().getCollation(name));
            for (int i = 0; i < 30_000; i++) {
                String left = REFERENCE.string(random, 8);
                String right = random.nextBoolean() ? REFERENCE.string(random, 8) : REFERENCE.related(random, left);
                int length = Math.max(left.length(), right.length());
                int expected = Integer.signum(noPad.compare(utf8Slice(padded(left, length)), utf8Slice(padded(right, length))));
                assertConsistent(padSpace, name, left, right, expected);
            }
        }
    }

    @Test
    public void testWeightsFollowCollator()
    {
        // the padding is applied to weights read from ICU's collation elements, so the weights
        // must order strings as the collator does; this fails if an ICU upgrade changes how
        // collation elements are split
        Random random = new Random(2);
        for (int strength : new int[] {Collator.TERTIARY, Collator.SECONDARY}) {
            RuleBasedCollator collator = rootCollator(strength);
            CollationElementIterator iterator = collator.cloneAsThawed().getCollationElementIterator("");
            int levels = strength + 1;
            for (int i = 0; i < 50_000; i++) {
                String left = ADVERSARIAL.string(random, 6);
                String right = random.nextBoolean() ? ADVERSARIAL.string(random, 6) : ADVERSARIAL.related(random, left);
                int expected = Integer.signum(collator.compare(left, right));
                int actual = compareUnpadded(CollationWeights.of(iterator, left, levels), CollationWeights.of(iterator, right, levels), levels);
                if (actual != expected) {
                    assertThat(actual).as("strength %s: %s vs %s", strength, quote(left), quote(right)).isEqualTo(expected);
                }
            }
        }
    }

    @Test
    public void testComparisonIsTotalPreorder()
    {
        // equality, ordering, and keys agree, and the order is antisymmetric and transitive, so
        // sorting, grouping, and joining by keys or by comparisons give the same results
        Random random = new Random(3);
        List<String> pool = new ArrayList<>();
        for (int i = 0; i < 150; i++) {
            String value = ADVERSARIAL.string(random, 6);
            pool.add(value);
            pool.add(ADVERSARIAL.related(random, value));
            pool.add(value + ADVERSARIAL.padding(random, 3));
        }
        for (String name : PAD_SPACE_COLLATIONS) {
            CollationComparison comparison = padSpace(name);
            for (String left : pool) {
                for (String right : pool) {
                    int forward = Integer.signum(comparison.compare(utf8Slice(left), utf8Slice(right)));
                    int backward = Integer.signum(comparison.compare(utf8Slice(right), utf8Slice(left)));
                    if (forward != -backward) {
                        assertThat(forward).as("%s: antisymmetry of %s and %s", name, quote(left), quote(right)).isEqualTo(-backward);
                    }
                    assertConsistent(comparison, name, left, right, forward);
                }
            }
            // a sort with an intransitive comparison leaves some later value below an earlier one
            List<String> sorted = new ArrayList<>(pool);
            sorted.sort((left, right) -> comparison.compare(utf8Slice(left), utf8Slice(right)));
            for (int i = 0; i < sorted.size(); i++) {
                for (int j = i + 1; j < sorted.size(); j++) {
                    if (comparison.compare(utf8Slice(sorted.get(i)), utf8Slice(sorted.get(j))) > 0) {
                        fail("%s: not transitive: %s sorts before %s but compares above it".formatted(name, quote(sorted.get(i)), quote(sorted.get(j))));
                    }
                }
            }
        }
    }

    @Test
    public void testTrailingSpacesAreInvisible()
    {
        Random random = new Random(4);
        for (String name : PAD_SPACE_COLLATIONS) {
            CollationComparison comparison = padSpace(name);
            for (int i = 0; i < 20_000; i++) {
                String value = ADVERSARIAL.string(random, 6);
                String other = ADVERSARIAL.string(random, 6);
                String extended = value + " ".repeat(1 + random.nextInt(4));
                String context = "%s: %s".formatted(name, quote(value));
                assertThat(comparison.collationKey(utf8Slice(extended))).as(context).isEqualTo(comparison.collationKey(utf8Slice(value)));
                assertThat(comparison.equal(utf8Slice(extended), utf8Slice(value))).as(context).isTrue();
                assertThat(Integer.signum(comparison.compare(utf8Slice(extended), utf8Slice(other))))
                        .as("%s vs %s", context, quote(other))
                        .isEqualTo(Integer.signum(comparison.compare(utf8Slice(value), utf8Slice(other))));
            }
        }
    }

    @Test
    public void testIgnorableCharactersAreInvisible()
    {
        // an ICU PAD SPACE collation pads weights, and an ignorable character has none, so
        // inserting one anywhere, including among trailing spaces, changes nothing. Thai is left
        // out: an ignorable character between a prevowel and its consonant splits their
        // contraction, as it does without padding.
        Alphabet noContractions = new Alphabet(
                IntStream.of(ALL).filter(codePoint -> Character.UnicodeScript.of(codePoint) != Character.UnicodeScript.THAI).toArray(),
                ADVERSARIAL.paddingLike());
        Random random = new Random(5);
        for (String name : ICU_COLLATIONS) {
            CollationComparison comparison = padSpace(name);
            for (int i = 0; i < 20_000; i++) {
                String value = noContractions.string(random, 6) + " ".repeat(random.nextInt(3));
                int position = value.offsetByCodePoints(0, random.nextInt(value.codePointCount(0, value.length()) + 1));
                String inserted = value.substring(0, position) + Character.toString(IGNORABLE[random.nextInt(IGNORABLE.length)]) + value.substring(position);
                assertThat(comparison.collationKey(utf8Slice(inserted)))
                        .as("%s: %s", name, quote(inserted))
                        .isEqualTo(comparison.collationKey(utf8Slice(value)));
            }
        }
    }

    @Test
    public void testCanonicalEquivalents()
    {
        // precomposed and decomposed forms have the same weights
        String[][] equivalents = {
                {string(0x00E9), string('e', 0x0301)},
                {string(0x00C5), string('A', 0x030A)},
                {string(0x1EC7), string('e', 0x0323, 0x0302)},
                {string(0xAC00), string(0x1100, 0x1161)},
        };
        for (String name : ICU_COLLATIONS) {
            CollationComparison comparison = padSpace(name);
            for (String[] pair : equivalents) {
                assertEqual(comparison, name, pair[0], pair[1]);
                assertEqual(comparison, name, pair[0] + "  ", pair[1]);
                assertOrder(comparison, name, pair[0] + "\t", pair[1]);
            }
        }
    }

    @Test
    public void testCaseInsensitiveCollationIgnoresCase()
    {
        Random random = new Random(6);
        CollationComparison comparison = padSpace(UNICODE_CI_NAME);
        Alphabet letters = new Alphabet(new int[] {'a', 'b', 'z', 'A', 'B', 'Z', 0x00E9, 0x00C9, 0x00E6, 0x00C6}, new int[] {' ', '\t'});
        for (int i = 0; i < 20_000; i++) {
            String value = letters.string(random, 8);
            assertThat(comparison.collationKey(utf8Slice(value.toUpperCase(ENGLISH))))
                    .as(quote(value))
                    .isEqualTo(comparison.collationKey(utf8Slice(value.toLowerCase(ENGLISH))));
        }
    }

    @Test
    public void testInvalidUtf8()
    {
        // varchar values need not be valid UTF-8; the binary collation compares their bytes as
        // char values do, and every collation stays consistent with its keys
        byte[] bytes = {'a', ' ', ' ', '\t', '!', (byte) 0x80, (byte) 0xBF, (byte) 0xC3, (byte) 0xE2, (byte) 0xF0, (byte) 0xFF};
        Random random = new Random(7);
        List<Slice> pool = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            byte[] value = new byte[random.nextInt(7)];
            for (int j = 0; j < value.length; j++) {
                value[j] = bytes[random.nextInt(bytes.length)];
            }
            pool.add(Slices.wrappedBuffer(value));
        }
        for (String name : PAD_SPACE_COLLATIONS) {
            CollationComparison comparison = padSpace(name);
            for (Slice left : pool) {
                for (Slice right : pool) {
                    int compared = Integer.signum(comparison.compare(left, right));
                    int keyCompared = Integer.signum(comparison.collationKey(left).compareTo(comparison.collationKey(right)));
                    boolean equal = comparison.equal(left, right);
                    boolean binary = name.equals(UCS_BASIC.name());
                    if (keyCompared != compared || equal != (compared == 0) || (binary && compared != referencePaddedBytes(left, right))) {
                        String context = "%s: %s vs %s".formatted(name, HexFormat.of().formatHex(left.getBytes()), HexFormat.of().formatHex(right.getBytes()));
                        if (binary) {
                            assertThat(compared).as(context + ", compare").isEqualTo(referencePaddedBytes(left, right));
                        }
                        assertThat(keyCompared).as(context + ", keys").isEqualTo(compared);
                        assertThat(equal).as(context + ", equal").isEqualTo(compared == 0);
                    }
                }
            }
        }
    }

    @Test
    public void testLongStrings()
    {
        Random random = new Random(8);
        for (String name : PAD_SPACE_COLLATIONS) {
            CollationComparison padSpace = padSpace(name);
            CollationComparison noPad = collationRegistry().comparison(collationRegistry().getCollation(name));
            for (int i = 0; i < 300; i++) {
                // long runs of spaces with a few other characters, so that the strings share
                // long prefixes and differ near their ends
                StringBuilder builder = new StringBuilder();
                int length = random.nextInt(2_000);
                for (int j = 0; j < length; j++) {
                    builder.appendCodePoint(random.nextInt(50) == 0 ? SINGLE_WEIGHT[random.nextInt(SINGLE_WEIGHT.length)] : ' ');
                }
                String left = builder.toString();
                String right = REFERENCE.related(random, left);
                int padded = Math.max(left.length(), right.length());
                assertConsistent(padSpace, name, left, right, Integer.signum(noPad.compare(utf8Slice(padded(left, padded)), utf8Slice(padded(right, padded)))));
            }
        }
    }

    @Test
    public void testConcurrentUse()
            throws Exception
    {
        // ICU comparisons use per-thread iterators; results must not depend on the thread
        Random random = new Random(9);
        List<String[]> pairs = new ArrayList<>();
        for (int i = 0; i < 5_000; i++) {
            String left = ADVERSARIAL.string(random, 6);
            pairs.add(new String[] {left, ADVERSARIAL.related(random, left)});
        }
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            for (String name : PAD_SPACE_COLLATIONS) {
                CollationComparison comparison = padSpace(name);
                List<String> expected = results(comparison, pairs);
                List<Future<List<String>>> futures = new ArrayList<>();
                for (int thread = 0; thread < 8; thread++) {
                    futures.add(executor.submit(() -> results(comparison, pairs)));
                }
                for (Future<List<String>> future : futures) {
                    assertThat(future.get()).as(name).isEqualTo(expected);
                }
            }
        }
        finally {
            executor.shutdownNow();
        }
    }

    private static List<String> results(CollationComparison comparison, List<String[]> pairs)
    {
        List<String> results = new ArrayList<>();
        for (String[] pair : pairs) {
            results.add(Integer.signum(comparison.compare(utf8Slice(pair[0]), utf8Slice(pair[1]))) + " " + HexFormat.of().formatHex(comparison.collationKey(utf8Slice(pair[0])).getBytes()));
        }
        return results;
    }

    private static CollationComparison padSpace(String name)
    {
        return collationRegistry().comparison(collationRegistry().getCollation(name).withPadSpace());
    }

    // the comparison, the key order, and equality all agree with the expected result
    private static void assertConsistent(CollationComparison comparison, String name, String left, String right, int expected)
    {
        Slice leftSlice = utf8Slice(left);
        Slice rightSlice = utf8Slice(right);
        int compared = Integer.signum(comparison.compare(leftSlice, rightSlice));
        int keyCompared = Integer.signum(comparison.collationKey(leftSlice).compareTo(comparison.collationKey(rightSlice)));
        boolean equal = comparison.equal(leftSlice, rightSlice);
        if (compared != expected || keyCompared != expected || equal != (expected == 0)) {
            String context = "%s: %s vs %s".formatted(name, quote(left), quote(right));
            assertThat(compared).as(context + ", compare").isEqualTo(expected);
            assertThat(keyCompared).as(context + ", keys").isEqualTo(expected);
            assertThat(equal).as(context + ", equal").isEqualTo(expected == 0);
        }
    }

    private static void assertOrder(CollationComparison comparison, String name, String lesser, String greater)
    {
        assertConsistent(comparison, name, lesser, greater, -1);
        assertConsistent(comparison, name, greater, lesser, 1);
    }

    private static void assertEqual(CollationComparison comparison, String name, String left, String right)
    {
        assertConsistent(comparison, name, left, right, 0);
        assertThat(comparison.collationKey(utf8Slice(left)))
                .as("%s: %s = %s", name, quote(left), quote(right))
                .isEqualTo(comparison.collationKey(utf8Slice(right)));
    }

    private static int compareUnpadded(CollationWeights left, CollationWeights right, int levels)
    {
        for (int level = 0; level < levels; level++) {
            int result = Arrays.compareUnsigned(
                    left.weights(level),
                    0,
                    left.count(level),
                    right.weights(level),
                    0,
                    right.count(level));
            if (result != 0) {
                return Integer.signum(result);
            }
        }
        return 0;
    }

    // the order of char values: bytes, with the shorter value extended with spaces
    private static int referencePaddedBytes(Slice left, Slice right)
    {
        int length = Math.max(left.length(), right.length());
        for (int i = 0; i < length; i++) {
            int leftByte = i < left.length() ? left.getUnsignedByte(i) : ' ';
            int rightByte = i < right.length() ? right.getUnsignedByte(i) : ' ';
            if (leftByte != rightByte) {
                return Integer.signum(leftByte - rightByte);
            }
        }
        return 0;
    }

    private static String padded(String value, int length)
    {
        return value + " ".repeat(length - value.length());
    }

    private static String string(int... codePoints)
    {
        return new String(codePoints, 0, codePoints.length);
    }

    private static String quote(String value)
    {
        StringBuilder builder = new StringBuilder("'");
        value.codePoints().forEach(codePoint -> {
            if (codePoint < 0x20 || codePoint > 0x7E) {
                builder.append("\\u{%X}".formatted(codePoint));
            }
            else {
                builder.appendCodePoint(codePoint);
            }
        });
        return builder.append("'").toString();
    }

    private record Alphabet(int[] codePoints, int[] paddingLike)
    {
        // a string biased toward spaces and white space, so that strings often share prefixes
        // and end in characters that are or resemble padding
        String string(Random random, int maxLength)
        {
            StringBuilder builder = new StringBuilder();
            int length = random.nextInt(maxLength + 1);
            for (int i = 0; i < length; i++) {
                int choice = random.nextInt(10);
                if (choice < 3) {
                    builder.append(' ');
                }
                else if (choice < 4) {
                    builder.appendCodePoint(paddingLike[random.nextInt(paddingLike.length)]);
                }
                else {
                    builder.appendCodePoint(codePoints[random.nextInt(codePoints.length)]);
                }
            }
            return builder.toString();
        }

        String padding(Random random, int maxLength)
        {
            StringBuilder builder = new StringBuilder();
            int length = 1 + random.nextInt(maxLength);
            for (int i = 0; i < length; i++) {
                builder.appendCodePoint(paddingLike[random.nextInt(paddingLike.length)]);
            }
            return builder.toString();
        }

        // a string that differs from the value only near its end: by trailing spaces, by a
        // suffix, or by its last character, which is where padding decides the order
        String related(Random random, String value)
        {
            String suffix = string(random, 3);
            return switch (random.nextInt(5)) {
                case 0 -> value + " ".repeat(1 + random.nextInt(3));
                case 1 -> value + suffix;
                case 2 -> value + " ".repeat(1 + random.nextInt(3)) + suffix;
                case 3 -> value + padding(random, 3);
                default -> {
                    if (value.isEmpty()) {
                        yield suffix;
                    }
                    yield value.substring(0, value.offsetByCodePoints(value.length(), -1)) + suffix;
                }
            };
        }
    }
}
