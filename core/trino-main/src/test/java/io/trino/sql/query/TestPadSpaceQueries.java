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
package io.trino.sql.query;

import com.google.common.collect.ImmutableList;
import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;
import com.ibm.icu.util.ULocale;
import io.trino.Session;
import io.trino.testing.MaterializedRow;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.nullsFirst;
import static java.util.Comparator.nullsLast;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/// Runs queries over strings that differ in trailing spaces, in white space that sorts around
/// a space, and in characters with a space's weights, through every operator that compares,
/// hashes, or sorts strings, with and without collation keys, and checks each result against a
/// reference: the strings padded with spaces to a common length and compared by code point or
/// by ICU. Every character in the data has one weight at each level of the root collation, for
/// which padding with spaces is what a `PAD SPACE` collation means.
@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestPadSpaceQueries
{
    // characters with one weight at each level, including white space below the space (tab, line
    // feed, next line) and characters with the space's primary weight (no-break and ideographic spaces)
    private static final int[] CHARACTERS = {'a', 'A', 'b', '!', ' ', ' ', ' ', '\t', '\n', 0x0085, 0x00A0, 0x3000, 0x4E00};

    private static final List<String> HAND_PICKED = ImmutableList.of(
            "",
            " ",
            "  ",
            "a",
            "a ",
            "a  ",
            "A",
            "A ",
            "a\t",
            "a \t",
            "a\t ",
            "a\n",
            "a!",
            "a!  ",
            "ab",
            "a b",
            " a",
            "\t",
            string('a', 0x00A0),
            string('a', 0x3000),
            string('a', 0x0085),
            string(0x4E00),
            string(0x4E00, ' '));

    private final QueryAssertions assertions = new QueryAssertions();
    private final List<String> values;
    private final String table;
    private final List<String> probes;

    public TestPadSpaceQueries()
    {
        Random random = new Random(42);
        List<String> values = new ArrayList<>(HAND_PICKED);
        for (int i = 0; i < 40; i++) {
            values.add(randomString(random));
        }
        // nulls sort and group apart from every string
        values.add(null);
        values.add(null);
        this.values = values;
        this.table = IntStream.range(0, values.size())
                .mapToObj(id -> "(%s, %s)".formatted(id, values.get(id) == null ? "CAST(NULL AS varchar)" : literal(values.get(id))))
                .collect(joining(", ", "(VALUES ", ") t(id, x)"));

        List<String> probes = new ArrayList<>(HAND_PICKED);
        for (int i = 0; i < 20; i++) {
            probes.add(randomString(random));
        }
        this.probes = probes;
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testSortingAndRanking()
    {
        for (Case testCase : cases()) {
            Comparator<Integer> byValue = Comparator.comparing(values::get, nullsLast(testCase.reference()));
            Comparator<Integer> byValueThenId = byValue.thenComparing(naturalOrder());

            // sort, sort in reverse with nulls first, and top-n
            testCase.assertRows("SELECT id FROM %s ORDER BY k, id", ids().sorted(byValueThenId).map(id -> List.<Object>of(id)).toList());
            Comparator<Integer> descending = Comparator.<Integer, String>comparing(values::get, nullsFirst(testCase.reference().reversed())).thenComparing(naturalOrder());
            testCase.assertRows("SELECT id FROM %s ORDER BY k DESC NULLS FIRST, id LIMIT 11", ids().sorted(descending).limit(11).map(id -> List.<Object>of(id)).toList());

            // rank, and RANGE frames, whose peers are the rows that are equal under the collation
            testCase.assertRows(
                    "SELECT id, rank() OVER (ORDER BY k), count(*) OVER (ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM %s ORDER BY id",
                    ids().map(id -> List.<Object>of(
                                    id,
                                    1 + ids().filter(other -> byValue.compare(other, id) < 0).count(),
                                    ids().filter(other -> byValue.compare(other, id) <= 0).count()))
                            .toList());

            // composite values order by their strings under the collation
            List<List<Object>> nonNullOrder = ids().filter(id -> values.get(id) != null).sorted(byValueThenId).map(id -> List.<Object>of(id)).toList();
            testCase.assertRows("SELECT id FROM %s WHERE k IS NOT NULL ORDER BY ROW(k), id", nonNullOrder);
            testCase.assertRows("SELECT id FROM %s WHERE k IS NOT NULL ORDER BY ARRAY[k], id", nonNullOrder);
        }
    }

    @Test
    public void testGroupingAndDistinct()
    {
        for (Case testCase : cases()) {
            List<List<String>> classes = testCase.classes(values);
            long nonNullClasses = classes.stream().filter(members -> members.getFirst() != null).count();

            testCase.assertRows(
                    "SELECT count(*) FROM %s GROUP BY k ORDER BY 1",
                    classes.stream().map(members -> (long) members.size()).sorted().map(size -> List.<Object>of(size)).toList());
            testCase.assertValue("SELECT count(DISTINCT k) FROM %s", nonNullClasses);
            testCase.assertValue("SELECT count(*) FROM (SELECT DISTINCT k FROM %s)", (long) classes.size());
            testCase.assertValue("SELECT count(*) FROM (SELECT row_number() OVER (PARTITION BY k ORDER BY id) r FROM %s) WHERE r = 1", (long) classes.size());

            // a distinct value is one of the equal strings, one for each class
            List<String> distinct = testCase.column("SELECT DISTINCT k FROM %s WHERE k IS NOT NULL");
            assertThat(distinct).as(testCase.context()).hasSize((int) nonNullClasses);
            assertThat(testCase.classes(distinct)).as(testCase.context()).hasSize((int) nonNullClasses);

            // the extremes, and the values that min_by and max_by return, are least and greatest
            List<String> nonNull = values.stream().filter(value -> value != null).toList();
            String least = nonNull.stream().min(testCase.reference()).orElseThrow();
            String greatest = nonNull.stream().max(testCase.reference()).orElseThrow();
            MaterializedRow extremes = testCase.row("SELECT min(k), max(k), min_by(x, k), max_by(x, k) FROM %s");
            for (int field = 0; field < 4; field++) {
                String expected = field % 2 == 0 ? least : greatest;
                assertThat(testCase.reference().compare((String) extremes.getField(field), expected))
                        .as("%s: field %s of %s", testCase.context(), field, extremes)
                        .isZero();
            }

            // array_sort orders under the collation
            List<?> sorted = (List<?>) testCase.row("SELECT array_sort(array_agg(k)) FROM %s WHERE k IS NOT NULL").getField(0);
            assertThat(sorted).as(testCase.context()).hasSize(nonNull.size());
            for (int i = 1; i < sorted.size(); i++) {
                assertThat(testCase.reference().compare((String) sorted.get(i - 1), (String) sorted.get(i)))
                        .as("%s: %s", testCase.context(), sorted)
                        .isLessThanOrEqualTo(0);
            }
        }
    }

    @Test
    public void testJoinsAndSetOperations()
    {
        for (Case testCase : cases()) {
            for (String distribution : new String[] {"BROADCAST", "PARTITIONED"}) {
                Case joinCase = testCase.with(JOIN_DISTRIBUTION_TYPE, distribution);
                List<List<String>> classes = joinCase.classes(values);
                long pairs = classes.stream()
                        .filter(members -> members.getFirst() != null)
                        .mapToLong(members -> (long) members.size() * members.size())
                        .sum();
                joinCase.assertValue("SELECT count(*) FROM %1$s a JOIN %1$s b ON a.k = b.k", pairs);
                // with a non-equality conjunct, which keeps the join from being a plain hash lookup
                joinCase.assertValue(
                        "SELECT count(*) FROM %1$s a JOIN %1$s b ON a.k = b.k AND a.id < b.id",
                        classes.stream().filter(members -> members.getFirst() != null).mapToLong(members -> (long) members.size() * (members.size() - 1) / 2).sum());
                joinCase.assertValue(
                        "SELECT count(*) FROM %1$s a LEFT JOIN (SELECT DISTINCT k FROM %1$s WHERE id %% 3 = 0) b ON a.k = b.k WHERE b.k IS NULL",
                        ids().filter(id -> !joinCase.matchesAny(values.get(id), other -> other % 3 == 0)).count());

                // semi joins
                joinCase.assertValue(
                        "SELECT count(*) FROM %1$s WHERE k IN (SELECT k FROM %1$s WHERE id %% 3 = 0)",
                        ids().filter(id -> joinCase.matchesAny(values.get(id), other -> other % 3 == 0)).count());
                joinCase.assertValue(
                        "SELECT count(*) FROM %1$s a WHERE EXISTS (SELECT 1 FROM %1$s b WHERE b.k = a.k AND b.id %% 3 = 0)",
                        ids().filter(id -> joinCase.matchesAny(values.get(id), other -> other % 3 == 0)).count());
            }

            // set operations deduplicate under the collation
            Predicate<Integer> even = id -> id % 2 == 0;
            List<String> evens = ids().filter(even).map(values::get).toList();
            List<String> odds = ids().filter(even.negate()).map(values::get).toList();
            long intersection = testCase.classes(evens).stream().filter(members -> testCase.containsEqual(odds, members.getFirst())).count();
            long difference = testCase.classes(evens).stream().filter(members -> !testCase.containsEqual(odds, members.getFirst())).count();
            testCase.assertValue("SELECT count(*) FROM (SELECT k FROM %1$s WHERE id %% 2 = 0 INTERSECT SELECT k FROM %1$s WHERE id %% 2 = 1)", intersection);
            testCase.assertValue("SELECT count(*) FROM (SELECT k FROM %1$s WHERE id %% 2 = 0 EXCEPT SELECT k FROM %1$s WHERE id %% 2 = 1)", difference);
            testCase.assertValue("SELECT count(*) FROM (SELECT k FROM %1$s WHERE id %% 2 = 0 UNION SELECT k FROM %1$s WHERE id %% 2 = 1)", (long) testCase.classes(values).size());
        }
    }

    @Test
    public void testPredicates()
    {
        for (Case testCase : cases()) {
            Comparator<String> reference = testCase.reference();
            List<String> nonNull = values.stream().filter(value -> value != null).toList();
            for (String probe : probes) {
                // evaluated for each row
                testCase.assertRow(
                        "SELECT count_if(k < %2$s), count_if(k <= %2$s), count_if(k = %2$s), count_if(k > %2$s), count_if(k BETWEEN %2$s AND %2$s), count_if(k IS DISTINCT FROM %2$s) FROM %1$s".formatted("%1$s", literal(probe)),
                        List.of(
                                nonNull.stream().filter(value -> reference.compare(value, probe) < 0).count(),
                                nonNull.stream().filter(value -> reference.compare(value, probe) <= 0).count(),
                                nonNull.stream().filter(value -> reference.compare(value, probe) == 0).count(),
                                nonNull.stream().filter(value -> reference.compare(value, probe) > 0).count(),
                                nonNull.stream().filter(value -> reference.compare(value, probe) == 0).count(),
                                (long) values.size() - nonNull.stream().filter(value -> reference.compare(value, probe) == 0).count()));
            }

            // a long IN list, which is evaluated with a hash set
            List<String> list = probes.subList(0, 30);
            testCase.assertValue(
                    "SELECT count(*) FROM %%s WHERE k IN (%s)".formatted(list.stream().map(TestPadSpaceQueries::literal).collect(joining(", "))),
                    nonNull.stream().filter(value -> list.stream().anyMatch(item -> reference.compare(value, item) == 0)).count());

            // constant comparisons are folded during planning, with the same result
            for (int i = 0; i + 1 < probes.size(); i += 2) {
                String left = probes.get(i);
                String right = probes.get(i + 1);
                int expected = Integer.signum(reference.compare(left, right));
                testCase.assertRow(
                        "SELECT %2$s COLLATE %1$s < %3$s, %2$s COLLATE %1$s = %3$s, %2$s COLLATE %1$s > %3$s".formatted(testCase.collation(), literal(left), literal(right)),
                        List.of(expected < 0, expected == 0, expected > 0));
            }
        }
    }

    private List<Case> cases()
    {
        ImmutableList.Builder<Case> cases = ImmutableList.builder();
        for (String collation : new String[] {"ucs_basic PAD SPACE", "unicode PAD SPACE", "unicode_ci PAD SPACE"}) {
            cases.add(new Case(collation, assertions.getDefaultSession(), collation));
        }
        return cases.build();
    }

    private Stream<Integer> ids()
    {
        return IntStream.range(0, values.size()).boxed();
    }

    private final class Case
    {
        private final String collation;
        private final Session session;
        private final String context;
        private final Comparator<String> reference;

        Case(String collation, Session session, String context)
        {
            this.collation = collation;
            this.session = session;
            this.context = context;
            this.reference = paddedComparator(collation);
        }

        Case with(String property, String value)
        {
            return new Case(collation, Session.builder(session).setSystemProperty(property, value).build(), "%s, %s %s".formatted(context, property, value));
        }

        String collation()
        {
            return collation;
        }

        String context()
        {
            return context;
        }

        Comparator<String> reference()
        {
            return reference;
        }

        // the values grouped into classes of equal values; null is a class of its own
        List<List<String>> classes(List<String> values)
        {
            List<String> sorted = values.stream().sorted(nullsLast(reference)).toList();
            List<List<String>> classes = new ArrayList<>();
            for (String value : sorted) {
                List<String> last = classes.isEmpty() ? null : classes.getLast();
                if (last != null && sameClass(last.getFirst(), value)) {
                    last.add(value);
                }
                else {
                    List<String> members = new ArrayList<>();
                    members.add(value);
                    classes.add(members);
                }
            }
            return classes;
        }

        boolean containsEqual(List<String> values, String value)
        {
            return values.stream().anyMatch(other -> sameClass(other, value));
        }

        // whether a non-null value equals the value of some row whose id matches
        boolean matchesAny(String value, Predicate<Integer> ids)
        {
            return value != null && ids().filter(ids).map(values::get).anyMatch(other -> other != null && reference.compare(other, value) == 0);
        }

        private boolean sameClass(String left, String right)
        {
            if (left == null || right == null) {
                return left == null && right == null;
            }
            return reference.compare(left, right) == 0;
        }

        void assertRows(String query, List<List<Object>> expected)
        {
            assertThat(rows(query)).as("%s: %s", context, query).isEqualTo(expected);
        }

        void assertRow(String query, List<Object> expected)
        {
            assertRows(query, ImmutableList.of(expected));
        }

        void assertValue(String query, Object expected)
        {
            assertRow(query, ImmutableList.of(expected));
        }

        MaterializedRow row(String query)
        {
            return assertions.execute(session, sql(query)).getMaterializedRows().getFirst();
        }

        List<String> column(String query)
        {
            return assertions.execute(session, sql(query)).getMaterializedRows().stream()
                    .map(row -> (String) row.getField(0))
                    .collect(toImmutableList());
        }

        private List<List<Object>> rows(String query)
        {
            return assertions.execute(session, sql(query)).getMaterializedRows().stream()
                    .map(MaterializedRow::getFields)
                    .collect(toImmutableList());
        }

        // the query with the collated table in place of %s
        private String sql(String query)
        {
            return query.formatted("(SELECT id, x, x COLLATE %s k FROM %s)".formatted(collation, table));
        }
    }

    // the strings compared as the standard specifies: padded with spaces to a common length
    private static Comparator<String> paddedComparator(String collation)
    {
        Comparator<String> comparator = switch (collation.substring(0, collation.indexOf(' '))) {
            case "ucs_basic" -> TestPadSpaceQueries::compareCodePoints;
            case "unicode" -> icu(Collator.TERTIARY)::compare;
            case "unicode_ci" -> icu(Collator.SECONDARY)::compare;
            default -> throw new IllegalArgumentException(collation);
        };
        return (left, right) -> {
            int length = Math.max(left.length(), right.length());
            return comparator.compare(left + " ".repeat(length - left.length()), right + " ".repeat(length - right.length()));
        };
    }

    private static RuleBasedCollator icu(int strength)
    {
        RuleBasedCollator collator = (RuleBasedCollator) Collator.getInstance(ULocale.ROOT).cloneAsThawed();
        collator.setStrength(strength);
        collator.setAlternateHandlingShifted(false);
        return (RuleBasedCollator) collator.freeze();
    }

    private static int compareCodePoints(String left, String right)
    {
        int[] leftCodePoints = left.codePoints().toArray();
        int[] rightCodePoints = right.codePoints().toArray();
        return Arrays.compare(leftCodePoints, rightCodePoints);
    }

    private static String randomString(Random random)
    {
        StringBuilder builder = new StringBuilder();
        int length = random.nextInt(5);
        for (int i = 0; i < length; i++) {
            builder.appendCodePoint(CHARACTERS[random.nextInt(CHARACTERS.length)]);
        }
        return builder.toString();
    }

    private static String string(int... codePoints)
    {
        return new String(codePoints, 0, codePoints.length);
    }

    // a Unicode string literal, which spells every character other than printable ASCII as an escape
    private static String literal(String value)
    {
        StringBuilder builder = new StringBuilder("U&'");
        value.codePoints().forEach(codePoint -> {
            if (codePoint == '\'') {
                builder.append("''");
            }
            else if (codePoint == '\\') {
                builder.append("\\\\");
            }
            else if (codePoint >= 0x20 && codePoint < 0x7F) {
                builder.appendCodePoint(codePoint);
            }
            else {
                builder.append("\\+%06X".formatted(codePoint));
            }
        });
        return builder.append("'").toString();
    }
}
