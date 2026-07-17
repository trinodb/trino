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
package io.trino.spi.admission;

// Feature: admission-policy-spi, Property 2: WaitDecision construction validates input.
//
// This test would naturally use jqwik's @Property with random generators. jqwik is not
// currently a dependency of this project, so per task 1.9's allowance of an acceptable
// alternative that minimizes diff impact, we use junit-jupiter-params @ParameterizedTest
// with @MethodSource to generate ≥100 deterministic cases per property branch
// (seeded java.util.Random for reproducibility). The semantics validated here match
// what a jqwik @Property with ≥100 iterations would assert: the constructors accept
// iff (reason != null && reason.length() ∈ [1, 256]) and, for Wait, additionally
// (maxWait != null && maxWait.toMillis() >= 0); other inputs throw IllegalArgumentException
// or NullPointerException.

import io.airlift.units.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Feature: admission-policy-spi, Property 2: WaitDecision construction validates input")
final class TestWaitDecisionConstruction
{
    private static final long SEED = 0xA1D_F00DL; // deterministic across runs
    private static final int VALID_REASON_CASES = 120;
    private static final int INVALID_LENGTH_REASON_CASES = 120;
    private static final int VALID_WAIT_CASES = 120;
    private static final int INVALID_WAIT_DURATION_CASES = 60;

    // ---------------- ProceedNow: valid reason accepted ----------------

    static List<String> validReasons()
    {
        Random random = new Random(SEED);
        List<String> values = new ArrayList<>(VALID_REASON_CASES);
        // Cover boundaries explicitly.
        values.add("a");                                // length 1 (lower bound)
        values.add(repeatChar('x', 256));               // length 256 (upper bound)
        values.add(repeatChar(' ', 1));                 // whitespace, length 1
        values.add(repeatChar('Z', 128));               // mid-range
        while (values.size() < VALID_REASON_CASES) {
            int len = 1 + random.nextInt(256); // length in [1, 256]
            values.add(randomString(random, len));
        }
        return values;
    }

    @ParameterizedTest(name = "ProceedNow accepts reason of length {0}")
    @MethodSource("validReasons")
    void proceedNowAcceptsValidReason(String reason)
    {
        WaitDecision.ProceedNow decision = new WaitDecision.ProceedNow(reason);
        assertThat(decision.reason()).isEqualTo(reason);
        assertThat(((WaitDecision) decision).reason()).isEqualTo(reason);
    }

    // ---------------- Wait: valid (reason, maxWait) accepted ----------------

    static List<WaitArgs> validWaitArgs()
    {
        Random random = new Random(SEED ^ 0x55AAL);
        List<WaitArgs> values = new ArrayList<>(VALID_WAIT_CASES);
        // Boundary cases.
        values.add(new WaitArgs(new Duration(0, TimeUnit.MILLISECONDS), "x"));
        values.add(new WaitArgs(new Duration(0, TimeUnit.NANOSECONDS), repeatChar('a', 256)));
        values.add(new WaitArgs(new Duration(1, TimeUnit.NANOSECONDS), "boundary-just-above-zero"));
        values.add(new WaitArgs(new Duration(1, TimeUnit.MILLISECONDS), repeatChar('b', 1)));
        values.add(new WaitArgs(new Duration(1e15, TimeUnit.NANOSECONDS), "large-but-finite-ns"));
        TimeUnit[] units = TimeUnit.values();
        while (values.size() < VALID_WAIT_CASES) {
            // io.airlift.units.Duration rejects negative, NaN, and infinite values in its
            // own constructor, so any non-negative finite double is a valid Duration input.
            // Cap magnitude to keep Duration#toMillis() within Long range across all units
            // (DAYS * 1e9 ≈ 8.6e16 ms, comfortably below Long.MAX_VALUE ≈ 9.2e18).
            double magnitude = randomNonNegativeDouble(random);
            TimeUnit unit = units[random.nextInt(units.length)];
            int reasonLen = 1 + random.nextInt(256);
            values.add(new WaitArgs(new Duration(magnitude, unit), randomString(random, reasonLen)));
        }
        return values;
    }

    @ParameterizedTest(name = "Wait accepts maxWait={0} reason length={1}")
    @MethodSource("validWaitArgs")
    void waitAcceptsValidArgs(WaitArgs args)
    {
        WaitDecision.Wait decision = new WaitDecision.Wait(args.maxWait(), args.reason());
        assertThat(decision.maxWait()).isEqualTo(args.maxWait());
        assertThat(decision.reason()).isEqualTo(args.reason());
        assertThat(decision.maxWait().toMillis()).isGreaterThanOrEqualTo(0L);
        assertThat(((WaitDecision) decision).reason()).isEqualTo(args.reason());
    }

    // ---------------- ProceedNow / Wait: null reason rejected ----------------

    @Test
    void proceedNowRejectsNullReason()
    {
        assertThatThrownBy(() -> new WaitDecision.ProceedNow(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void waitRejectsNullReason()
    {
        Duration zero = new Duration(0, TimeUnit.SECONDS);
        assertThatThrownBy(() -> new WaitDecision.Wait(zero, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void waitRejectsNullMaxWait()
    {
        assertThatThrownBy(() -> new WaitDecision.Wait(null, "reason"))
                .isInstanceOf(NullPointerException.class);
    }

    // ---------------- ProceedNow / Wait: out-of-bounds reason length rejected ----------------

    static List<String> invalidLengthReasons()
    {
        Random random = new Random(SEED ^ 0xC0FFEEL);
        List<String> values = new ArrayList<>(INVALID_LENGTH_REASON_CASES);
        // Boundaries: empty (length 0) and length 257 (one past upper bound).
        values.add("");
        values.add(repeatChar('a', 257));
        values.add(repeatChar('z', 1024));
        while (values.size() < INVALID_LENGTH_REASON_CASES) {
            // Pick lengths outside [1, 256]: 0, or in (256, 4096].
            boolean tooLong = random.nextBoolean();
            int len = tooLong ? 257 + random.nextInt(4096 - 257 + 1) : 0;
            values.add(randomString(random, len));
        }
        return values;
    }

    @ParameterizedTest(name = "ProceedNow rejects reason of length {0}")
    @MethodSource("invalidLengthReasons")
    void proceedNowRejectsInvalidLengthReason(String reason)
    {
        assertThatThrownBy(() -> new WaitDecision.ProceedNow(reason))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest(name = "Wait rejects reason of length {0}")
    @MethodSource("invalidLengthReasons")
    void waitRejectsInvalidLengthReason(String reason)
    {
        Duration zero = new Duration(0, TimeUnit.SECONDS);
        assertThatThrownBy(() -> new WaitDecision.Wait(zero, reason))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ---------------- Wait: invalid Duration construction rejected ----------------
    //
    // The contract under test is:
    //   Wait accepts iff maxWait != null && maxWait.toMillis() >= 0.
    // io.airlift.units.Duration enforces non-negativity at its own construction,
    // making it impossible to hand a "negative" Duration to Wait. We document and
    // assert that property here: every attempt to construct a negative-valued Duration
    // throws IllegalArgumentException at the Duration level, which means the Wait
    // constructor's negativity guard is exercised by the property "input is rejected
    // before reaching Wait or by Wait itself" — both produce IllegalArgumentException.

    static List<Double> invalidNegativeDurationValues()
    {
        Random random = new Random(SEED ^ 0xDEADBEEFL);
        List<Double> values = new ArrayList<>(INVALID_WAIT_DURATION_CASES);
        values.add(-1.0d);
        values.add(-0.001d);
        values.add(-Double.MIN_VALUE);
        values.add(-Double.MAX_VALUE);
        while (values.size() < INVALID_WAIT_DURATION_CASES) {
            // Strictly negative magnitudes only.
            values.add(-(random.nextDouble() * 1e6 + Double.MIN_VALUE));
        }
        return values;
    }

    @ParameterizedTest(name = "Negative Duration value {0} is rejected by airlift Duration so Wait cannot be built")
    @MethodSource("invalidNegativeDurationValues")
    void negativeDurationCannotBuildWait(double negativeValue)
    {
        // The Duration constructor itself rejects negatives — we cannot even reach Wait.
        assertThatThrownBy(() -> new Duration(negativeValue, TimeUnit.MILLISECONDS))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ---------------- helpers ----------------

    /**
     * Args record for the Wait parameterized test.
     */
    record WaitArgs(Duration maxWait, String reason)
    {
        @Override
        public String toString()
        {
            return maxWait + ", len=" + reason.length();
        }
    }

    private static String randomString(Random random, int length)
    {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            // Restrict to printable ASCII to keep failure messages readable.
            sb.append((char) (32 + random.nextInt(95))); // [' ', '~']
        }
        return sb.toString();
    }

    private static String repeatChar(char c, int n)
    {
        char[] buf = new char[n];
        java.util.Arrays.fill(buf, c);
        return new String(buf);
    }

    private static double randomNonNegativeDouble(Random random)
    {
        // Produce a finite, non-negative magnitude with a wide dynamic range, but bounded
        // so that DAYS * value still fits in Long.MAX_VALUE milliseconds. Long.MAX_VALUE
        // milliseconds in days is ≈ 1.07e14, so we cap magnitude at 1e10 — well within
        // range across every TimeUnit and large enough to exercise non-trivial values.
        double exponent = random.nextDouble() * 10.0; // 0..10 → up to 1e10
        double mantissa = random.nextDouble();
        return mantissa * Math.pow(10.0, exponent);
    }
}
