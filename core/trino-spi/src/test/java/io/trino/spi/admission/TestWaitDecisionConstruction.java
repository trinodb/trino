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

// Validates WaitDecision construction. We use junit-jupiter-params @ParameterizedTest with
// @MethodSource to generate 100+ deterministic cases per branch (seeded java.util.Random for
// reproducibility). The constructors accept iff (reason != null && reason.length() in [1, 256])
// and, for Wait, additionally (releaseCondition != null && maxWait != null && !maxWait.isNegative());
// other inputs throw IllegalArgumentException or NullPointerException. The two factories are also
// checked: forClusterCapacity yields an empty releaseCondition; forCondition yields a present one.

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("WaitDecision construction validates input")
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

    // ---------------- Wait: valid (reason, maxWait) accepted via both factories ----------------

    static List<WaitArgs> validWaitArgs()
    {
        Random random = new Random(SEED ^ 0x55AAL);
        List<WaitArgs> values = new ArrayList<>(VALID_WAIT_CASES);
        // Boundary cases.
        values.add(new WaitArgs(Duration.ZERO, "x"));
        values.add(new WaitArgs(Duration.ofNanos(0), repeatChar('a', 256)));
        values.add(new WaitArgs(Duration.ofNanos(1), "boundary-just-above-zero"));
        values.add(new WaitArgs(Duration.ofMillis(1), repeatChar('b', 1)));
        values.add(new WaitArgs(Duration.ofNanos((long) 1e15), "large-but-finite-ns"));
        while (values.size() < VALID_WAIT_CASES) {
            // Any non-negative java.time.Duration is a valid input; the Wait constructor
            // rejects only null and negative durations. Generate non-negative magnitudes
            // across a wide dynamic range.
            int reasonLen = 1 + random.nextInt(256);
            values.add(new WaitArgs(randomNonNegativeDuration(random), randomString(random, reasonLen)));
        }
        return values;
    }

    @ParameterizedTest(name = "Wait.forClusterCapacity accepts maxWait={0} reason length={1}")
    @MethodSource("validWaitArgs")
    void forClusterCapacityAcceptsValidArgsAndHasEmptyReleaseCondition(WaitArgs args)
    {
        WaitDecision.Wait decision = WaitDecision.Wait.forClusterCapacity(args.maxWait(), args.reason());
        assertThat(decision.releaseCondition()).isEmpty();
        assertThat(decision.maxWait()).isEqualTo(args.maxWait());
        assertThat(decision.reason()).isEqualTo(args.reason());
        assertThat(decision.maxWait().isNegative()).isFalse();
        assertThat(((WaitDecision) decision).reason()).isEqualTo(args.reason());
    }

    @ParameterizedTest(name = "Wait.forCondition accepts maxWait={0} reason length={1}")
    @MethodSource("validWaitArgs")
    void forConditionAcceptsValidArgsAndHasPresentReleaseCondition(WaitArgs args)
    {
        CompletableFuture<Void> condition = new CompletableFuture<>();
        WaitDecision.Wait decision = WaitDecision.Wait.forCondition(condition, args.maxWait(), args.reason());
        assertThat(decision.releaseCondition()).containsSame(condition);
        assertThat(decision.maxWait()).isEqualTo(args.maxWait());
        assertThat(decision.reason()).isEqualTo(args.reason());
    }

    // ---------------- ProceedNow / Wait: null argument rejected ----------------

    @Test
    void proceedNowRejectsNullReason()
    {
        assertThatThrownBy(() -> new WaitDecision.ProceedNow(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void waitRejectsNullReason()
    {
        assertThatThrownBy(() -> WaitDecision.Wait.forClusterCapacity(Duration.ZERO, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void waitRejectsNullMaxWait()
    {
        assertThatThrownBy(() -> WaitDecision.Wait.forClusterCapacity(null, "reason"))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void waitRejectsNullReleaseConditionOptional()
    {
        assertThatThrownBy(() -> new WaitDecision.Wait(null, Duration.ZERO, "reason"))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void forConditionRejectsNullFuture()
    {
        assertThatThrownBy(() -> WaitDecision.Wait.forCondition(null, Duration.ZERO, "reason"))
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
        assertThatThrownBy(() -> WaitDecision.Wait.forClusterCapacity(Duration.ZERO, reason))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ---------------- Wait: negative maxWait rejected ----------------
    //
    // The contract under test is:
    //   Wait accepts iff maxWait != null && !maxWait.isNegative().
    // Unlike io.airlift.units.Duration, java.time.Duration permits negative values, so a
    // negative Duration is a well-formed object that reaches the Wait constructor. The
    // constructor's own negativity guard must reject it with IllegalArgumentException.

    static List<Duration> invalidNegativeDurations()
    {
        Random random = new Random(SEED ^ 0xDEADBEEFL);
        List<Duration> values = new ArrayList<>(INVALID_WAIT_DURATION_CASES);
        values.add(Duration.ofMillis(-1));
        values.add(Duration.ofNanos(-1));
        values.add(Duration.ofSeconds(-1));
        values.add(Duration.ofNanos(Long.MIN_VALUE));
        while (values.size() < INVALID_WAIT_DURATION_CASES) {
            // Strictly negative magnitudes only.
            values.add(Duration.ofNanos(-(1 + (random.nextLong() >>> 1) % (long) 1e12)));
        }
        return values;
    }

    @ParameterizedTest(name = "Wait rejects negative maxWait {0}")
    @MethodSource("invalidNegativeDurations")
    void waitRejectsNegativeMaxWait(Duration negativeMaxWait)
    {
        assertThatThrownBy(() -> WaitDecision.Wait.forClusterCapacity(negativeMaxWait, "reason"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WaitDecision.Wait.forCondition(new CompletableFuture<>(), negativeMaxWait, "reason"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ---------------- helpers ----------------

    /**
     * Args record for the Wait parameterized tests.
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
        Arrays.fill(buf, c);
        return new String(buf);
    }

    private static Duration randomNonNegativeDuration(Random random)
    {
        // Produce a finite, non-negative duration with a wide dynamic range. Cap the
        // magnitude well below Long.MAX_VALUE nanoseconds so toNanos()/toMillis() stay
        // in range while still exercising non-trivial values.
        double exponent = random.nextDouble() * 15.0; // 0..15 → up to 1e15 ns (~11.6 days)
        long nanos = (long) (random.nextDouble() * Math.pow(10.0, exponent));
        return Duration.ofNanos(nanos);
    }
}
