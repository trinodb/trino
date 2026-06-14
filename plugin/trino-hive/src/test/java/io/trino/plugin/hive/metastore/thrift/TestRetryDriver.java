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
package io.trino.plugin.hive.metastore.thrift;

import io.airlift.units.Duration;
import io.trino.hive.thrift.metastore.MetaException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestRetryDriver
{
    @Test
    void testSuccessfulCall()
            throws Exception
    {
        AtomicInteger attempts = new AtomicInteger(0);
        String result = RetryDriver.retry().maxAttempts(3).run("test", () -> {
            attempts.incrementAndGet();
            return "success";
        });

        assertThat(result).isEqualTo("success");
        assertThat(attempts.get()).isEqualTo(1);
    }

    @Test
    void testRetryOnFailure()
            throws Exception
    {
        AtomicInteger attempts = new AtomicInteger(0);
        String result = RetryDriver.retry()
                .maxAttempts(3)
                .exponentialBackoff(new Duration(1, MILLISECONDS), new Duration(10, MILLISECONDS),
                        new Duration(1, SECONDS), 2.0)
                .run("test", () -> {
                    if (attempts.incrementAndGet() < 3) {
                        throw new RuntimeException("Temporary failure");
                    }
                    return "success";
                });

        assertThat(result).isEqualTo("success");
        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    void testStopOnSpecificException()
    {
        AtomicInteger attempts = new AtomicInteger(0);

        assertThatThrownBy(
                () -> RetryDriver.retry().maxAttempts(5).stopOn(IllegalArgumentException.class).run("test", () -> {
                    attempts.incrementAndGet();
                    throw new IllegalArgumentException("Stop immediately");
                })).isInstanceOf(IllegalArgumentException.class).hasMessage("Stop immediately");

        // Should stop on first attempt without retrying
        assertThat(attempts.get()).isEqualTo(1);
    }

    @Test
    void testDoNotRetryOnAccessControlException()
    {
        AtomicInteger attempts = new AtomicInteger(0);

        // Simulate MetaException wrapping AccessControlException (as seen in HDFS
        // permission denied errors)
        assertThatThrownBy(() -> RetryDriver.retry()
                .maxAttempts(5)
                .exponentialBackoff(new Duration(1, MILLISECONDS), new Duration(10, MILLISECONDS),
                        new Duration(1, SECONDS), 2.0)
                .run("test", () -> {
                    attempts.incrementAndGet();
                    throw new MetaException(
                            "org.apache.hadoop.security.AccessControlException: Permission denied: user=testuser, access=EXECUTE, inode=\"/user/hive\"");
                })).isInstanceOf(MetaException.class).hasMessageContaining("AccessControlException");

        // Should stop on first attempt without retrying
        assertThat(attempts.get()).isEqualTo(1);
    }

    @Test
    void testDoNotRetryOnNestedAccessControlException()
    {
        AtomicInteger attempts = new AtomicInteger(0);

        // Simulate exception with AccessControlException in the cause chain
        Exception accessControlCause = new RuntimeException(
                "org.apache.hadoop.security.AccessControlException: Permission denied");
        Exception wrappedException = new RuntimeException("Wrapper exception", accessControlCause);

        assertThatThrownBy(() -> RetryDriver.retry()
                .maxAttempts(5)
                .exponentialBackoff(new Duration(1, MILLISECONDS), new Duration(10, MILLISECONDS),
                        new Duration(1, SECONDS), 2.0)
                .run("test", () -> {
                    attempts.incrementAndGet();
                    throw wrappedException;
                })).isInstanceOf(RuntimeException.class).hasMessageContaining("Wrapper exception");

        // Should stop on first attempt without retrying
        assertThat(attempts.get()).isEqualTo(1);
    }

    @Test
    void testRetryOnOtherMetaException()
            throws Exception
    {
        AtomicInteger attempts = new AtomicInteger(0);

        // MetaException without AccessControlException should be retried
        String result = RetryDriver.retry()
                .maxAttempts(3)
                .exponentialBackoff(new Duration(1, MILLISECONDS), new Duration(10, MILLISECONDS),
                        new Duration(1, SECONDS), 2.0)
                .run("test", () -> {
                    if (attempts.incrementAndGet() < 3) {
                        throw new MetaException("Temporary metastore error");
                    }
                    return "success";
                });

        assertThat(result).isEqualTo("success");
        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    void testMaxAttemptsExceeded()
    {
        AtomicInteger attempts = new AtomicInteger(0);

        assertThatThrownBy(() -> RetryDriver.retry()
                .maxAttempts(3)
                .exponentialBackoff(new Duration(1, MILLISECONDS), new Duration(10, MILLISECONDS),
                        new Duration(1, SECONDS), 2.0)
                .run("test", () -> {
                    attempts.incrementAndGet();
                    throw new RuntimeException("Always fails");
                })).isInstanceOf(RuntimeException.class).hasMessage("Always fails");

        assertThat(attempts.get()).isEqualTo(3);
    }
}
