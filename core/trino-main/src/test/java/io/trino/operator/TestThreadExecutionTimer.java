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
package io.trino.operator;

import io.airlift.vthreadtime.VirtualThreadTime;
import io.trino.operator.OperationTimer.OperationTiming;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestThreadExecutionTimer
{
    @Test
    public void testVirtualThreadMountedTime()
            throws InterruptedException
    {
        assumeTrue(VirtualThreadTime.isSupported());

        AtomicReference<Measurement> result = new AtomicReference<>();
        Thread thread = Thread.ofVirtual().start(() -> {
            try (ThreadExecutionTimer timer = ThreadExecutionTimer.start()) {
                long wallStart = System.nanoTime();
                OperationTimer operationTimer = new OperationTimer(true, true);
                OperationTiming operationTiming = new OperationTiming();
                OperationTiming overallTiming = new OperationTiming();

                spin(Duration.ofMillis(20));
                operationTimer.recordOperationComplete(operationTiming);
                LockSupport.parkNanos(Duration.ofMillis(100).toNanos());
                spin(Duration.ofMillis(20));
                operationTimer.recordOperationComplete(operationTiming);
                operationTimer.end(overallTiming);

                result.set(new Measurement(
                        System.nanoTime() - wallStart,
                        timer.elapsedNanos(),
                        overallTiming.getWallNanos(),
                        overallTiming.getCpuNanos(),
                        operationTiming.getCpuNanos()));
            }
        });
        thread.join();

        Measurement measurement = result.get();
        assertThat(measurement.mountedNanos()).isGreaterThan(Duration.ofMillis(10).toNanos());
        assertThat(measurement.wallNanos() - measurement.mountedNanos()).isGreaterThan(Duration.ofMillis(50).toNanos());
        assertThat(measurement.operationWallNanos()).isGreaterThan(measurement.operationCpuNanos());
        assertThat(measurement.operationCpuNanos()).isGreaterThan(Duration.ofMillis(10).toNanos());
        assertThat(measurement.operatorCpuNanos()).isGreaterThan(Duration.ofMillis(10).toNanos());
        assertThat(measurement.operationCpuNanos()).isLessThanOrEqualTo(measurement.mountedNanos());
    }

    private static void spin(Duration duration)
    {
        long end = System.nanoTime() + duration.toNanos();
        while (System.nanoTime() < end) {
            Thread.onSpinWait();
        }
    }

    private record Measurement(
            long wallNanos,
            long mountedNanos,
            long operationWallNanos,
            long operationCpuNanos,
            long operatorCpuNanos) {}
}
