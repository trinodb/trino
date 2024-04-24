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
package io.trino.plugin.kudu;

import com.github.dockerjava.api.command.InspectContainerResponse;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class KuduTabletWaitStrategy
        extends AbstractWaitStrategy
{
    private final GenericContainer<?> master;

    public KuduTabletWaitStrategy(GenericContainer<?> master)
    {
        this.master = requireNonNull(master, "master is null");
    }

    @Override
    protected void waitUntilReady()
    {
        Failsafe.with(RetryPolicy.builder()
                        .withMaxDuration(startupTimeout)
                        .withMaxAttempts(Integer.MAX_VALUE) // limited by MaxDuration
                        .abortOn(e -> getExitCode().isPresent())
                        .build())
                .run(() -> {
                    // Note: This condition requires a dependency on org.rnorth.duct-tape:duct-tape
                    if (!getRateLimiter().getWhenReady(() -> master.getLogs().contains("Registered new tserver with Master"))) {
                        // We say "timed out" immediately. Failsafe will propagate this only when timeout reached.
                        throw new ContainerLaunchException("Timed out waiting for container to register tserver");
                    }
                });
    }

    private Optional<Long> getExitCode()
    {
        if (waitStrategyTarget.getContainerId() == null) {
            // Not yet started
            return Optional.empty();
        }

        InspectContainerResponse currentContainerInfo = waitStrategyTarget.getCurrentContainerInfo();
        if (currentContainerInfo.getState().getStartedAt() == null) {
            // not yet running
            return Optional.empty();
        }
        // currentContainerInfo.getState().getExitCode() is present (0) even in "running" state
        if (Boolean.TRUE.equals(currentContainerInfo.getState().getRunning())) {
            // running
            return Optional.empty();
        }
        return Optional.ofNullable(currentContainerInfo.getState().getExitCodeLong());
    }
}
