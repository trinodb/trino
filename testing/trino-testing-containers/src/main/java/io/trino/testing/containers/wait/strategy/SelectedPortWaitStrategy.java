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
package io.trino.testing.containers.wait.strategy;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.wait.internal.ExternalPortListeningCheck;
import org.testcontainers.containers.wait.internal.InternalCommandPortListeningCheck;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

// TODO add tests (https://github.com/trinodb/trino/issues/12010)
// This not only adds "wait for selected port" functionality (https://github.com/testcontainers/testcontainers-java/issues/2227),
// but also addresses (https://github.com/testcontainers/testcontainers-java/issues/2225)
public final class SelectedPortWaitStrategy
        extends AbstractWaitStrategy
{
    private final Set<Integer> exposedPorts;

    public SelectedPortWaitStrategy(int... exposedPorts)
    {
        this(ImmutableSet.copyOf(Ints.asList(exposedPorts)));
    }

    public SelectedPortWaitStrategy(Set<Integer> exposedPorts)
    {
        requireNonNull(exposedPorts, "exposedPorts is null");
        checkArgument(!exposedPorts.isEmpty(), "exposedPorts is empty");
        exposedPorts.forEach(exposedPort -> checkArgument(exposedPort != null && exposedPort > 0, "Invalid exposedPort: %s", exposedPort));
        this.exposedPorts = ImmutableSet.copyOf(exposedPorts);
    }

    @Override
    protected void waitUntilReady()
    {
        Callable<Boolean> internalCheck = new InternalCommandPortListeningCheck(waitStrategyTarget, exposedPorts);

        Set<Integer> externalPorts = exposedPorts.stream()
                .map(waitStrategyTarget::getMappedPort)
                .collect(toImmutableSet());
        Callable<Boolean> externalCheck = new ExternalPortListeningCheck(waitStrategyTarget, externalPorts);

        Failsafe.with(RetryPolicy.builder()
                        .withMaxDuration(startupTimeout)
                        .withMaxAttempts(Integer.MAX_VALUE) // limited by MaxDuration
                        .abortOn(e -> getExitCode().isPresent())
                        .build())
                .run(() -> {
                    // Note: This condition requires a dependency on org.rnorth.duct-tape:duct-tape
                    if (!getRateLimiter().getWhenReady(() -> internalCheck.call() && externalCheck.call())) {
                        // We say "timed out" immediately. Failsafe will propagate this only when timeout reached.
                        throw new ContainerLaunchException(format(
                                "Timed out waiting for container port to open (%s ports: %s should be listening)",
                                waitStrategyTarget.getHost(),
                                exposedPorts));
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
        if (currentContainerInfo.getState().getRunning() != null && currentContainerInfo.getState().getRunning()) {
            // running
            return Optional.empty();
        }
        return Optional.ofNullable(currentContainerInfo.getState().getExitCodeLong());
    }
}
