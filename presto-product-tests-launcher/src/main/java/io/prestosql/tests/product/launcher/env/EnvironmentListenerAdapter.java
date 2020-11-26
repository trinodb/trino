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
package io.prestosql.tests.product.launcher.env;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.google.common.base.Stopwatch;

import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class EnvironmentListenerAdapter
{
    private final Stopwatch startupTime = Stopwatch.createUnstarted();
    private final DockerContainer container;
    private EnvironmentListener delegate = EnvironmentListener.NO_OP;

    EnvironmentListenerAdapter(DockerContainer container)
    {
        this.container = requireNonNull(container, "container is null");
    }

    public void set(EnvironmentListener listener)
    {
        requireNonNull(listener, "listener is null");

        delegate = listener;
    }

    public Stopwatch getStartupTime()
    {
        return startupTime;
    }

    public void containerIsStarting(InspectContainerResponse containerInfo, Consumer<InspectContainerResponse> additionalAction)
    {
        this.startupTime.start();
        additionalAction.accept(containerInfo);
        delegate.containerStarting(container, containerInfo);
    }

    public void containerIsStarted(InspectContainerResponse containerInfo, Consumer<InspectContainerResponse> additionalAction)
    {
        this.startupTime.stop();
        additionalAction.accept(containerInfo);
        delegate.containerStarted(container, containerInfo);
    }

    public void containerIsStopping(InspectContainerResponse containerInfo, Consumer<InspectContainerResponse> additionalAction)
    {
        additionalAction.accept(containerInfo);
        delegate.containerStopping(container, containerInfo);
    }

    public void containerIsStopped(InspectContainerResponse containerInfo, Consumer<InspectContainerResponse> additionalAction)
    {
        additionalAction.accept(containerInfo);
        delegate.containerStopped(container, containerInfo);
    }
}
