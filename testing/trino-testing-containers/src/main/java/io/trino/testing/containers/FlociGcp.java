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
package io.trino.testing.containers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;

public final class FlociGcp
        extends GenericContainer<FlociGcp>
{
    public static final String FLOCI_GCP_IMAGE = "floci/floci-gcp:0.3.0";
    public static final String FLOCI_GCP_PROJECT_ID = "floci-local";

    private static final int FLOCI_GCP_PORT = 4588;

    public FlociGcp()
    {
        super(DockerImageName.parse(FLOCI_GCP_IMAGE));
        addExposedPort(FLOCI_GCP_PORT);
        waitingFor(Wait.forLogMessage(".*=== floci-gcp Ready ===.*\\n", 1));
    }

    public URI getEndpoint()
    {
        return URI.create("http://%s:%s".formatted(getHost(), getMappedPort(FLOCI_GCP_PORT)));
    }
}
