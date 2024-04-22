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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import org.testcontainers.containers.Network;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class Kes
        extends BaseTestContainer
{
    public static final int KES_PORT = 7373;
    private static final Logger log = Logger.get(Kes.class);
    private static final String DEFAULT_IMAGE = "minio/kes:2024-06-17T15-47-05Z";
    private static final String DEFAULT_HOST_NAME = "kes";

    public static Kes.Builder builder()
    {
        return new Builder();
    }

    protected Kes(String image, String hostName, Set<Integer> ports, Map<String, String> filesToMount, Map<String, String> envVars, Optional<Network> network, int startupRetryLimit)
    {
        super(image, hostName, ports, filesToMount, envVars, network, startupRetryLimit);
    }

    @Override
    protected void setupContainer()
    {
        super.setupContainer();
        withRunCommand(
                ImmutableList.of("server",
                        "--config", "config.yml"));
        copyResourceToContainer("kes/config.yml", "/config.yml");
        copyResourceToContainer("kes/kes.key", "/kes.key");
        copyResourceToContainer("kes/kes.crt", "/kes.crt");
    }

    @Override
    public void start()
    {
        super.start();
        log.info("MinIO KES container started with address: https://%s", getMappedHostAndPortForExposedPort(KES_PORT));
    }

    public String getMinioKesEndpointURL()
    {
        return "https://%s:%s".formatted(DEFAULT_HOST_NAME, KES_PORT);
    }

    public static class Builder
            extends BaseTestContainer.Builder<Kes.Builder, Kes>
    {
        private Builder()
        {
            this.image = DEFAULT_IMAGE;
            this.hostName = DEFAULT_HOST_NAME;
            this.exposePorts = ImmutableSet.of(KES_PORT);
        }

        @Override
        public Kes build()
        {
            return new Kes(image, hostName, exposePorts, filesToMount, envVars, network, startupRetryLimit);
        }
    }
}
