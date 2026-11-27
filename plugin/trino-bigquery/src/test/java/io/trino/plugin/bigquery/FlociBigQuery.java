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
package io.trino.plugin.bigquery;

import com.github.dockerjava.api.model.Bind;
import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.common.io.Closer;
import io.trino.testing.containers.FlociGcp;
import org.testcontainers.containers.Network;

import java.net.URI;
import java.util.Base64;

import static io.trino.testing.containers.FlociGcp.FLOCI_GCP_PROJECT_ID;
import static java.nio.charset.StandardCharsets.UTF_8;

public class FlociBigQuery
        implements AutoCloseable
{
    private final Closer closer;
    private final FlociGcp floci;

    public FlociBigQuery()
    {
        closer = Closer.create();
        Network network = Network.newNetwork();
        closer.register(network::close);

        floci = new FlociGcp()
                .withNetwork(network)
                .withEnv("FLOCI_GCP_SERVICES_DOCKER_NETWORK", network.getId())
                .withCreateContainerCmdModifier(command -> command.getHostConfig()
                        .withBinds(Bind.parse("/var/run/docker.sock:/var/run/docker.sock")));
        closer.register(floci::close);
    }

    public void start()
    {
        floci.start();
    }

    public BigQuery bigQuery()
    {
        return BigQueryOptions.newBuilder()
                .setHost(floci.getEndpoint().toString())
                .setProjectId(FLOCI_GCP_PROJECT_ID)
                .setCredentials(NoCredentials.getInstance())
                .build()
                .getService();
    }

    public URI endpoint()
    {
        return floci.getEndpoint();
    }

    public String credentialsKey()
    {
        return Base64.getEncoder().encodeToString(floci.getServiceAccountJson().getBytes(UTF_8));
    }

    @Override
    public void close()
            throws Exception
    {
        closer.close();
    }
}
