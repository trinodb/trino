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
package io.trino.plugin.hive.containers;

import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.testing.containers.Floci;
import org.testcontainers.containers.Network;

import java.net.URI;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.hive.containers.HiveFlociDataLake.State.INITIAL;
import static io.trino.plugin.hive.containers.HiveFlociDataLake.State.STARTING;
import static io.trino.plugin.hive.containers.HiveFlociDataLake.State.STOPPED;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.Network.newNetwork;

public abstract class HiveFlociDataLake
        implements AutoCloseable
{
    private final String bucketName;
    private final Floci floci;

    protected final AutoCloseableCloser closer = AutoCloseableCloser.create();
    protected final Network network;
    protected State state = INITIAL;

    public HiveFlociDataLake(String bucketName)
    {
        this.bucketName = requireNonNull(bucketName, "bucketName is null");
        this.network = closer.register(newNetwork());
        this.floci = closer.register(new Floci()
                .withNetwork(network)
                .withNetworkAliases("floci"));
    }

    public void start()
    {
        checkState(state == INITIAL, "Already started: %s", state);
        state = STARTING;
        floci.start();
        floci.createBucket(bucketName);
    }

    public void stop()
            throws Exception
    {
        closer.close();
        state = STOPPED;
    }

    public Network getNetwork()
    {
        return network;
    }

    public Floci floci()
    {
        return floci;
    }

    public abstract String runOnHive(String sql);

    public abstract HiveHadoop getHiveHadoop();

    public abstract URI getHiveMetastoreEndpoint();

    public String getBucketName()
    {
        return bucketName;
    }

    @Override
    public void close()
            throws Exception
    {
        stop();
    }

    protected enum State
    {
        INITIAL,
        STARTING,
        STARTED,
        STOPPED,
    }
}
