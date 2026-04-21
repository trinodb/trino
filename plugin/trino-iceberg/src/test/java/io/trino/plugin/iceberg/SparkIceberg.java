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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.testing.containers.BaseTestContainer;
import org.testcontainers.containers.Network;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.testing.TestingProperties.getDockerImagesVersion;

public class SparkIceberg
        extends BaseTestContainer
{
    public static final String SPARK4_ICEBERG_IMAGE = "ghcr.io/trinodb/testing/spark4-iceberg:" + getDockerImagesVersion();

    public static final String HOST_NAME = "spark";

    private static final int SPARK_THRIFT_PORT = 10213;

    public static Builder builder()
    {
        return new Builder();
    }

    private SparkIceberg(
            String image,
            String hostName,
            Set<Integer> ports,
            Map<String, String> filesToMount,
            Map<String, String> envVars,
            Optional<Network> network,
            int startupRetryLimit)
    {
        super(
                image,
                hostName,
                ports,
                filesToMount,
                envVars,
                network,
                startupRetryLimit);
    }

    @Override
    protected void setupContainer()
    {
        super.setupContainer();
        withRunCommand(
                ImmutableList.of(
                        "spark-submit",
                        "--master", "local[*]",
                        "--class", "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2",
                        "--name", "Thrift JDBC/ODBC Server",
                        "--packages", "org.apache.spark:spark-avro_2.12:3.2.1",
                        "--conf", "spark.hive.server2.thrift.port=" + SPARK_THRIFT_PORT,
                        "spark-internal"));
    }

    public static class Builder
            extends BaseTestContainer.Builder<Builder, SparkIceberg>
    {
        private Builder()
        {
            this.image = SPARK4_ICEBERG_IMAGE;
            this.hostName = HOST_NAME;
            this.exposePorts = ImmutableSet.of(SPARK_THRIFT_PORT);
        }

        @Override
        public SparkIceberg build()
        {
            return new SparkIceberg(image, hostName, exposePorts, filesToMount, envVars, network, startupRetryLimit);
        }
    }
}
