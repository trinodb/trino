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
package io.trino.filesystem.hdfs;

import io.airlift.log.Logger;
import io.trino.testing.containers.BaseTestContainer;
import io.trino.testing.containers.PrintingLogConsumer;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.StandardSystemProperty.USER_NAME;
import static io.trino.testing.TestingProperties.getDockerImagesVersion;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public class Hadoop
        extends BaseTestContainer
{
    private static final Logger log = Logger.get(Hadoop.class);

    private static final String IMAGE = "ghcr.io/trinodb/testing/hdp2.6-hive:" + getDockerImagesVersion();

    private static final int HDFS_PORT = 9000;

    public Hadoop()
    {
        super(
                IMAGE,
                "hadoop-master",
                Set.of(HDFS_PORT),
                emptyMap(),
                Map.of("HADOOP_USER_NAME", requireNonNull(USER_NAME.value())),
                Optional.empty(),
                1);
    }

    @Override
    protected void setupContainer()
    {
        super.setupContainer();
        withLogConsumer(new PrintingLogConsumer("hadoop | "));
        withRunCommand(List.of("bash", "-e", "-c", """
                rm /etc/supervisord.d/{hive*,mysql*,socks*,sshd*,yarn*}.conf
                supervisord -c /etc/supervisord.conf
                """));
    }

    @Override
    public void start()
    {
        super.start();
        executeInContainerFailOnError("hadoop", "fs", "-rm", "-r", "/*");
        log.info("Hadoop container started with HDFS endpoint: %s", getHdfsUri());
    }

    public String getHdfsUri()
    {
        return "hdfs://%s/".formatted(getMappedHostAndPortForExposedPort(HDFS_PORT));
    }
}
