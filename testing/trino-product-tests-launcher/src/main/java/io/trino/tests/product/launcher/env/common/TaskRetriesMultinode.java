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
package io.trino.tests.product.launcher.env.common;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.Environment;

import java.util.List;

import static io.trino.tests.product.launcher.env.EnvironmentContainers.COORDINATOR;
import static io.trino.tests.product.launcher.env.EnvironmentContainers.isTrinoWorker;
import static io.trino.tests.product.launcher.env.common.Standard.CONTAINER_TRINO_CONFIG_PROPERTIES;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

public class TaskRetriesMultinode
        implements EnvironmentExtender
{
    private final StandardMultinode standardMultinode;
    private final DockerFiles.ResourceProvider configDir;

    @Inject
    public TaskRetriesMultinode(
            StandardMultinode standardMultinode,
            DockerFiles dockerFiles)
    {
        this.standardMultinode = requireNonNull(standardMultinode, "standardMultinode is null");
        this.configDir = dockerFiles.getDockerFilesHostDirectory("common/task-retries-multinode");
    }

    @Override
    public List<EnvironmentExtender> getDependencies()
    {
        return ImmutableList.of(standardMultinode);
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        builder.configureContainer(COORDINATOR, container -> container
                .withCopyFileToContainer(forHostPath(configDir.getPath("multinode-master-config.properties")), CONTAINER_TRINO_CONFIG_PROPERTIES));
        builder.configureContainers(container -> {
            if (isTrinoWorker(container.getLogicalName())) {
                container.withCopyFileToContainer(forHostPath(configDir.getPath("multinode-worker-config.properties")), CONTAINER_TRINO_CONFIG_PROPERTIES);
            }
        });
    }
}
