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
package io.trino.plugin.oracle;

import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.MountableFile;

import java.time.Duration;

import static io.trino.testing.containers.TestContainers.startOrReuse;

/**
 * Variant of {@link TestingOracleServer} that runs the non-slim gvenzl/oracle-free image.
 * The slim image strips Oracle Spatial; the non-slim image ships MDSYS.SDO_GEOMETRY and SDO_UTIL,
 * which are required to reproduce and test the geometry-column mapping issue from trinodb/trino#30020.
 * The non-slim image is larger and slower to start.
 */
public class TestingOracleSpatialServer
        extends TestingOracleServer
{
    @Override
    protected void createContainer()
    {
        OracleContainer container = new OracleContainer("gvenzl/oracle-free:23.9")
                .withCopyFileToContainer(MountableFile.forClasspathResource("init.sql"), "/container-entrypoint-initdb.d/01-init.sql")
                .withCopyFileToContainer(MountableFile.forClasspathResource("restart.sh"), "/container-entrypoint-initdb.d/02-restart.sh")
                .withCopyFileToContainer(MountableFile.forHostPath(createConfigureScript()), "/container-entrypoint-initdb.d/03-create-users.sql")
                .waitingFor(Wait.forLogMessage(".*DATABASE IS READY TO USE!.*\\s", 1).withStartupTimeout(Duration.ofMinutes(10)))
                .withStartupTimeoutSeconds(600);
        try {
            this.cleanup = startOrReuse(container);
            this.container = container;
        }
        catch (Throwable e) {
            try (container) {
                throw e;
            }
        }
    }
}
