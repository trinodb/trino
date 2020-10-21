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
package io.prestosql.cli.rpm;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.File;
import java.time.Duration;

import static org.testcontainers.containers.wait.strategy.Wait.forLogMessage;

@Test(singleThreaded = true)
public class ClientIT
{
    @Parameters("rpm")
    @Test
    public void testWithJava11(String rpm)
    {
        testClient("prestodev/centos7-oj11", rpm);
    }

    private static void testClient(String baseImage, String rpmHostPath)
    {
        String rpm = "/" + new File(rpmHostPath).getName();

        String command = "" +
                // install RPM
                "yum localinstall -y " + rpm + "\n" +
                "presto --help\n";

        try (GenericContainer<?> container = new GenericContainer<>(baseImage)) {
            container.withFileSystemBind(rpmHostPath, rpm, BindMode.READ_ONLY)
                    .withCommand("sh", "-xeuc", command)
                    .waitingFor(forLogMessage(".*presto - Presto command line interface.*", 1).withStartupTimeout(Duration.ofMinutes(1)))
                    .start();
        }
    }
}
