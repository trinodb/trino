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
package io.trino.plugin.ignite;

import java.io.Closeable;

import static io.trino.plugin.ignite.TestIgniteContainer.HTTP_PORT;
import static java.lang.String.format;

public class TestingIgniteServer
        implements Closeable
{
    private static final String IGNITE_IMAGE = "apacheignite/ignite:2.10.0";
    private final TestIgniteContainer dockerContainer;

    public TestingIgniteServer()
    {
        // Use 2nd stable version
        dockerContainer = (TestIgniteContainer) new TestIgniteContainer(IGNITE_IMAGE)
                .withStartupAttempts(10);

        dockerContainer.start();
        System.out.println("aaaaaa");
    }

    public String getJdbcUrl()
    {
        return format("jdbc:ignite:thin://%s:%s/", dockerContainer.getContainerIpAddress(),
                dockerContainer.getMappedPort(HTTP_PORT));
    }

    @Override
    public void close()
    {
        dockerContainer.stop();
    }
}
