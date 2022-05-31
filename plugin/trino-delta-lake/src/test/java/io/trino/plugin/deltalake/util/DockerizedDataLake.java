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
package io.trino.plugin.deltalake.util;

import com.google.common.io.Closer;
import org.testcontainers.containers.Network;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.deltalake.util.DockerImages.DEFAULT_HADOOP_BASE_IMAGE;
import static io.trino.plugin.deltalake.util.DockerImages.DOCKER_IMAGES_VERSION;
import static java.lang.String.format;

public class DockerizedDataLake
        implements Closeable
{
    protected final Network network;
    protected final TestingHadoop testingHadoop;
    protected final Closer closer = Closer.create();

    public DockerizedDataLake(
            Optional<String> hadoopBaseImage,
            Map<String, String> hadoopImageResourceMap,
            Map<String, String> hadoopImageFileMap)
    {
        try {
            network = Network.newNetwork();
            closer.register(network::close);
            this.testingHadoop = TestingHadoop.builder(format("%s:%s", hadoopBaseImage.orElse(DEFAULT_HADOOP_BASE_IMAGE), DOCKER_IMAGES_VERSION))
                    .setResourcesToMount(hadoopImageResourceMap)
                    .setFilesToMount(hadoopImageFileMap)
                    .setNetwork(network)
                    .build();
            closer.register(testingHadoop);
        }
        catch (Exception e) {
            try {
                closer.close();
            }
            catch (IOException closeException) {
                if (e != closeException) {
                    e.addSuppressed(closeException);
                }
            }
            throw e;
        }
    }

    public DockerizedDataLake(TestingHadoop testingHadoop)
    {
        try {
            network = testingHadoop.getContainer().getNetwork();
            closer.register(network::close);
            this.testingHadoop = testingHadoop;
            closer.register(testingHadoop);
        }
        catch (Exception e) {
            try {
                closer.close();
            }
            catch (IOException closeException) {
                if (e != closeException) {
                    e.addSuppressed(closeException);
                }
            }
            throw e;
        }
    }

    @Override
    public void close()
            throws IOException
    {
        closer.close();
    }

    public TestingHadoop getTestingHadoop()
    {
        return testingHadoop;
    }
}
