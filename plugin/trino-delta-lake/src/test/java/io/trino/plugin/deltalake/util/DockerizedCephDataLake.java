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

import io.trino.testing.ceph.CephClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.deltalake.util.CephContainer.CEPH_ACCESS_KEY;
import static io.trino.plugin.deltalake.util.CephContainer.CEPH_SECRET_KEY;

public final class DockerizedCephDataLake
        extends DockerizedDataLake
{
    private final CephContainer cephServer;
    private final CephClient cephClient;
    private final String bucketName;

    public DockerizedCephDataLake(
            String bucketName,
            Optional<String> hadoopBaseImage,
            Map<String, String> hadoopImageResourceMap,
            Map<String, String> hadoopImageFileMap)
    {
        super(hadoopBaseImage, hadoopImageResourceMap, hadoopImageFileMap);
        try {
            this.bucketName = bucketName;
            this.cephServer = initCephServer();
            this.cephClient = initCephClient(this.cephServer.getCephEndpoint(), bucketName);
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

    private CephContainer initCephServer()
    {
        CephContainer container = new CephContainer(network);
        closer.register(container);
        container.start();
        return container;
    }

    private static CephClient initCephClient(String s3endpoint, String bucketName)
    {
        CephClient client = new CephClient(s3endpoint, CEPH_ACCESS_KEY, CEPH_SECRET_KEY);
        client.makeBucket(bucketName);
        return client;
    }

    public void copyResources(String resourcePath, String target)
    {
        cephClient.copyResourcePath(bucketName, resourcePath, target);
    }

    public List<String> listFiles(String targetDirectory)
    {
        return cephClient.listObjects(bucketName, targetDirectory);
    }

    public String getCephEndpoint()
    {
        return cephServer.getCephEndpoint();
    }
}
