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
package io.trino.filesystem.ozone;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestOzoneFileSystem
        extends AbstractTestOzoneFileSystem
{
    @BeforeAll
    void setup()
            throws IOException
    {
        initialize("");
    }

    @Test
    void localSetupTest()
            throws IOException
    {
        OzoneConfiguration conf = new OzoneConfiguration();
        OzoneClient ozoneClient = OzoneClientFactory.getRpcClient("127.0.1.1", 9862, conf);

        // Get a reference to the ObjectStore using the client
        ObjectStore objectStore = ozoneClient.getObjectStore();

//        // Let us create a volume to store our game assets.
//        // This default arguments for creating that volume.
//        objectStore.createVolume("assets");
        // Let us verify that the volume got created.
        OzoneVolume assets = objectStore.getVolume("s3v");

        OzoneBucket bucket1 = assets.getBucket("bucket1");

        Iterator<? extends OzoneKey> iterator = bucket1.listKeys("/");
        while (iterator.hasNext()) {
            OzoneKey next = iterator.next();
            System.out.println(next.getVolumeName());
            System.out.println(next.getBucketName());
            System.out.println(next.getName());
            System.out.println(next.getCreationTime());
            System.out.println(next.getDataSize());
            System.out.println(next.getModificationTime());
            System.out.println(next.getReplicationConfig());
            if (next.isFile()) {
                byte[] bytes = bucket1.readFile(next.getName()).readAllBytes();
                System.out.println(new String(bytes, StandardCharsets.UTF_8));
            }
        }
    }
}
