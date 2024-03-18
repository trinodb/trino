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
package io.varada.cloudstorage.hdfs;

import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.TrinoOutputFile;
import io.varada.cloudstorage.CloudStorageService;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

public class HdfsCloudStorage
        extends CloudStorageService
{
    private static final Logger logger = Logger.get(HdfsCloudStorage.class);

    public HdfsCloudStorage(TrinoFileSystemFactory fileSystemFactory)
    {
        super(fileSystemFactory);
    }

    @Override
    public void uploadFile(Location source, Location target)
            throws IOException
    {
        copyFile(source, target);
    }

    @Override
    public void downloadFile(Location source, Location target)
            throws IOException
    {
        copyFile(source, target);
        // workaround for https://issues.apache.org/jira/browse/HADOOP-7199 - delete local crc file if exist
        String crcFileName = target.sibling("." + target.fileName() + ".crc").toString();
        File crcFile = new File(crcFileName);
        if (crcFile.exists()) {
            if (!crcFile.delete()) {
                logger.error("downloadFile fail to delete crcFile %s", crcFileName);
            }
        }
    }

    @Override
    public void copyFile(Location source, Location destination)
            throws IOException
    {
        TrinoInputFile inputFile = newInputFile(source);
        TrinoOutputFile outputFile = newOutputFile(destination);
        fileSystem.deleteFile(destination);
        try (TrinoInputStream inputStream = inputFile.newStream();
                OutputStream outputStream = outputFile.create()) {
            byte[] bytes = new byte[8192 * 100]; // PageSize * 100
            int length;

            while ((length = inputStream.read(bytes)) > 0) {
                outputStream.write(bytes, 0, length);
            }
        }
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        copyFile(source, target);
        deleteFile(source);
    }
}
