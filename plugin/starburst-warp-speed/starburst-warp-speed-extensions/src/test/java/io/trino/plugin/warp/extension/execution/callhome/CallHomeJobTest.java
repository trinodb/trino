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
package io.trino.plugin.warp.extension.execution.callhome;

import io.trino.spi.HostAddress;
import io.varada.cloudvendors.CloudVendorService;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class CallHomeJobTest
{
    @Test
    public void test()
            throws IOException
    {
        HostAddress currentNodeAddress = mock(HostAddress.class);
        CloudVendorService cloudVendorService = mock(CloudVendorService.class);

        String storePathStr = "s3://storePathBucket/storePathS3_1/storePathS3_2";

        Path testBaseDirPath = Files.createTempDirectory("callhome" + UUID.randomUUID());

        String logPath = testBaseDirPath.toFile().getAbsolutePath() + "/logs";
        Files.createDirectory(new File(logPath).toPath());

        List<String> serverLogFiles = List.of("server.log", "launcher.log", "gc.log");

        Stream.concat(serverLogFiles.stream(), Stream.of(UUID.randomUUID().toString()))
                .forEach(fileName -> createFile(logPath, fileName));

        Files.createFile(new File(testBaseDirPath.toFile().getAbsolutePath(), "prop1.properties").toPath());
        String catalogPath = testBaseDirPath.toFile().getAbsolutePath() + "/catalogs";
        Files.createDirectory(new File(catalogPath).toPath());

        List<String> catalogFiles = List.of("prop2.properties", "prop3.properties");
        catalogFiles.forEach(fileName -> createFile(catalogPath, fileName));

        when(cloudVendorService.getLastModified(anyString())).thenReturn(Optional.empty());

        CallHomeJob callHomeJob = new CallHomeJob(
                cloudVendorService,
                currentNodeAddress,
                storePathStr,
                logPath + "/server/",
                catalogPath,
                false);
        callHomeJob.run();

        serverLogFiles.forEach(fileName -> verify(cloudVendorService, times(1)).uploadFileToCloud(anyString(), contains(fileName)));
        Stream.concat(catalogFiles.stream(), Stream.of("prop1.properties"))
                .forEach(fileName -> verify(cloudVendorService, times(1)).uploadFileToCloud(anyString(), contains(fileName)));
    }

    private void createFile(String catalogPath, String fileName)
    {
        try {
            Files.createFile(new File(catalogPath, fileName).toPath());
        }
        catch (IOException e) {
            fail(e);
        }
    }
}
