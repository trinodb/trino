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
package io.trino.plugin.varada.storage.capacity;

import io.trino.plugin.varada.TestingTxService;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.configuration.WarmupDemoterConfiguration;
import io.trino.plugin.varada.di.VaradaInitializedServiceRegistry;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.engine.nativeimpl.NativeStorageStateHandler;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.mockito.Mockito.mock;

public class WorkerCapacityManagerTest
{
    private WorkerCapacityManager workerCapacityManager;
    private GlobalConfiguration globalConfiguration;

    @BeforeEach
    public void before()
    {
        globalConfiguration = new GlobalConfiguration();
        workerCapacityManager = new WorkerCapacityManager(globalConfiguration,
                mock(WarmupDemoterConfiguration.class),
                mock(StorageEngineConstants.class),
                mock(NativeStorageStateHandler.class),
                mock(VaradaInitializedServiceRegistry.class),
                TestingTxService.createMetricsManager());
    }

    @Test
    void tesCleanLocalStorage()
    {
        Path localStorePath;
        try {
            localStorePath = Files.createTempDirectory("cleanLocalStorage1");
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        File localStoreDirectory = localStorePath.toFile();
        globalConfiguration.setLocalStorePath(localStoreDirectory.getAbsolutePath());

        try {
            int numFiles = 10;
            for (int i = 0; i < numFiles; i++) {
                String tempFileName = String.format(localStorePath + "/test/case-%d/bucket/schema/table/part-%05d-7a144fa0-52b0-473d-b1ab-a5dbbadd01ae-c000.snappy.parquet/0/110606/", i, i);
                new File(tempFileName).mkdirs();
                File tempFile = new File(tempFileName + "1549797223000-" + i);

                if (tempFile.createNewFile()) {
                    byte[] array = new byte[8192];
                    new Random().nextBytes(array);
                    String generatedString = new String(array, UTF_8);

                    Writer writer = Files.newBufferedWriter(tempFile.toPath(), UTF_8);
                    writer.write(generatedString);
                    writer.close();
                }
                else {
                    throw new RuntimeException("failed to create file");
                }
            }
            Assertions.assertEquals(1, requireNonNull(localStoreDirectory.list()).length);

            workerCapacityManager.init();

            try {
                while (workerCapacityManager.getTotalCapacity() == 0) {
                    Thread.sleep(100);
                }
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            Assertions.assertEquals(0, requireNonNull(localStoreDirectory.list()).length);
            FileUtils.deleteDirectory(localStoreDirectory);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testAvoidCleanLocalStorage()
    {
        Path localStorePath;
        try {
            localStorePath = Files.createTempDirectory("cleanLocalStorage2");
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        File localStoreDirectory = localStorePath.toFile();
        globalConfiguration.setLocalStorePath(localStoreDirectory.getAbsolutePath());

        try {
            String tempFileName = String.format(localStorePath + "/test/bucket/schema/table/part-7a144fa0-52b0-473d-b1ab-a5dbbadd01ae-c000.snappy.parquet/0/110606/");
            new File(tempFileName).mkdirs();
            File tempFile = new File(tempFileName + "1549797223000");

            if (tempFile.createNewFile()) {
                byte[] array = new byte[8192];
                new Random().nextBytes(array);
                String generatedString = new String(array, UTF_8);

                Writer writer = Files.newBufferedWriter(tempFile.toPath(), UTF_8);
                writer.write(generatedString);
                writer.close();
            }
            else {
                throw new RuntimeException("failed to create file");
            }

            File doNotRemoveFile = new File(localStorePath + "/DO-NOT-REMOVE");
            if (!doNotRemoveFile.createNewFile()) {
                throw new RuntimeException("failed to create file");
            }

            Assertions.assertEquals(2, requireNonNull(localStoreDirectory.list()).length);

            workerCapacityManager.init();

            try {
                while (workerCapacityManager.getTotalCapacity() == 0) {
                    Thread.sleep(100);
                }
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            Assertions.assertEquals(1, requireNonNull(localStoreDirectory.list()).length);
            FileUtils.deleteDirectory(localStoreDirectory);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
