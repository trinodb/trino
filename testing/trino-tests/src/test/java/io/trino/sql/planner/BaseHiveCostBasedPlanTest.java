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

package io.trino.sql.planner;

import io.trino.plugin.hive.RecordingMetastoreConfig;
import io.trino.plugin.hive.TestingHiveConnectorFactory;
import io.trino.plugin.hive.metastore.UnimplementedHiveMetastore;
import io.trino.plugin.hive.metastore.recording.HiveMetastoreRecording;
import io.trino.plugin.hive.metastore.recording.RecordingHiveMetastore;
import io.trino.spi.connector.ConnectorFactory;
import org.testng.annotations.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.hive.metastore.recording.TestRecordingHiveMetastore.createJsonCodec;

public abstract class BaseHiveCostBasedPlanTest
        extends BaseCostBasedPlanTest
{
    @Override
    protected ConnectorFactory createConnectorFactory()
    {
        RecordingMetastoreConfig recordingConfig = new RecordingMetastoreConfig()
                .setRecordingPath(getRecordingPath())
                .setReplay(true);
        try {
            // The RecordingHiveMetastore loads the metadata files generated through HiveMetadataRecorder
            // which essentially helps to generate the optimal query plans for validation purposes. These files
            // contain all the metadata including statistics.
            RecordingHiveMetastore metastore = new RecordingHiveMetastore(
                    new UnimplementedHiveMetastore(),
                    new HiveMetastoreRecording(recordingConfig, createJsonCodec()));
            return new TestingHiveConnectorFactory(metastore);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected String getSchema()
    {
        String fileName = Paths.get(getRecordingPath()).getFileName().toString();
        return fileName.split("\\.")[0];
    }

    @Override
    @BeforeClass
    public void prepareTables()
    {
        // Nothing to do. Tables are populated using the recording.
    }

    private String getRecordingPath()
    {
        URL resource = getClass().getResource(getMetadataDir());
        if (resource == null) {
            throw new RuntimeException("Hive metadata directory doesn't exist: " + getMetadataDir());
        }

        File[] files = new File(resource.getPath()).listFiles();
        if (files == null) {
            throw new RuntimeException("Hive metadata recording file doesn't exist in directory: " + getMetadataDir());
        }

        return Arrays.stream(files)
                .filter(f -> !f.isDirectory())
                .collect(onlyElement())
                .getPath();
    }

    protected abstract String getMetadataDir();
}
