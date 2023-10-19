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
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Optional;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.hive.metastore.recording.TestRecordingHiveMetastore.createJsonCodec;
import static java.util.Objects.requireNonNull;

public abstract class BaseHiveCostBasedPlanTest
        extends BaseCostBasedPlanTest
{
    private final String metadataDir;

    protected BaseHiveCostBasedPlanTest(String metadataDir, boolean partitioned)
    {
        super(
                getSchema(metadataDir),
                // In case of Hive connector, query plans do not currently depend on file format
                Optional.empty(),
                partitioned);
        this.metadataDir = requireNonNull(metadataDir, "metadataDir is null");
    }

    @Override
    protected ConnectorFactory createConnectorFactory()
    {
        RecordingMetastoreConfig recordingConfig = new RecordingMetastoreConfig()
                .setRecordingPath(getRecordingPath(metadataDir))
                .setReplay(true);
        // The RecordingHiveMetastore loads the metadata files generated through HiveMetadataRecorder
        // which essentially helps to generate the optimal query plans for validation purposes. These files
        // contain all the metadata including statistics.
        RecordingHiveMetastore metastore = new RecordingHiveMetastore(
                new UnimplementedHiveMetastore(),
                new HiveMetastoreRecording(recordingConfig, createJsonCodec()));
        return new TestingHiveConnectorFactory(metastore);
    }

    private static String getSchema(String metadataDir)
    {
        String fileName = Paths.get(getRecordingPath(metadataDir)).getFileName().toString();
        return fileName.split("\\.")[0];
    }

    @Override
    @BeforeClass
    public void prepareTables()
    {
        // Nothing to do. Tables are populated using the recording.
    }

    private static String getRecordingPath(String metadataDir)
    {
        URL resource = BaseHiveCostBasedPlanTest.class.getResource(metadataDir);
        if (resource == null) {
            throw new RuntimeException("Hive metadata directory doesn't exist: " + metadataDir);
        }

        File[] files = new File(resource.getPath()).listFiles();
        if (files == null) {
            throw new RuntimeException("Hive metadata recording file doesn't exist in directory: " + metadataDir);
        }

        return Arrays.stream(files)
                .filter(f -> !f.isDirectory())
                .collect(onlyElement())
                .getPath();
    }
}
