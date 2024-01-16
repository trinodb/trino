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
package io.trino.plugin.hive.functions;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.gcs.GcsFileSystemConfig;
import io.trino.filesystem.gcs.GcsFileSystemFactory;
import io.trino.filesystem.gcs.GcsStorageFactory;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.testing.QueryRunner;

import java.io.IOException;
import java.util.Base64;

import static java.nio.charset.StandardCharsets.UTF_8;

public class TestUnloadGcs
        extends BaseUnloadFileSystemTest
{
    private final String gcpStorageBucket;
    private final String gcsJsonKey;

    public TestUnloadGcs()
    {
        gcpStorageBucket = requireEnv("GCP_STORAGE_BUCKET");
        gcsJsonKey = new String(Base64.getDecoder().decode(requireEnv("GCP_CREDENTIALS_KEY")), UTF_8);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setHiveProperties(ImmutableMap.<String, String>builder()
                        .put("fs.native-gcs.enabled", "true")
                        .put("gcs.json-key", gcsJsonKey)
                        .put("hive.security", "allow-all")
                        .buildOrThrow())
                .build();
    }

    @Override
    protected TrinoFileSystemFactory getFileSystemFactory()
            throws IOException
    {
        GcsFileSystemConfig config = new GcsFileSystemConfig().setJsonKey(gcsJsonKey);
        return new GcsFileSystemFactory(config, new GcsStorageFactory(config));
    }

    @Override
    protected String getLocation(String path)
    {
        return "gs://%s/%s".formatted(gcpStorageBucket, path);
    }
}
