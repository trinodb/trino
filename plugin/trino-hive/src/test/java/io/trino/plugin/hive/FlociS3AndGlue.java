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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.containers.Floci;
import software.amazon.awssdk.services.glue.GlueClient;

import java.util.Map;

public final class FlociS3AndGlue
        implements AutoCloseable
{
    private final Floci floci = new Floci();

    public FlociS3AndGlue()
    {
        floci.start();
    }

    public void createBucket(String bucketName)
    {
        floci.createBucket(bucketName);
    }

    public GlueClient createGlueClient()
    {
        return GlueClient.builder()
                .applyMutation(floci::updateClient)
                .build();
    }

    public Map<String, String> glueProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("hive.metastore.glue.endpoint-url", floci.endpoint().toString())
                .put("hive.metastore.glue.region", Floci.FLOCI_REGION)
                .put("hive.metastore.glue.aws-access-key", Floci.FLOCI_ACCESS_KEY)
                .put("hive.metastore.glue.aws-secret-key", Floci.FLOCI_SECRET_KEY)
                .buildOrThrow();
    }

    public Map<String, String> s3AndGlueProperties()
    {
        return ImmutableMap.<String, String>builder()
                .putAll(glueProperties())
                .put("s3.region", Floci.FLOCI_REGION)
                .put("s3.endpoint", floci.endpoint().toString())
                .put("s3.aws-access-key", Floci.FLOCI_ACCESS_KEY)
                .put("s3.aws-secret-key", Floci.FLOCI_SECRET_KEY)
                .put("s3.path-style-access", "true")
                .buildOrThrow();
    }

    @Override
    public void close()
    {
        floci.close();
    }
}
