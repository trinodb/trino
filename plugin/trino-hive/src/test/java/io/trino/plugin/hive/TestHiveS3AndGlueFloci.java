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

import software.amazon.awssdk.services.s3.S3Client;

import java.util.Map;

import static io.trino.testing.TestingNames.randomNameSuffix;

public class TestHiveS3AndGlueFloci
        extends BaseHiveS3AndGlueTest
{
    private FlociS3AndGlue floci;

    public TestHiveS3AndGlueFloci()
    {
        super("test-hive-s3-glue-" + randomNameSuffix());
    }

    @Override
    protected Map<String, String> s3AndGlueProperties()
    {
        return floci().s3AndGlueProperties();
    }

    @Override
    protected S3Client createS3Client()
    {
        return floci().createS3Client();
    }

    private FlociS3AndGlue floci()
    {
        if (floci == null) {
            floci = closeAfterClass(new FlociS3AndGlue());
            floci.createBucket(bucketName);
        }
        return floci;
    }
}
