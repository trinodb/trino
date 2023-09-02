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
package io.trino.filesystem.s3;

import io.trino.filesystem.s3.S3FileSystemTestingEnvironment.S3FileSystemTestingEnvironmentAwsS3;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class TestS3FileSystemAwsS3
        extends AbstractTestS3FileSystem
{
    private S3FileSystemTestingEnvironmentAwsS3 testingEnvironment;

    @BeforeAll
    public void setup()
    {
        testingEnvironment = new S3FileSystemTestingEnvironmentAwsS3();
    }

    @AfterAll
    public void teardown()
    {
        if (testingEnvironment != null) {
            testingEnvironment.close();
            testingEnvironment = null;
        }
    }

    @Override
    public S3FileSystemTestingEnvironment s3TestingEnvironment()
    {
        return testingEnvironment;
    }
}
