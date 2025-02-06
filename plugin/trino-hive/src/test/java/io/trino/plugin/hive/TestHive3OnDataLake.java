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

import io.trino.plugin.hive.containers.Hive3MinioDataLake;
import io.trino.plugin.hive.containers.HiveHadoop;
import org.junit.jupiter.api.TestInstance;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
class TestHive3OnDataLake
        extends BaseTestHiveOnDataLake
{
    private static final String BUCKET_NAME = "test-hive-insert-overwrite-" + randomNameSuffix();

    public TestHive3OnDataLake()
    {
        super(BUCKET_NAME, new Hive3MinioDataLake(BUCKET_NAME, HiveHadoop.HIVE3_IMAGE));
    }
}
