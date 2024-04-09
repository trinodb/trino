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
package io.trino.plugin.hive.fs;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.parallel.Execution;

import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

// some tests may invalidate the whole cache affecting therefore other concurrent tests
@Execution(SAME_THREAD)
public class TestCachingDirectoryLister
        extends BaseCachingDirectoryListerTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createQueryRunner(ImmutableMap.<String, String>builder()
                .put("hive.allow-register-partition-procedure", "true")
                .put("hive.recursive-directories", "true")
                .put("hive.file-status-cache-expire-time", "5m")
                .put("hive.file-status-cache.max-retained-size", "1MB")
                .put("hive.file-status-cache-tables", "tpch.*")
                .buildOrThrow());
    }
}
