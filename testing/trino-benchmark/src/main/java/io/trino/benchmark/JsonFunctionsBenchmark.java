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
package io.trino.benchmark;

import io.trino.testing.LocalQueryRunner;

import static io.trino.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;

public abstract class JsonFunctionsBenchmark
{
    public static void main(String... args)
    {
        LocalQueryRunner localQueryRunner = createLocalQueryRunner();
        new JsonExtractBenchmark(localQueryRunner).runBenchmark(new JsonAvgBenchmarkResultWriter(System.out));
        new JsonQueryBenchmark(localQueryRunner).runBenchmark(new JsonAvgBenchmarkResultWriter(System.out));
        new JsonExtractScalarBenchmark(localQueryRunner).runBenchmark(new JsonAvgBenchmarkResultWriter(System.out));
        new JsonValueBenchmark(localQueryRunner).runBenchmark(new JsonAvgBenchmarkResultWriter(System.out));
    }

    public static class JsonExtractBenchmark
            extends AbstractSqlBenchmark
    {
        public JsonExtractBenchmark(LocalQueryRunner localQueryRunner)
        {
            super(localQueryRunner, "json_extract", 5, 50, "SELECT json_extract(format('{name : %s, random_number : %s}', partkey, random()), '$.name') FROM lineitem");
        }
    }

    public static class JsonQueryBenchmark
            extends AbstractSqlBenchmark
    {
        public JsonQueryBenchmark(LocalQueryRunner localQueryRunner)
        {
            super(localQueryRunner, "json_query", 5, 50, "SELECT json_query(format('{name : %s, random_number : %s}', partkey, random()), 'strict $.name') FROM lineitem");
        }
    }

    public static class JsonExtractScalarBenchmark
            extends AbstractSqlBenchmark
    {
        public JsonExtractScalarBenchmark(LocalQueryRunner localQueryRunner)
        {
            super(localQueryRunner, "json_extract_scalar", 5, 50, "SELECT json_extract_scalar(format('{comment : %s, random_number : %s}', comment, random()), '$.comment') FROM lineitem");
        }
    }

    public static class JsonValueBenchmark
            extends AbstractSqlBenchmark
    {
        public JsonValueBenchmark(LocalQueryRunner localQueryRunner)
        {
            super(localQueryRunner, "json_value", 5, 50, "SELECT json_value(format('{comment : %s, random_number : %s}', comment, random()), 'strict $.comment') FROM lineitem");
        }
    }
}
