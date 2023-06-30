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

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

import static io.airlift.concurrent.MoreFutures.getDone;

public final class UpdateExpectedPlans
{
    private UpdateExpectedPlans() {}

    public static void main(String[] args)
            throws Exception
    {
        String[] noArgs = new String[0];

        List<Future<Void>> futures = ForkJoinPool.commonPool().invokeAll(
                ImmutableList.<Callable<Void>>builder()
                        // in alphabetical order
                        .add(runMain(TestHivePartitionedTpcdsCostBasedPlan.class, noArgs))
                        .add(runMain(TestHivePartitionedTpchCostBasedPlan.class, noArgs))
                        .add(runMain(TestHiveTpcdsCostBasedPlan.class, noArgs))
                        .add(runMain(TestHiveTpchCostBasedPlan.class, noArgs))
                        .add(runMain(TestIcebergOrcPartitionedTpcdsCostBasedPlan.class, noArgs))
                        .add(runMain(TestIcebergOrcPartitionedTpchCostBasedPlan.class, noArgs))
                        .add(runMain(TestIcebergOrcTpcdsCostBasedPlan.class, noArgs))
                        .add(runMain(TestIcebergOrcTpchCostBasedPlan.class, noArgs))
                        .add(runMain(TestIcebergParquetPartitionedTpcdsCostBasedPlan.class, noArgs))
                        .add(runMain(TestIcebergParquetPartitionedTpchCostBasedPlan.class, noArgs))
                        .add(runMain(TestIcebergParquetTpcdsCostBasedPlan.class, noArgs))
                        .add(runMain(TestIcebergParquetTpchCostBasedPlan.class, noArgs))
                        .add(runMain(TestIcebergSmallFilesParquetTpcdsCostBasedPlan.class, noArgs))
                        .build());

        for (Future<Void> future : futures) {
            getDone(future);
        }
    }

    private static Callable<Void> runMain(Class<?> clazz, String[] args)
            throws NoSuchMethodException
    {
        Method main = clazz.getMethod("main", String[].class);
        return () -> {
            main.invoke(null, new Object[] {args});
            return null;
        };
    }
}
