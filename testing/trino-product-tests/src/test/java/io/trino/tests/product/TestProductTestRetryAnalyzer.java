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
package io.trino.tests.product;

import org.testng.ITestNGMethod;
import org.testng.ITestResult;
import org.testng.annotations.Test;
import org.testng.internal.NoOpTestClass;
import org.testng.internal.TestResult;

import java.lang.reflect.Proxy;

import static io.trino.testng.services.FlakyTestRetryAnalyzer.ALLOWED_RETRIES_COUNT;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestProductTestRetryAnalyzer
{
    @Test
    public void testRetryOnHadoopReplicationFailure()
    {
        ProductTestRetryAnalyzer analyzer = new ProductTestRetryAnalyzer();
        ITestResult result = failedResult("testMethod", new RuntimeException(
                "could only be replicated to 0 nodes instead of minReplication"));

        for (int i = 0; i < ALLOWED_RETRIES_COUNT; i++) {
            assertThat(analyzer.retry(result))
                    .describedAs("retry attempt %d", i + 1)
                    .isTrue();
        }
        assertThat(analyzer.retry(result))
                .describedAs("should not retry beyond allowed count")
                .isFalse();
    }

    @Test
    public void testRetryOnHadoopWriteFailure()
    {
        ProductTestRetryAnalyzer analyzer = new ProductTestRetryAnalyzer();
        ITestResult result = failedResult("testMethod", new RuntimeException(
                "could only be written to 0 of the 1 minReplication"));

        assertThat(analyzer.retry(result)).isTrue();
    }

    @Test
    public void testRetryOnHadoopMapReduceFailure()
    {
        ProductTestRetryAnalyzer analyzer = new ProductTestRetryAnalyzer();
        ITestResult result = failedResult("testMethod", new RuntimeException(
                "return code 1 from org.apache.hadoop.hive.ql.exec.mr.MapRedTask"));

        assertThat(analyzer.retry(result)).isTrue();
    }

    @Test
    public void testNoRetryOnUnknownFailure()
    {
        ProductTestRetryAnalyzer analyzer = new ProductTestRetryAnalyzer();
        ITestResult result = failedResult("testMethod", new RuntimeException(
                "some unknown failure"));

        assertThat(analyzer.retry(result)).isFalse();
    }

    @Test
    public void testNoRetryOnSuccess()
    {
        ProductTestRetryAnalyzer analyzer = new ProductTestRetryAnalyzer();
        ITestResult result = successResult("testMethod");

        assertThat(analyzer.retry(result)).isFalse();
    }

    @Test
    public void testNoRetryWhenDisabled()
    {
        System.setProperty("io.trino.tests.product.ProductTestRetryAnalyzer.disabled", "true");
        try {
            ProductTestRetryAnalyzer analyzer = new ProductTestRetryAnalyzer();
            ITestResult result = failedResult("testMethod", new RuntimeException(
                    "could only be replicated to 0 nodes instead of minReplication"));

            assertThat(analyzer.retry(result)).isFalse();
        }
        finally {
            System.clearProperty("io.trino.tests.product.ProductTestRetryAnalyzer.disabled");
        }
    }

    private static ITestResult failedResult(String methodName, Throwable throwable)
    {
        return testResult(methodName, ITestResult.FAILURE, throwable);
    }

    private static ITestResult successResult(String methodName)
    {
        return testResult(methodName, ITestResult.SUCCESS, null);
    }

    private static ITestResult testResult(String methodName, int status, Throwable throwable)
    {
        NoOpTestClass testClass = new StubTestClass();
        ITestNGMethod method = stubMethod(methodName, testClass);

        TestResult result = new TestResult();
        result.setStatus(status);
        result.setThrowable(throwable);
        result.setMethod(method);
        result.setTestClass(testClass);
        return result;
    }

    private static ITestNGMethod stubMethod(String methodName, NoOpTestClass testClass)
    {
        return (ITestNGMethod) Proxy.newProxyInstance(
                ITestNGMethod.class.getClassLoader(),
                new Class<?>[] {ITestNGMethod.class},
                (proxy, method, args) ->
                        switch (method.getName()) {
                            case "getMethodName" -> methodName;
                            case "getTestClass" -> testClass;
                            default -> throw new UnsupportedOperationException(method.getName());
                        });
    }

    private static class StubTestClass
            extends NoOpTestClass
    {
        StubTestClass()
        {
            super();
            setTestClass(TestProductTestRetryAnalyzer.class);
        }
    }
}
