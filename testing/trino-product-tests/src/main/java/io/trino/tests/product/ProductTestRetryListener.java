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

import io.airlift.log.Logger;
import io.trino.tempto.internal.convention.ConventionBasedTestProxyGenerator;
import org.testng.IClassListener;
import org.testng.ITestClass;
import org.testng.ITestNGMethod;

public class ProductTestRetryListener
        implements IClassListener
{
    private static final Logger log = Logger.get(ProductTestRetryListener.class);

    @Override
    public void onBeforeClass(ITestClass testClass)
    {
        Class<?> realClass = testClass.getRealClass();
        if (isConventionBasedTest(realClass)) {
            for (ITestNGMethod method : testClass.getTestMethods()) {
                log.debug("Instrumenting method %s with %s.", method.getMethodName(), ProductTestRetryAnalyzer.class.getSimpleName());
                method.setRetryAnalyzer(new ProductTestRetryAnalyzer());
            }
        }
    }

    @Override
    public void onAfterClass(ITestClass testClass) {}

    private static boolean isConventionBasedTest(Class<?> realClass)
    {
        return ConventionBasedTestProxyGenerator.ConventionBasedTestProxy.class.isAssignableFrom(realClass);
    }
}
