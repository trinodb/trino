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
package io.trino.testng.services;

import io.airlift.log.Logger;
import org.testng.IAnnotationTransformer;
import org.testng.annotations.ITestAnnotation;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

public class RetryAnnotationTransformer
        implements IAnnotationTransformer
{
    private static final Logger log = Logger.get(RetryAnnotationTransformer.class);

    @Override
    public void transform(ITestAnnotation annotation, Class testClass, Constructor testConstructor, Method testMethod)
    {
        if (testMethod != null) {
            log.debug("Instrumenting method %s with %s.", testMethod.getName(), FlakyTestRetryAnalyzer.class.getSimpleName());
            annotation.setRetryAnalyzer(FlakyTestRetryAnalyzer.class);
        }
    }
}
