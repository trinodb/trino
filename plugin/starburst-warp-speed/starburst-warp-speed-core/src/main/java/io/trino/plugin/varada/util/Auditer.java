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
package io.trino.plugin.varada.util;

import io.airlift.log.Logger;
import io.varada.tools.util.StopWatch;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

public class Auditer
        implements MethodInterceptor
{
    private static final Logger logger = Logger.get("AUDIT");

    @Override
    public Object invoke(MethodInvocation invocation)
            throws Throwable
    {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        try {
            return invocation.proceed();
        }
        finally {
            stopWatch.stop();
            logger.debug("%s::%s took %dms", invocation.getMethod().getDeclaringClass().getSimpleName(), invocation.getMethod().getName(), stopWatch.getTime());
        }
    }
}
