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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.VaradaErrorCode;
import io.trino.plugin.warp.gen.constants.FailureRepetitionMode;
import io.trino.spi.TrinoException;

import java.lang.reflect.InvocationHandler;
import java.util.HashMap;
import java.util.Map;

@Singleton
public class FailureGeneratorInvocationHandler
{
    private static final Logger logger = Logger.get(FailureGeneratorInvocationHandler.class);

    private Map<String, FailureAction> invocationResults = new HashMap<>();

    @Inject
    public FailureGeneratorInvocationHandler() {}

    public static String getKey(String className, String methodName)
    {
        return className + "::" + methodName;
    }

    public InvocationHandler getMethodInvocationHandler(Object inst)
    {
        return (proxy, method, args) -> {
            String key = getKey(method.getDeclaringClass().getName(), method.getName());
            FailureAction failureAction = invocationResults.get(key);
            if (failureAction != null && failureAction.shouldExecute()) {
                logger.debug("invoking proxy of %s - %s", key, failureAction);
                switch (failureAction.getFailureType()) {
                    case RETURN_NULL -> { /* do nothing */ }
                    case JAVA_EXCEPTION -> throw new TrinoException(VaradaErrorCode.VARADA_GENERIC, "this is a fake java error");
                    case NATIVE_EXCEPTION -> throw new TrinoException(VaradaErrorCode.VARADA_NATIVE_ERROR, "this is a fake native error");
                    case NATIVE_UNRECOVERABLE_EXCEPTION -> throw new TrinoException(VaradaErrorCode.VARADA_NATIVE_UNRECOVERABLE_ERROR, "this is a fake unrecoverable native error");
                    case NATIVE_PANIC -> throw new TrinoException(VaradaErrorCode.VARADA_ILLEGAL_PARAMETER, "NATIVE_PANIC should not get here");
                }
                return null;
            }
            return method.invoke(inst, args);
        };
    }

    public void updateInvocationResult(Map<String, FailureAction> invocationResults)
    {
        this.invocationResults = invocationResults;
    }

    public static final class FailureAction
    {
        private final FailureType failureType;
        private final FailureRepetitionMode failureRepetitionMode;
        private final int repetitionCount;
        private int nextExecution = -1;
        private int executionCount;

        public FailureAction(FailureType failureType, FailureRepetitionMode failureRepetitionMode, int repetitionCount)
        {
            this.failureType = failureType;
            this.failureRepetitionMode = failureRepetitionMode;
            this.repetitionCount = repetitionCount;
            calculateNextExecution();
        }

        public FailureType getFailureType()
        {
            return failureType;
        }

        public boolean shouldExecute()
        {
            boolean ret = false;
            executionCount++;
            if (executionCount == nextExecution) {
                ret = true;
                calculateNextExecution();
            }
            return ret;
        }

        private void calculateNextExecution()
        {
            switch (failureRepetitionMode) {
                case REP_MODE_ALWAYS:
                    nextExecution = repetitionCount;
                    executionCount = 0;
                    break;
                case REP_MODE_ONCE:
                    nextExecution = 1;
                    break;
                case REP_MODE_RANDOM:
                    nextExecution = (int) (Math.random() * repetitionCount);
                    executionCount = 0;
                    break;
            }
        }

        @Override
        public String toString()
        {
            return "FailureAction{" +
                    "failureType=" + failureType +
                    ", repetitionMode=" + failureRepetitionMode +
                    ", repetitionCount=" + repetitionCount +
                    ", nextExecution=" + nextExecution +
                    ", executionCount=" + executionCount +
                    '}';
        }
    }

    public enum FailureType
    {
        JAVA_EXCEPTION,
        NATIVE_EXCEPTION,
        NATIVE_UNRECOVERABLE_EXCEPTION,
        NATIVE_PANIC,
        RETURN_NULL
    }
}
