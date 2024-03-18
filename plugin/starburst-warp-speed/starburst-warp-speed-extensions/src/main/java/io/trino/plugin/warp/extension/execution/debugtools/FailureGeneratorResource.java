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
package io.trino.plugin.warp.extension.execution.debugtools;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.varada.util.ArrayUtils;
import io.trino.plugin.varada.util.FailureGeneratorInvocationHandler;
import io.trino.plugin.varada.util.FailureGeneratorInvocationHandler.FailureAction;
import io.trino.plugin.varada.util.FailureGeneratorInvocationHandler.FailureType;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import io.trino.plugin.warp.gen.constants.FailureRepetitionMode;
import io.varada.annotation.Audit;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.warp.extension.execution.debugtools.FailureGeneratorResource.TASK_NAME;

@TaskResourceMarker
@Singleton
@Path(TASK_NAME)
//@Api(value = "Failure Generator", tags = "Debug", description = "Failure Generator")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class FailureGeneratorResource
        implements TaskResource
{
    public static final String TASK_NAME = "generate-failure";

    private final StorageEngine storageEngine;
    private final FailureGeneratorInvocationHandler failureGeneratorInvocationHandler;

    @Inject
    public FailureGeneratorResource(StorageEngine storageEngine,
            FailureGeneratorInvocationHandler failureGeneratorInvocationHandler)
    {
        this.storageEngine = storageEngine;
        this.failureGeneratorInvocationHandler = failureGeneratorInvocationHandler;
    }

    @POST
    @Audit
    //@ApiOperation(value = "generate-failure", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public void generateFailure(List<FailureGeneratorData> failureGeneratorDataList)
    {
        Map<String, FailureAction> newResults = new HashMap<>();
        List<Integer> panicIds = new ArrayList<>();
        List<Integer> repModes = new ArrayList<>();
        List<Integer> ratios = new ArrayList<>();
        int numNativeElements = 0;

        for (FailureGeneratorData failureGeneratorData : failureGeneratorDataList) {
            if (!FailureType.NATIVE_PANIC.equals(failureGeneratorData.getFailureType())) {
                validateMethodExist(failureGeneratorData.getClassName(), failureGeneratorData.getMethodName());
                FailureAction failureAction = new FailureAction(failureGeneratorData.getFailureType(), failureGeneratorData.getRepetitionMode(), failureGeneratorData.getRepetitionCount());
                String key = FailureGeneratorInvocationHandler.getKey(failureGeneratorData.getClassName(), failureGeneratorData.getMethodName());
                newResults.put(key, failureAction);
            }
            else {
                panicIds.add(Integer.parseInt(failureGeneratorData.getMethodName()));
                repModes.add(failureGeneratorData.getRepetitionMode().ordinal());
                ratios.add(failureGeneratorData.getRepetitionCount());
                numNativeElements++;
            }
        }

        storageEngine.setDebugThrowPolicy(numNativeElements, ArrayUtils.convertToIntArray(panicIds), ArrayUtils.convertToIntArray(repModes), ArrayUtils.convertToIntArray(ratios));
        failureGeneratorInvocationHandler.updateInvocationResult(newResults);
    }

    private void validateMethodExist(String className, String methodName)
    {
        Class<?> aClass;
        try {
            aClass = Class.forName(className);
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException(String.format("class doesn't exist - %s", className));
        }
        Optional<Method> m = Arrays.stream(aClass.getMethods()).filter(method -> method.getName().equals(methodName)).findFirst();
        if (m.isEmpty()) {
            throw new RuntimeException(String.format("class + method doesn't exist - %s::%s", className, methodName));
        }
    }

    public static class FailureGeneratorData
    {
        private final String className;
        private final String methodName;
        private final FailureType failureType;
        private final int repetitionCount;
        private final FailureRepetitionMode failureRepetitionMode;

        @JsonCreator
        public FailureGeneratorData(@JsonProperty("className") String className,
                @JsonProperty("methodName") String methodName,
                @JsonProperty("repetitionMode") FailureRepetitionMode failureRepetitionMode,
                @JsonProperty("failureType") FailureType failureType,
                @JsonProperty("repetitionCount") int repetitionCount)
        {
            this.className = className;
            this.methodName = methodName;
            this.failureRepetitionMode = failureRepetitionMode;
            this.failureType = failureType;
            this.repetitionCount = repetitionCount;
        }

        @JsonProperty("className")
        public String getClassName()
        {
            return className;
        }

        @JsonProperty("methodName")
        public String getMethodName()
        {
            return methodName;
        }

        @JsonProperty("failureType")
        public FailureType getFailureType()
        {
            return failureType;
        }

        @JsonProperty("repetitionCount")
        public int getRepetitionCount()
        {
            return repetitionCount;
        }

        @JsonProperty("repetitionMode")
        public FailureRepetitionMode getRepetitionMode()
        {
            return failureRepetitionMode;
        }
    }
}
