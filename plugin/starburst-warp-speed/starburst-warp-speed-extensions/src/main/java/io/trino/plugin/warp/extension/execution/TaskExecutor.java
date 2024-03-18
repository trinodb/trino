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
package io.trino.plugin.warp.extension.execution;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.trino.plugin.varada.dispatcher.connectors.ConnectorTaskExecutor;
import io.trino.plugin.varada.execution.TaskData;
import io.trino.spi.TrinoException;
import io.varada.annotation.Audit;
import io.varada.tools.util.StringUtils;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HttpMethod;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static java.lang.String.format;

@SuppressWarnings({"unchecked", "rawtypes", "UnnecessaryLocalVariable"})
@Singleton
public class TaskExecutor
        implements ConnectorTaskExecutor
{
    public static final String NAMED_TASK_EXEC_ENABLE_SUPPLIER = "TaskExecutionEnabledSupplier";
    public static final String TOO_MANY_TASK_DATA_PARAMS = "Task infra supports only a zero/single TaskData input parameter, while '%s' has %d input parameters";
    private static final Logger logger = Logger.get(TaskExecutor.class);
    private final Set<BooleanSupplier> isEnabledSuppliers;
    private final Map<String, TaskInvocationMethod> executionMap;
    private final ObjectMapper objectMapper;

    @Inject
    public TaskExecutor(Set<TaskResource> set,
            @Named(NAMED_TASK_EXEC_ENABLE_SUPPLIER) Set<BooleanSupplier> isEnabledSuppliers)
    {
        this.isEnabledSuppliers = isEnabledSuppliers;
        objectMapper = new ObjectMapperProvider().get();
        executionMap = new HashMap<>();
        buildExecutionMapping(set);
    }

    private void buildExecutionMapping(Set<TaskResource> set)
    {
        set.forEach(this::buildResourceExecutionMap);
    }

    private void buildResourceExecutionMap(TaskResource taskResource)
    {
        Class<?> taskResourceClass = getResourceDeclaringClass(taskResource.getClass());
        TaskResourceMarker taskResourceMarker = taskResourceClass.getAnnotation(TaskResourceMarker.class);
        Path classPath = taskResourceClass.getAnnotation(Path.class);
        for (Method method : taskResourceClass.getDeclaredMethods()) {
            boolean shouldLog = method.getAnnotation(Audit.class) != null;
            String httpMethod = getMethod(method);
            if (httpMethod != null) {
                String taskName = getTaskName(httpMethod, method, classPath);
                if (executionMap.containsKey(taskName)) {
                    throw new RuntimeException(format("Duplicate task '%s' trying to register", taskName));
                }

                ImmutableList.Builder<TaskInvocationParameter> taskInvocationParameterBuilder = ImmutableList.builder();
                for (int i = 0; i < method.getParameterCount(); i++) {
                    String parameterName = getParamName(method, i);
                    Class parameterClass = method.getParameterTypes()[i];

                    Class parameterTypeClass = null;
                    Type[] types = method.getGenericParameterTypes();
                    if (i < types.length) {
                        Type type = types[i];
                        if (type instanceof ParameterizedType) {
                            if (!type.getTypeName().startsWith(List.class.getName())) { // only List is supported as generic
                                throw new IllegalArgumentException(format("Unsupported class %s, only List is allowed", type.getTypeName()));
                            }
                            try {
                                parameterTypeClass = Class.forName(((ParameterizedType) types[i]).getActualTypeArguments()[0].getTypeName());
                            }
                            catch (ClassNotFoundException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        else if (type instanceof Class) {
                            parameterTypeClass = (Class) type;
                        }
                    }

                    if (isTaskDataClass(parameterClass) && method.getParameterCount() > 1) {
                        throw new RuntimeException(String.format(TOO_MANY_TASK_DATA_PARAMS, taskName, method.getParameterCount()));
                    }

                    taskInvocationParameterBuilder.add(new TaskInvocationParameter(parameterName, parameterClass, parameterTypeClass));
                }

                TaskInvocationMethod taskInvocationMethod = new TaskInvocationMethod(method,
                        taskResource,
                        taskInvocationParameterBuilder.build(),
                        shouldLog,
                        taskResourceMarker.shouldCheckExecutionAllowed());
                logger.debug("exposing %s method %s", taskName, taskInvocationMethod);
                executionMap.put(taskName, taskInvocationMethod);
            }
        }
    }

    private String getMethod(Method method)
    {
        String ret = null;
        if (method.getAnnotation(PUT.class) != null) {
            ret = HttpMethod.PUT;
        }
        else if (method.getAnnotation(GET.class) != null) {
            ret = HttpMethod.GET;
        }
        else if (method.getAnnotation(POST.class) != null) {
            ret = HttpMethod.POST;
        }
        else if (method.getAnnotation(DELETE.class) != null) {
            ret = HttpMethod.DELETE;
        }
        return ret;
    }

    private String getTaskName(String httpMethod, Method method, Path classPath)
    {
        StringBuilder ret = new StringBuilder();
        ret.append(httpMethod);
        Path methodPath = method.getAnnotation(Path.class);
        ret.append("_");
        if (classPath != null && StringUtils.isNotEmpty(classPath.value()) && methodPath != null) {
            ret.append(classPath.value()).append("/").append(methodPath.value());
        }
        else if (classPath != null && StringUtils.isNotEmpty(classPath.value())) {
            ret.append(classPath.value());
        }
        else if (methodPath != null) {
            ret.append(methodPath.value());
        }
        else {
            throw new IllegalArgumentException("task must have either Path definition on class or method");
        }
        return ret.toString();
    }

    private Class<?> getResourceDeclaringClass(Class<? extends TaskResource> aClass)
    {
        try {
            if (aClass.getName().contains("EnhancerByGuice")) {
                return Class.forName(aClass.getName().substring(0, aClass.getName().indexOf("EnhancerByGuice") - 2));
            }
        }
        catch (Throwable t) {
            logger.error("failed to get task info for class %s", aClass);
        }
        return aClass;
    }

    private String getParamName(Method method, int i)
    {
        String parameterName = null;
        if (method.getParameterAnnotations().length > 0 && method.getParameterAnnotations()[i].length > 0) {
            Annotation parameterAnnotation = method.getParameterAnnotations()[i][0];
            if (parameterAnnotation instanceof JsonProperty) {
                parameterName = ((JsonProperty) parameterAnnotation).value();
            }
        }
        if (StringUtils.isEmpty(parameterName)) {
            parameterName = method.getParameters()[i].getName();
        }
        return parameterName;
    }

    @Override
    public Object executeTask(String taskName, String dataStr, String httpMethod)
    {
        TaskInvocationMethod taskInvocationMethod = executionMap.get(taskName);
        try {
            if (taskInvocationMethod == null) {
                taskInvocationMethod = executionMap.get(httpMethod + "_" + taskName);
                if (taskInvocationMethod == null) {
                    throw new RuntimeException(format("Task '%s' doesn't have definitions",
                            taskName));
                }
            }

            if (isEnabledSuppliers.stream().noneMatch(BooleanSupplier::getAsBoolean) &&
                    taskInvocationMethod.shouldCheckExecutionAllowed) {
                throw new RuntimeException(format("Execution of task '%s' is disabled for now",
                        taskName));
            }

            if (taskInvocationMethod.isLoggable) {
                logger.debug("executing task %s", taskName);
            }

            if (StringUtils.isNotEmpty(dataStr) && !taskInvocationMethod.taskInvocationParameters.isEmpty()) {
                Class parameterClass = taskInvocationMethod.taskInvocationParameters.stream().findFirst().orElseThrow().clazz;
                // old style TaskData parameter, we expect a single param in this case
                if (isTaskDataClass(parameterClass)) {
                    return invokeTaskData(taskName, dataStr, taskInvocationMethod, parameterClass);
                }
                return invokeJsonSupport(dataStr, taskInvocationMethod);
            }
            else if (!taskInvocationMethod.taskInvocationParameters.isEmpty()) { // invoke method with empty parameters
                return taskInvocationMethod.invocationMethod.invoke(taskInvocationMethod.taskResource, new Object[] {null});
            }
            else { // invoke method without parameters
                return taskInvocationMethod.invocationMethod.invoke(taskInvocationMethod.taskResource);
            }
        }
        catch (InvocationTargetException ite) {
            if (ite.getTargetException() != null && ite.getTargetException() instanceof TrinoException) {
                throw (TrinoException) ite.getTargetException();
            }
            throw new RuntimeException(String.format("Task '%s' execution failed", taskName), ite);
        }
        catch (IllegalAccessException | IllegalArgumentException | IOException e) {
            throw new RuntimeException(format("Task '%s' execution failed with params %s", taskName, dataStr), e);
        }
    }

    private boolean isTaskDataClass(Class parameterClass)
    {
        return Objects.nonNull(parameterClass.getSuperclass()) && parameterClass.getSuperclass().equals(TaskData.class);
    }

    private Object invokeJsonSupport(String dataStr, TaskInvocationMethod taskInvocationMethod)
            throws JsonProcessingException, IllegalAccessException, InvocationTargetException
    {
        List<Object> parameterValues;
        if (dataStr.startsWith("{")) { // support for "{ "param1": "message1", "param2": "message2"}"
            parameterValues = getParamValuesForMap(dataStr, taskInvocationMethod);
        }
        else if (dataStr.startsWith("[")) { // support for "["message1", "message2"]"
            parameterValues = getParamValuesForList(dataStr, taskInvocationMethod);
        }
        else {
            throw new IllegalArgumentException("unsupported json");
        }

        return taskInvocationMethod.invocationMethod.invoke(taskInvocationMethod.taskResource, parameterValues.toArray());
    }

    private Object invokeTaskData(String taskName, String dataStr, TaskInvocationMethod taskInvocationMethod, Class parameterClass)
            throws JsonProcessingException, IllegalAccessException, InvocationTargetException
    {
        Map<String, Object> dataMap = objectMapper.readerFor(new TypeReference<Map<String, Object>>() {}).readValue(dataStr);
        Class<TaskData> taskDataClass = parameterClass;
        dataMap.put("@class", taskDataClass);
        TaskData data = objectMapper.readValue(objectMapper.writeValueAsString(dataMap), taskDataClass);

        logger.debug("executing task %s data %s", taskName, data);
        return taskInvocationMethod.invocationMethod.invoke(taskInvocationMethod.taskResource, data);
    }

    private List<Object> getParamValuesForList(String dataStr, TaskInvocationMethod taskInvocationMethod)
            throws JsonProcessingException
    {
        TaskInvocationParameter taskInvocationParameter = taskInvocationMethod.taskInvocationParameters.stream().findFirst().orElseThrow();
        Class typeClass = taskInvocationParameter.typedClass;
        List<Object> dataList = objectMapper.readerFor(new TypeReference<List<Object>>() {}).readValue(dataStr);

        if (Objects.isNull(typeClass)) {
            return List.of(dataList);
        }

        return List.of(dataList.stream().map(data -> {
            try {
                return objectMapper.readerFor(typeClass).readValue(objectMapper.writeValueAsString(data));
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList()));
    }

    private List<Object> getParamValuesForMap(String dataStr, TaskInvocationMethod taskInvocationMethod)
            throws JsonProcessingException
    {
        if (taskInvocationMethod.taskInvocationParameters.size() == 1 &&
                taskInvocationMethod.taskInvocationParameters.get(0).typedClass != null) {
            ObjectReader paramReader = objectMapper.readerFor(taskInvocationMethod.taskInvocationParameters.get(0).typedClass);
            return List.of(paramReader.readValue(dataStr));
        }
        Map<String, Object> dataMap = objectMapper.readerFor(new TypeReference<Map<String, Object>>() {}).readValue(dataStr);
        return dataMap.entrySet()
                .stream()
                .map(entry -> {
                    Optional<TaskInvocationParameter> optionalTaskInvocationParameter = taskInvocationMethod.taskInvocationParameters.stream()
                            .filter(taskInvocationParameter -> taskInvocationParameter.name.equals(entry.getKey()))
                            .findFirst();
                    if (optionalTaskInvocationParameter.isPresent()) {
                        try {
                            return objectMapper.readValue(objectMapper.writeValueAsString(entry.getValue()), optionalTaskInvocationParameter.orElseThrow().clazz);
                        }
                        catch (JsonProcessingException e) {
                            logger.error("failed with taskInvocationMethod=%s on entry=%s class=%s",
                                    taskInvocationMethod, entry, optionalTaskInvocationParameter.orElseThrow().clazz);
                            throw new RuntimeException(e);
                        }
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * represents a single api method
     * invocationMethod - the method to be invoked
     * taskResource - the method's task resource
     * taskInvocationParameters - the input params for the method
     * isLoggable - if true, log method invocation
     * shouldCheckExecutionAllowed - if true invoke method, otherwise return without invoking
     */
    private record TaskInvocationMethod(
            @SuppressWarnings("unused") Method invocationMethod,
            @SuppressWarnings("unused") TaskResource taskResource,
            @SuppressWarnings("unused") List<TaskInvocationParameter> taskInvocationParameters,
            @SuppressWarnings("unused") boolean isLoggable,
            @SuppressWarnings("unused") boolean shouldCheckExecutionAllowed) {}

    /**
     * represents a single method param
     * name - the name of the param in the api
     * clazz - the class of the param in java method
     * typedClass - optional, the type of the generic clazz (ex: {@code List<Integer>})
     */
    private record TaskInvocationParameter(
            @SuppressWarnings("unused") String name,
            @SuppressWarnings("unused") Class clazz,
            @SuppressWarnings("unused") Class typedClass) {}
}
