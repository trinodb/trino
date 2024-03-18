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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import io.trino.plugin.varada.dispatcher.connectors.ConnectorTaskExecutor;
import org.reflections.Reflections;

import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;

public class VaradaTasksModule
        implements Module
{
    private final boolean isCoordinator;
    private final boolean isWorker;
    private final Set<Class<? extends BooleanSupplier>> taskExecutionEnabledSupplierClassSet;

    public VaradaTasksModule(boolean isCoordinator,
            boolean isWorker,
            Set<Class<? extends BooleanSupplier>> taskExecutionEnabledSupplierClassSet)
    {
        this.isCoordinator = isCoordinator;
        this.isWorker = isWorker;
        this.taskExecutionEnabledSupplierClassSet = taskExecutionEnabledSupplierClassSet;
    }

    @Override
    public void configure(Binder binder)
    {
        Multibinder<BooleanSupplier> booleanSupplierMultibinder = Multibinder.newSetBinder(binder,
                BooleanSupplier.class,
                Names.named(TaskExecutor.NAMED_TASK_EXEC_ENABLE_SUPPLIER));
        taskExecutionEnabledSupplierClassSet.forEach(supplier -> booleanSupplierMultibinder.addBinding()
                .to(supplier.asSubclass(BooleanSupplier.class)));

        Reflections reflections = new Reflections("io.trino.plugin.warp");

        Set<Class<?>> taskExecutors = reflections.getTypesAnnotatedWith(TaskResourceMarker.class)
                .stream()
                .filter(aClass -> !aClass.getPackage().getName().contains("test"))
                .filter(this::isTaskAvailable)
                .collect(Collectors.toSet());

        binder.bind(ConnectorTaskExecutor.class).to(TaskExecutor.class);
        Multibinder<TaskResource> multibinder = Multibinder.newSetBinder(binder, TaskResource.class);

        taskExecutors.forEach((task) -> {
            jaxrsBinder(binder).bind(task);
            multibinder.addBinding().to(task.asSubclass(TaskResource.class));
        });
    }

    private boolean isTaskAvailable(Class<?> aClass)
    {
        boolean keep = false;
        if (aClass.getAnnotationsByType(TaskResourceMarker.class).length > 0) {
            if (isCoordinator) {
                keep = aClass.getAnnotationsByType(TaskResourceMarker.class)[0].coordinator();
            }
            if (isWorker) {
                keep = keep || aClass.getAnnotationsByType(TaskResourceMarker.class)[0].worker();
            }
        }
        return keep;
    }
}
