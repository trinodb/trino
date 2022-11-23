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
package io.trino.plugin.hive.aws.athena;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.hive.aws.athena.projection.DateProjectionFactory;
import io.trino.plugin.hive.aws.athena.projection.EnumProjectionFactory;
import io.trino.plugin.hive.aws.athena.projection.InjectedProjectionFactory;
import io.trino.plugin.hive.aws.athena.projection.IntegerProjectionFactory;
import io.trino.plugin.hive.aws.athena.projection.ProjectionFactory;
import io.trino.plugin.hive.aws.athena.projection.ProjectionType;
import io.trino.plugin.hive.metastore.HiveMetastoreDecorator;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class PartitionProjectionModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        MapBinder<ProjectionType, ProjectionFactory> projectionFactoriesBinder =
                newMapBinder(binder, ProjectionType.class, ProjectionFactory.class);
        projectionFactoriesBinder.addBinding(ProjectionType.ENUM).to(EnumProjectionFactory.class).in(Scopes.SINGLETON);
        projectionFactoriesBinder.addBinding(ProjectionType.INTEGER).to(IntegerProjectionFactory.class).in(Scopes.SINGLETON);
        projectionFactoriesBinder.addBinding(ProjectionType.DATE).to(DateProjectionFactory.class).in(Scopes.SINGLETON);
        projectionFactoriesBinder.addBinding(ProjectionType.INJECTED).to(InjectedProjectionFactory.class).in(Scopes.SINGLETON);

        binder.bind(PartitionProjectionService.class).in(Scopes.SINGLETON);

        newSetBinder(binder, HiveMetastoreDecorator.class)
                .addBinding()
                .to(PartitionProjectionMetastoreDecorator.class)
                .in(Scopes.SINGLETON);
    }
}
