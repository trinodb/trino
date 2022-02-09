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
package io.trino.sql.query;

import io.trino.FeaturesConfig;
import io.trino.SystemSessionProperties;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.memory.MemoryManagerConfig;
import io.trino.memory.NodeMemoryConfig;
import io.trino.metadata.SessionPropertyManager;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestFilterHideInacessibleColumnsSession
{
    @Test
    public void testDisableWhenEnabledByDefault()
    {
        FeaturesConfig featuresConfig = new FeaturesConfig();
        featuresConfig.setHideInaccessibleColumns(true);
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager(new SystemSessionProperties(
                new QueryManagerConfig(),
                new TaskManagerConfig(),
                new MemoryManagerConfig(),
                featuresConfig,
                new NodeMemoryConfig(),
                new DynamicFilterConfig(),
                new NodeSchedulerConfig()));
        assertThatThrownBy(() -> sessionPropertyManager.validateSystemSessionProperty(SystemSessionProperties.HIDE_INACCESSIBLE_COLUMNS, "false"))
                .hasMessage("hide_inaccessible_columns cannot be disabled with session property when it was enabled with configuration");
    }

    @Test
    public void testEnableWhenAlreadyEnabledByDefault()
    {
        FeaturesConfig featuresConfig = new FeaturesConfig();
        featuresConfig.setHideInaccessibleColumns(true);
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager(new SystemSessionProperties(
                new QueryManagerConfig(),
                new TaskManagerConfig(),
                new MemoryManagerConfig(),
                featuresConfig,
                new NodeMemoryConfig(),
                new DynamicFilterConfig(),
                new NodeSchedulerConfig()));
        assertThatNoException().isThrownBy(() -> sessionPropertyManager.validateSystemSessionProperty(SystemSessionProperties.HIDE_INACCESSIBLE_COLUMNS, "true"));
    }

    @Test
    public void testDisableWhenAlreadyDisabledByDefault()
    {
        FeaturesConfig featuresConfig = new FeaturesConfig();
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager(new SystemSessionProperties(
                new QueryManagerConfig(),
                new TaskManagerConfig(),
                new MemoryManagerConfig(),
                featuresConfig,
                new NodeMemoryConfig(),
                new DynamicFilterConfig(),
                new NodeSchedulerConfig()));
        assertThatNoException().isThrownBy(() -> sessionPropertyManager.validateSystemSessionProperty(SystemSessionProperties.HIDE_INACCESSIBLE_COLUMNS, "false"));
    }

    @Test
    public void testEnableWhenDisabledByDefault()
    {
        FeaturesConfig featuresConfig = new FeaturesConfig();
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager(new SystemSessionProperties(
                new QueryManagerConfig(),
                new TaskManagerConfig(),
                new MemoryManagerConfig(),
                featuresConfig,
                new NodeMemoryConfig(),
                new DynamicFilterConfig(),
                new NodeSchedulerConfig()));
        assertThatNoException().isThrownBy(() -> sessionPropertyManager.validateSystemSessionProperty(SystemSessionProperties.HIDE_INACCESSIBLE_COLUMNS, "true"));
    }
}
