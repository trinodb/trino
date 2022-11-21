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
package io.trino;

import io.airlift.units.Duration;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.memory.MemoryManagerConfig;
import io.trino.memory.NodeMemoryConfig;
import io.trino.spi.session.PropertyMetadata;
import io.trino.sql.planner.OptimizerConfig;
import org.testng.annotations.Test;

import static io.trino.SystemSessionProperties.QUERY_MAX_RUN_TIME;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestSystemSessionProperties
{
    @Test
    public void testQueryMaxRunTimeWithoutHardLimit()
    {
        SystemSessionProperties properties = new SystemSessionProperties(
                new QueryManagerConfig()
                        .setQueryMaxRunTime(new Duration(2, HOURS)),
                new TaskManagerConfig(),
                new MemoryManagerConfig(),
                new FeaturesConfig(),
                new OptimizerConfig(),
                new NodeMemoryConfig(),
                new DynamicFilterConfig(),
                new NodeSchedulerConfig());
        PropertyMetadata<?> property = getProperty(properties, QUERY_MAX_RUN_TIME);
        assertEquals(property.decode("1h"), new Duration(1, HOURS));
        assertEquals(property.decode("3h"), new Duration(3, HOURS));
    }

    @Test
    public void testQueryMaxRunTimeWithHardLimit()
    {
        SystemSessionProperties properties = new SystemSessionProperties(
                new QueryManagerConfig()
                        .setQueryMaxRunTime(new Duration(2, HOURS))
                        .setQueryMaxRunTimeHardLimit(new Duration(4, HOURS)),
                new TaskManagerConfig(),
                new MemoryManagerConfig(),
                new FeaturesConfig(),
                new OptimizerConfig(),
                new NodeMemoryConfig(),
                new DynamicFilterConfig(),
                new NodeSchedulerConfig());
        PropertyMetadata<?> property = getProperty(properties, QUERY_MAX_RUN_TIME);
        assertEquals(property.decode("1h"), new Duration(1, HOURS));
        assertEquals(property.decode("3h"), new Duration(3, HOURS));
        assertThatThrownBy(() -> property.decode("5h"))
                .hasMessage("query_max_run_time must not exceed 'query.max-run-time.hard-limit' 4.00h: 5.00h");
    }

    private static PropertyMetadata<?> getProperty(SystemSessionProperties properties, String propertyName)
    {
        return properties.getSessionProperties().stream()
                .filter(p -> p.getName().equals(propertyName))
                .findFirst()
                .orElseThrow();
    }
}
