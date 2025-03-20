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
package io.trino.plugin.resourcegroups;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.resourcegroups.SelectionContext;
import io.trino.spi.resourcegroups.SelectionCriteria;
import io.trino.spi.session.ResourceEstimates;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestResourceGroupIdTemplate
{
    private static final ResourceEstimates EMPTY_RESOURCE_ESTIMATES = new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty());

    @Test
    public void testExpansion()
    {
        ResourceGroupIdTemplate template = new ResourceGroupIdTemplate("test.${USER}.${SOURCE}");
        ResourceGroupId expected = new ResourceGroupId(new ResourceGroupId(new ResourceGroupId("test"), "u"), "s");
        assertThat(template.expandTemplate(new VariableMap(ImmutableMap.of("USER", "u", "SOURCE", "s")))).isEqualTo(expected);
        template = new ResourceGroupIdTemplate("test.${USER}");
        assertThat(template.expandTemplate(new VariableMap(ImmutableMap.of("USER", "alice.smith", "SOURCE", "s")))).isEqualTo(new ResourceGroupId(new ResourceGroupId("test"), "alice.smith"));
    }

    @Test
    public void testExtraction()
    {
        ResourceGroupIdTemplate template = new ResourceGroupIdTemplate("test.pipeline.job_${pipeline}_user:${USER}.${USER}");
        ResourceGroupId expected = new ResourceGroupId(new ResourceGroupId(new ResourceGroupId(new ResourceGroupId("test"), "pipeline"), "job_testpipeline_user:user"), "user");

        Pattern sourcePattern = Pattern.compile("scheduler.important.(?<pipeline>[^\\[]*).*");
        StaticSelector selector = new StaticSelector(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(sourcePattern), Optional.empty(), Optional.empty(), Optional.empty(), template);
        SelectionCriteria context = new SelectionCriteria(true, "user", ImmutableSet.of(), "user", Optional.empty(), Optional.of("scheduler.important.testpipeline[5]"), ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES, Optional.empty());

        assertThat(selector.match(context).map(SelectionContext::getResourceGroupId)).isEqualTo(Optional.of(expected));
    }

    @Test
    public void testNoSource()
    {
        ResourceGroupIdTemplate template = new ResourceGroupIdTemplate("test.pipeline.${pipeline}.${SOURCE}_s");
        ResourceGroupId expected = new ResourceGroupId(new ResourceGroupId(new ResourceGroupId(new ResourceGroupId("test"), "pipeline"), "testpipeline"), "_s");

        Pattern userPattern = Pattern.compile("scheduler.important.(?<pipeline>[^\\[]*).*");
        StaticSelector selector = new StaticSelector(Optional.of(userPattern), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), template);
        SelectionCriteria context = new SelectionCriteria(true, "scheduler.important.testpipeline[5]", ImmutableSet.of(), "user", Optional.empty(), Optional.empty(), ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES, Optional.empty());

        assertThat(selector.match(context).map(SelectionContext::getResourceGroupId)).isEqualTo(Optional.of(expected));
    }

    @Test
    public void testNoMatch()
    {
        ResourceGroupIdTemplate template = new ResourceGroupIdTemplate("test.pipeline.${pipeline}.${USER}");
        Pattern sourcePattern = Pattern.compile("scheduler.important.(?<pipeline>[^\\[]*).*");
        StaticSelector selector = new StaticSelector(Optional.empty(), Optional.empty(), Optional.of(sourcePattern), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), template);
        SelectionCriteria context = new SelectionCriteria(true, "user", ImmutableSet.of(), "user", Optional.empty(), Optional.of("scheduler.testpipeline[5]"), ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES, Optional.empty());

        assertThat(selector.match(context)).isEmpty();
    }

    @Test
    public void testUnresolvedVariableLoadTime()
    {
        assertThatThrownBy(() -> {
            ResourceGroupIdTemplate template = new ResourceGroupIdTemplate("test.pipeline.${pipeline}.${user}");
            Pattern sourcePattern = Pattern.compile("scheduler.important.(?<pipeline>[^\\[]*).*");
            StaticSelector selector = new StaticSelector(Optional.empty(), Optional.empty(), Optional.of(sourcePattern), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), template);
            SelectionCriteria context = new SelectionCriteria(true, "user", ImmutableSet.of(), "user", Optional.empty(), Optional.of("scheduler.important.testpipeline[5]"), ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES, Optional.empty());
            selector.match(context);
        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("unresolved variables \\[user\\] in resource group ID.*");
    }

    @Test
    public void testUnresolvedVariableRunTime()
    {
        assertThatThrownBy(() -> {
            ResourceGroupIdTemplate template = new ResourceGroupIdTemplate("test.pipeline.${pipeline}.${USER}");
            Pattern sourcePattern = Pattern.compile("scheduler.important.(testpipeline\\[|(?<pipeline>[^\\[]*)).*");
            StaticSelector selector = new StaticSelector(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(sourcePattern), Optional.empty(), Optional.empty(), Optional.empty(), template);
            SelectionCriteria context = new SelectionCriteria(true, "user", ImmutableSet.of(), "user", Optional.empty(), Optional.of("scheduler.important.testpipeline[5]"), ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES, Optional.empty());
            selector.match(context);
        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("unresolved variable 'pipeline' in resource group '\\$\\{pipeline\\}', available.*");
    }
}
