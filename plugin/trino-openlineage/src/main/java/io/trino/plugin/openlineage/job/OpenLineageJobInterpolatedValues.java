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
package io.trino.plugin.openlineage.job;

import io.trino.plugin.base.logging.FormatInterpolator.InterpolatedValue;

import java.util.function.Function;

public enum OpenLineageJobInterpolatedValues
        implements InterpolatedValue<OpenLineageJobContext>
{
    QUERY_ID(jobContext -> jobContext.queryMetadata().getQueryId()),
    SOURCE(jobContext -> jobContext.queryContext().getSource().orElse("")),
    CLIENT_IP(jobContext -> jobContext.queryContext().getRemoteClientAddress().orElse("")),
    USER(jobContext -> jobContext.queryContext().getUser());

    private final Function<OpenLineageJobContext, String> valueProvider;

    OpenLineageJobInterpolatedValues(Function<OpenLineageJobContext, String> valueProvider)
    {
        this.valueProvider = valueProvider;
    }

    @Override
    public String value(OpenLineageJobContext context)
    {
        return valueProvider.apply(context);
    }
}
