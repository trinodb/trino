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
package io.prestosql.sql.planner.iterative.rule;

import io.prestosql.Session;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.connector.SampleType;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.SampleNode;
import io.prestosql.sql.planner.plan.SampleNode.Type;
import io.prestosql.sql.planner.plan.TableScanNode;

import static io.prestosql.SystemSessionProperties.isAllowPushdownIntoConnectors;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.plan.Patterns.Sample.sampleType;
import static io.prestosql.sql.planner.plan.Patterns.sample;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.tableScan;
import static io.prestosql.sql.planner.plan.SampleNode.Type.SYSTEM;

public class PushSampleIntoTableScan
        implements Rule<SampleNode>
{
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();
    private static final Pattern<SampleNode> PATTERN = sample().with(sampleType().equalTo(SYSTEM))
            .with(source().matching(tableScan().capturedAs(TABLE_SCAN)));

    private final Metadata metadata;

    public PushSampleIntoTableScan(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public Pattern<SampleNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isAllowPushdownIntoConnectors(session);
    }

    @Override
    public Rule.Result apply(SampleNode sample, Captures captures, Rule.Context context)
    {
        TableScanNode tableScan = captures.get(TABLE_SCAN);

        return metadata.applySample(context.getSession(), tableScan.getTable(), getSamplingType(sample.getSampleType()), sample.getSampleRatio())
                .map(result -> Result.ofPlanNode(new TableScanNode(
                        tableScan.getId(),
                        result,
                        tableScan.getOutputSymbols(),
                        tableScan.getAssignments(),
                        tableScan.getEnforcedConstraint())))
                .orElseGet(Result::empty);
    }

    private static SampleType getSamplingType(Type sampleNodeType)
    {
        switch (sampleNodeType) {
            case SYSTEM:
                return SampleType.SYSTEM;
            case BERNOULLI:
                return SampleType.BERNOULLI;
            default:
                throw new UnsupportedOperationException("Not yet implemented for " + sampleNodeType);
        }
    }
}
