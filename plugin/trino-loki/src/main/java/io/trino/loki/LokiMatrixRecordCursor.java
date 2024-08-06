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
package io.trino.loki;

import io.trino.loki.model.Matrix;
import io.trino.loki.model.MetricPoint;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

public class LokiMatrixRecordCursor
        extends LokiRecordCursor
{
    private final Iterator<LokiMatrixRecordCursor.Point> metricItr;

    record Point(MetricPoint p, Map<String, String> labels) {}

    private Point current;

    public LokiMatrixRecordCursor(List<LokiColumnHandle> columnHandles, Matrix matrix)
    {
        super(columnHandles);

        this.metricItr = matrix.getMetrics()
                .stream()
                .flatMap(metric -> metric.values().stream()
                        .map(value -> new LokiMatrixRecordCursor.Point(value, metric.labels()))).iterator();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!metricItr.hasNext()) {
            return false;
        }
        current = metricItr.next();
        return true;
    }

    @Override
    Object getEntryValue(int field)
    {
        checkState(current != null, "Cursor has not been advanced yet");

        int columnIndex = fieldToColumnIndex[field];
        return switch (columnIndex) {
            case 0 -> getSqlMapFromMap(columnHandles.get(columnIndex).type(), current.labels);
            case 1 -> current.p.getTs();
            case 2 -> current.p.getValue();
            default -> null;
        };
    }
}
