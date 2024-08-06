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
