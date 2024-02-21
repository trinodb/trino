/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.formats.csv;

import com.google.common.collect.ImmutableSet;
import io.starburst.schema.discovery.infer.InferType;
import io.starburst.schema.discovery.internal.Column;
import io.starburst.schema.discovery.models.DiscoveredColumns;
import io.trino.plugin.hive.type.Category;
import io.trino.plugin.hive.type.TypeInfo;

import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;

// ported/taken from Spark's CsvInferSchema.scala
class CsvInferSchema
{
    private final CsvStreamSampler csvStreamSampler;
    private final CsvOptions options;
    private final InferType inferType;

    CsvInferSchema(CsvStreamSampler csvStreamSampler, CsvOptions options)
    {
        this.csvStreamSampler = csvStreamSampler;
        this.options = options;
        inferType = new InferType(options, csvStreamSampler.getColumnNames());
    }

    // ref: https://github.com/apache/spark/blob/v3.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/csv/CSVInferSchema.scala#L58
    DiscoveredColumns infer(boolean fieldsAreQuoted)
    {
        List<TypeInfo> startType = inferType.buildStartType();
        List<TypeInfo> rootTypes = csvStreamSampler.stream().reduce(startType, this::inferRowType, inferType::mergeRowTypes);
        List<Column> columns = inferType.toStructFields(rootTypes);
        ImmutableSet<String> flags = fieldsAreQuoted ? ImmutableSet.of(CsvFlags.HAS_QUOTED_FIELDS) : ImmutableSet.<String>of();
        List<String> examples = csvStreamSampler.examples();
        List<Column> columnsWithExamples = IntStream.range(0, columns.size())
                .mapToObj(index -> (index < examples.size()) ? columns.get(index).withSampleValue(examples.get(index)) : columns.get(index))
                .collect(toImmutableList());
        return new DiscoveredColumns(columnsWithExamples, flags);
    }

    // ref: https://github.com/apache/spark/blob/v3.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/csv/CSVInferSchema.scala#L82
    private List<TypeInfo> inferRowType(List<TypeInfo> rowSoFar, List<String> next)
    {
        return inferType.inferRowType(rowSoFar, next, (type, field) -> inferField(type, field, options.checkComplexHadoopTypes()));
    }

    // ref: https://github.com/apache/spark/blob/v3.1.2/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/csv/CSVInferSchema.scala#L101
    TypeInfo inferField(TypeInfo typeSoFar, String field, boolean checkComplexHadoopTypes)
    {
        if (field.equals(options.nullValue())) {
            return typeSoFar;
        }

        if (checkComplexHadoopTypes) {    // added by JZ
            return InferComplexHadoop.inferComplexHadoop(options, typeSoFar, field, (t, f) -> inferField(t, f, t.getCategory() != Category.PRIMITIVE));
        }
        return inferType.inferType(typeSoFar, field);
    }
}
