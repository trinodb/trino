/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.internal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.airlift.json.ObjectMapperProvider;
import io.starburst.schema.discovery.models.DiscoveredSchema;
import io.starburst.schema.discovery.models.DiscoveredTable;
import io.starburst.schema.discovery.models.SlashEndedPath;
import org.junit.jupiter.api.Test;

import java.util.List;

public class RescanMergerTest
{
    @Test
    public void testMergeRescan()
            throws JsonProcessingException
    {
        String rescanPreviousMetadata = """
                [{"buckets":[],"columns":{"columns":[{"name":"store","type":"string"},{"name":"type","type":"string"},{"name":"size","type":"int"}],"flags":[]},"discoveredPartitions":{"columns":[],"values":[]},"format":"ORC","options":{"comment":"#","complexhadoop":"false","dateformat":"yyyy-MM-dd","delimiter":",","encoding":"UTF-8","escape":"\\\\","excludepatterns":".*","generatedheadersformat":"COL%d","headers":"true","ignoreleadingwhitespace":"false","ignoretrailingwhitespace":"false","includepatterns":"**","lineseparator":"","locale":"US","maxsamplefilespertable":"25","maxsamplelines":"10","maxsampletables":"50","nanvalue":"NaN","negativeinf":"-Inf","nullvalue":"","positiveinf":"Inf","quote":"\\"","samplefilespertablemodulo":"3","samplelinesmodulo":"3","supportbuckets":"false","timestampformat":"yyyy-MM-dd HH:mm:ss[.SSSSSSS]"},"path":"s3://starburst-demo/SalesDemo/stores_orc","tableName":{"tableName":"stores_orc"},"valid":true},{"buckets":[],"columns":{"columns":[{"name":"Store","type":"int"},{"name":"Type","type":"string"},{"name":"Size","type":"int"}],"flags":[]},"discoveredPartitions":{"columns":[],"values":[]},"format":"CSV","options":{"comment":"#","complexhadoop":"false","dateformat":"yyyy-MM-dd","delimiter":",","encoding":"UTF-8","escape":"\\\\","excludepatterns":".*","generatedheadersformat":"COL%d","headers":"true","ignoreleadingwhitespace":"false","ignoretrailingwhitespace":"false","includepatterns":"**","lineseparator":"","locale":"US","maxsamplefilespertable":"25","maxsamplelines":"10","maxsampletables":"50","nanvalue":"NaN","negativeinf":"-Inf","nullvalue":"","positiveinf":"Inf","quote":"\\"","samplefilespertablemodulo":"3","samplelinesmodulo":"3","supportbuckets":"false","timestampformat":"yyyy-MM-dd HH:mm:ss[.SSSSSSS]"},"path":"s3://starburst-demo/SalesDemo/stores","tableName":{"tableName":"stores"},"valid":true},{"buckets":[],"columns":{"columns":[{"name":"Store","type":"int"},{"name":"Dept","type":"int"},{"name":"Date","type":"date"},{"name":"Weekly_Sales","type":"double"},{"name":"IsHoliday","type":"boolean"}],"flags":[]},"discoveredPartitions":{"columns":[],"values":[]},"format":"CSV","options":{"comment":"#","complexhadoop":"false","dateformat":"yyyy-MM-dd","delimiter":",","encoding":"UTF-8","escape":"\\\\","excludepatterns":".*","generatedheadersformat":"COL%d","headers":"true","ignoreleadingwhitespace":"false","ignoretrailingwhitespace":"false","includepatterns":"**","lineseparator":"","locale":"US","maxsamplefilespertable":"25","maxsamplelines":"10","maxsampletables":"50","nanvalue":"NaN","negativeinf":"-Inf","nullvalue":"","positiveinf":"Inf","quote":"\\"","samplefilespertablemodulo":"3","samplelinesmodulo":"3","supportbuckets":"false","timestampformat":"yyyy-MM-dd HH:mm:ss[.SSSSSSS]"},"path":"s3://starburst-demo/SalesDemo/sales","tableName":{"tableName":"sales"},"valid":true}]
                """;
        ObjectMapper objectMapper = new ObjectMapperProvider().get();
        String rescanNewMetadata = """
                [{"buckets":[],"columns":{"columns":[{"name":"store","type":"string"},{"name":"type","type":"string"},{"name":"size","type":"int"}],"flags":[]},"discoveredPartitions":{"columns":[],"values":[]},"format":"ORC","options":{"comment":"#","complexHadoop":"false","dateFormat":"yyyy-MM-dd","delimiter":",","discovered_schema.stores_orc.dateFormat":"yyyy-MM-dd","discovered_schema.stores_orc.delimiter":"\\\\\\\\002C","discovered_schema.stores_orc.headers":"true","discovered_schema.stores_orc.quote":"\\\\","discovered_schema.stores_orc.timestampFormat":"yyyy-MM-dd\\u0027T\\u0027HH:mm:ss.SSSZ","encoding":"UTF-8","escape":"\\\\\\\\","excludePatterns":".*","generatedHeadersFormat":"COL%d","headers":"true","ignoreLeadingWhiteSpace":"false","ignoreTrailingWhiteSpace":"false","includePatterns":"**","lineSeparator":"","locale":"US","maxSampleFilesPerTable":"25","maxSampleLines":"10","maxSampleTables":"50","nanValue":"NaN","negativeInf":"-Inf","nullValue":"","positiveInf":"Inf","quote":"\\\\","sampleFilesPerTableModulo":"3","sampleLinesModulo":"3","supportBuckets":"false","timestampFormat":"yyyy-MM-dd HH:mm:ss[.SSSSSSS]"},"path":"s3://starburst-demo/SalesDemo/stores_orc","tableName":{"tableName":"stores_orc"},"valid":true}]
                """;
        List<DiscoveredTable> rescanDiscoveredTables = objectMapper.readValue(rescanNewMetadata, new TypeReference<>() {});
        DiscoveredSchema mergedSchema = RescanMerger.mergeRescan(new DiscoveredSchema(SlashEndedPath.SINGLE_SLASH_EMPTY, rescanDiscoveredTables, ImmutableList.of()), objectMapper, rescanPreviousMetadata);
        System.out.println(mergedSchema);
    }
}
