/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.json.ObjectMapperProvider;
import io.starburst.schema.discovery.models.DiscoveredTable;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatNoException;

public class TestOldDiscoveryIsDeserializable
{
    private static final String OLD_DISCOVERY_WITH_PARTITION_STRINGIFIED = "[{\"buckets\":[],\"columns\":{\"columns\":[{\"name\":\"year\",\"type\":\"int\"},{\"name\":\"make\",\"type\":\"string\"},{\"name\":\"model\",\"type\":\"string\"},{\"name\":\"comment\",\"type\":\"string\"}],\"flags\":[\"QUOTED_FIELDS\"]},\"discoveredPartitions\":{\"columns\":[{\"name\":\"date\",\"type\":\"string\"}],\"values\":[{\"path\":\"s3://osz-test-us-east-1/cars_partitioned/date\\u003d2022-04/\",\"values\":{\"date\":\"2022-04\"}},{\"path\":\"s3://osz-test-us-east-1/cars_partitioned/date\\u003d2022-05/\",\"values\":{\"date\":\"2022-05\"}}]},\"errors\":[],\"format\":\"CSV\",\"options\":{\"comment\":\"#\",\"complexhadoop\":\"false\",\"dateformat\":\"yyyy-MM-dd\",\"delimiter\":\",\",\"encoding\":\"UTF-8\",\"escape\":\"\\\\\",\"excludepatterns\":\".*\",\"generatedheadersformat\":\"COL%d\",\"headers\":\"true\",\"ignoreleadingwhitespace\":\"false\",\"ignoretrailingwhitespace\":\"false\",\"includepatterns\":\"**\",\"lineseparator\":\"\",\"locale\":\"US\",\"maxsamplefilespertable\":\"25\",\"maxsamplelines\":\"10\",\"maxsampletables\":\"50\",\"nanvalue\":\"NaN\",\"negativeinf\":\"-Inf\",\"nullvalue\":\"\",\"positiveinf\":\"Inf\",\"quote\":\"\\\"\",\"samplefilespertablemodulo\":\"3\",\"samplelinesmodulo\":\"3\",\"supportbuckets\":\"false\",\"timestampformat\":\"yyyy-MM-dd HH:mm:ss[.SSSSSSS]\"},\"path\":\"s3://osz-test-us-east-1/cars_partitioned/\",\"tableName\":{\"tableName\":\"cars_partitioned\"},\"valid\":true}]";

    @Test
    public void testDeserialization()
    {
        ObjectMapper objectMapper = new ObjectMapperProvider().get();
        assertThatNoException().isThrownBy(() -> objectMapper.readValue(OLD_DISCOVERY_WITH_PARTITION_STRINGIFIED, new TypeReference<List<DiscoveredTable>>() {}));
    }
}
