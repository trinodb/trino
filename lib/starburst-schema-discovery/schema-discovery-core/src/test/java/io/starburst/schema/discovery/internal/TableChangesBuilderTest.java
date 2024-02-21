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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.starburst.schema.discovery.TableChanges;
import io.starburst.schema.discovery.TableChanges.TableName;
import io.starburst.schema.discovery.models.DiscoveredColumns;
import io.starburst.schema.discovery.models.DiscoveredPartitions;
import io.starburst.schema.discovery.models.DiscoveredTable;
import io.starburst.schema.discovery.models.TableFormat;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.starburst.schema.discovery.models.LowerCaseString.toLowerCase;
import static io.starburst.schema.discovery.models.SlashEndedPath.ensureEndsWithSlash;
import static org.assertj.core.api.Assertions.assertThat;

public class TableChangesBuilderTest
{
    @Test
    public void testPreviousErrorTableGetsFixedChanges()
    {
        TableChangesBuilder tableChangesBuilder = new TableChangesBuilder(ensureEndsWithSlash("s3://dummy"));
        tableChangesBuilder.addPreviousTable(new DiscoveredTable(
                false,
                ensureEndsWithSlash("s3://dummy"),
                new TableName(Optional.empty(), toLowerCase("table123")),
                TableFormat.ERROR,
                ImmutableMap.of(),
                DiscoveredColumns.EMPTY_DISCOVERED_COLUMNS,
                DiscoveredPartitions.EMPTY_DISCOVERED_PARTITIONS,
                ImmutableList.of(toLowerCase("s3://dummy"))));
        tableChangesBuilder.addCurrentTable(new DiscoveredTable(
                true,
                ensureEndsWithSlash("s3://dummy"),
                new TableName(Optional.empty(), toLowerCase("table123")),
                TableFormat.CSV,
                ImmutableMap.of(),
                new DiscoveredColumns(ImmutableList.of(new Column(toLowerCase("column123"), new HiveType(HiveTypes.STRING_TYPE))), ImmutableList.of()),
                DiscoveredPartitions.EMPTY_DISCOVERED_PARTITIONS,
                ImmutableList.of(toLowerCase("s3://dummy"))));

        TableChanges tableChanges = tableChangesBuilder.build();
        assertThat(tableChanges.droppedTables()).isEmpty();
        assertThat(tableChanges.addedTables()).hasSize(1);
    }
}
