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
package io.trino.plugin.iceberg.catalog.hms;

import com.google.common.collect.ImmutableMap;
import io.trino.hive.thrift.metastore.MetaException;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.plugin.iceberg.catalog.hms.AbstractMetastoreTableOperations.CommitStatus.FAILURE;
import static io.trino.plugin.iceberg.catalog.hms.AbstractMetastoreTableOperations.CommitStatus.SUCCESS;
import static io.trino.plugin.iceberg.catalog.hms.AbstractMetastoreTableOperations.CommitStatus.UNKNOWN;
import static io.trino.plugin.iceberg.catalog.hms.HiveMetastoreTableOperations.checkCommitStatus;
import static io.trino.plugin.iceberg.catalog.hms.HiveMetastoreTableOperations.isConcurrentModificationRejection;
import static org.assertj.core.api.Assertions.assertThat;

final class TestHiveMetastoreTableOperations
{
    private static final SchemaTableName TABLE = new SchemaTableName("test_schema", "test_table");
    private static final String TABLE_LOCATION = "s3://bucket/test_schema/test_table";
    private static final String PREVIOUS_LOCATION = TABLE_LOCATION + "/metadata/00000-previous.metadata.json";
    private static final String BASE_LOCATION = TABLE_LOCATION + "/metadata/00001-base.metadata.json";
    private static final String NEW_LOCATION = TABLE_LOCATION + "/metadata/00002-ours.metadata.json";
    private static final String CONCURRENT_LOCATION = TABLE_LOCATION + "/metadata/00002-theirs.metadata.json";
    private static final String LATER_LOCATION = TABLE_LOCATION + "/metadata/00003-later.metadata.json";
    private static final String LATEST_LOCATION = TABLE_LOCATION + "/metadata/00004-latest.metadata.json";

    @Test
    void testCommittedLocationMatchesNewLocationIsSuccess()
    {
        // the metastore points at the metadata we wrote, so it does not even have to be read
        assertThat(checkCommitStatus(TABLE, BASE_LOCATION, NEW_LOCATION, rejectedByMetastore(), () -> Optional.of(NEW_LOCATION), TestHiveMetastoreTableOperations::failToLoad))
                .isEqualTo(SUCCESS);
        assertThat(checkCommitStatus(TABLE, BASE_LOCATION, NEW_LOCATION, timeout(), () -> Optional.of(NEW_LOCATION), TestHiveMetastoreTableOperations::failToLoad))
                .isEqualTo(SUCCESS);
    }

    @Test
    void testNewLocationInMetadataLogIsSuccess()
    {
        // our commit was applied, then a concurrent writer committed on top of it
        assertThat(checkCommitStatus(TABLE, BASE_LOCATION, NEW_LOCATION, rejectedByMetastore(), () -> Optional.of(LATER_LOCATION), _ -> metadataWithHistory(BASE_LOCATION, NEW_LOCATION)))
                .isEqualTo(SUCCESS);
        assertThat(checkCommitStatus(TABLE, BASE_LOCATION, NEW_LOCATION, timeout(), () -> Optional.of(LATER_LOCATION), _ -> metadataWithHistory(BASE_LOCATION, NEW_LOCATION)))
                .isEqualTo(SUCCESS);
        // the base was already pushed out of the metadata log (write.metadata.previous-versions-max), our location was not
        assertThat(checkCommitStatus(TABLE, BASE_LOCATION, NEW_LOCATION, rejectedByMetastore(), () -> Optional.of(LATEST_LOCATION), _ -> metadataWithHistory(NEW_LOCATION, LATER_LOCATION)))
                .isEqualTo(SUCCESS);
    }

    @Test
    void testRejectedConcurrentModificationIsFailure()
    {
        // HIVE-28121 and HIVE-26882 message forms: the base is still in the metadata log, our location is not
        assertThat(checkCommitStatus(TABLE, BASE_LOCATION, NEW_LOCATION, rejectedByMetastore(), () -> Optional.of(CONCURRENT_LOCATION), _ -> metadataWithHistory(BASE_LOCATION)))
                .isEqualTo(FAILURE);
        assertThat(checkCommitStatus(TABLE, BASE_LOCATION, NEW_LOCATION, rejectedByMetastoreWithValues(), () -> Optional.of(CONCURRENT_LOCATION), _ -> metadataWithHistory(BASE_LOCATION)))
                .isEqualTo(FAILURE);
        // more than one concurrent commit landed on top of the base
        assertThat(checkCommitStatus(TABLE, BASE_LOCATION, NEW_LOCATION, rejectedByMetastore(), () -> Optional.of(LATER_LOCATION), _ -> metadataWithHistory(BASE_LOCATION, CONCURRENT_LOCATION)))
                .isEqualTo(FAILURE);
    }

    @Test
    void testCommittedLocationStillExpectedIsUnknown()
    {
        // the metastore still points at the base of this commit; a metadata file never lists itself in its own
        // metadata log, so neither the rejection nor a timeout allows a verdict
        assertThat(checkCommitStatus(TABLE, BASE_LOCATION, NEW_LOCATION, rejectedByMetastore(), () -> Optional.of(BASE_LOCATION), _ -> metadataWithHistory(PREVIOUS_LOCATION)))
                .isEqualTo(UNKNOWN);
        assertThat(checkCommitStatus(TABLE, BASE_LOCATION, NEW_LOCATION, timeout(), () -> Optional.of(BASE_LOCATION), _ -> metadataWithHistory(PREVIOUS_LOCATION)))
                .isEqualTo(UNKNOWN);
    }

    @Test
    void testExpectedLocationMissingFromMetadataLogIsUnknown()
    {
        // without the base in the metadata log, an applied commit could have been pushed out as well: the rejection alone is not enough
        // write.metadata.previous-versions-max trimmed the base (and would have trimmed our location)
        assertThat(checkCommitStatus(TABLE, BASE_LOCATION, NEW_LOCATION, rejectedByMetastore(), () -> Optional.of(LATEST_LOCATION), _ -> metadataWithHistory(CONCURRENT_LOCATION, LATER_LOCATION)))
                .isEqualTo(UNKNOWN);
        // a file without history, e.g. one registered from an existing metadata file, proves nothing about the base
        assertThat(checkCommitStatus(TABLE, BASE_LOCATION, NEW_LOCATION, rejectedByMetastore(), () -> Optional.of(CONCURRENT_LOCATION), _ -> metadataWithHistory()))
                .isEqualTo(UNKNOWN);
        // locations are compared verbatim, like Iceberg does: a legacy '//metadata/' entry does not match the fixed base
        assertThat(checkCommitStatus(TABLE, BASE_LOCATION, NEW_LOCATION, rejectedByMetastore(), () -> Optional.of(CONCURRENT_LOCATION), _ -> metadataWithHistory(TABLE_LOCATION + "//metadata/00001-base.metadata.json")))
                .isEqualTo(UNKNOWN);
    }

    @Test
    void testFailureStateWithoutRejectionMessageIsUnknown()
    {
        assertThat(checkCommitStatus(TABLE, BASE_LOCATION, NEW_LOCATION, timeout(), () -> Optional.of(CONCURRENT_LOCATION), _ -> metadataWithHistory(BASE_LOCATION)))
                .isEqualTo(UNKNOWN);
    }

    @Test
    void testUnverifiableStateIsUnknown()
    {
        assertThat(checkCommitStatus(TABLE, BASE_LOCATION, NEW_LOCATION, rejectedByMetastore(), () -> { throw new RuntimeException("metastore unavailable"); }, TestHiveMetastoreTableOperations::failToLoad))
                .isEqualTo(UNKNOWN);
        assertThat(checkCommitStatus(TABLE, BASE_LOCATION, NEW_LOCATION, rejectedByMetastore(), Optional::empty, TestHiveMetastoreTableOperations::failToLoad))
                .isEqualTo(UNKNOWN);
        assertThat(checkCommitStatus(TABLE, BASE_LOCATION, NEW_LOCATION, rejectedByMetastore(), () -> Optional.of(CONCURRENT_LOCATION), TestHiveMetastoreTableOperations::failToLoad))
                .isEqualTo(UNKNOWN);
    }

    @Test
    void testIsConcurrentModificationRejection()
    {
        assertThat(isConcurrentModificationRejection(rejectedByMetastore())).isTrue();
        assertThat(isConcurrentModificationRejection(rejectedByMetastoreWithValues())).isTrue();
        assertThat(isConcurrentModificationRejection(new RuntimeException(rejectedByMetastore()))).isTrue();

        assertThat(isConcurrentModificationRejection(timeout())).isFalse();
        assertThat(isConcurrentModificationRejection(metastoreException("Unable to alter table"))).isFalse();
        assertThat(isConcurrentModificationRejection(new MetaException())).isFalse();
        assertThat(isConcurrentModificationRejection(metastoreException("The table has been modified. The parameter value for key 'comment' is different"))).isFalse();
    }

    // HIVE-28121: the conditional UPDATE affected no row
    private static RuntimeException rejectedByMetastore()
    {
        return metastoreException("The table has been modified. The parameter value for key 'metadata_location' is different");
    }

    // HIVE-26882: the in-memory comparison of the expected value failed
    private static RuntimeException rejectedByMetastoreWithValues()
    {
        return metastoreException("The table has been modified. The parameter value for key 'metadata_location' is '" + CONCURRENT_LOCATION + "'. The expected was value was '" + BASE_LOCATION + "'");
    }

    private static RuntimeException timeout()
    {
        return new TrinoException(HIVE_METASTORE_ERROR, new RuntimeException("Read timed out"));
    }

    private static RuntimeException metastoreException(String message)
    {
        return new TrinoException(HIVE_METASTORE_ERROR, new MetaException(message));
    }

    private static TableMetadata failToLoad(String location)
    {
        throw new RuntimeException("Cannot read " + location);
    }

    private static TableMetadata metadataWithHistory(String... previousLocations)
    {
        TableMetadata metadata = TableMetadata.newTableMetadata(
                new Schema(Types.NestedField.required(1, "x", Types.IntegerType.get())),
                PartitionSpec.unpartitioned(),
                TABLE_LOCATION,
                ImmutableMap.of());
        for (String previousLocation : previousLocations) {
            metadata = TableMetadata.buildFrom(metadata)
                    .setPreviousFileLocation(previousLocation)
                    .setProperties(ImmutableMap.of("touched", previousLocation))
                    .build();
        }
        return metadata;
    }
}
