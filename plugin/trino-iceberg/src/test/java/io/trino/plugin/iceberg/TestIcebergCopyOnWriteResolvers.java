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
package io.trino.plugin.iceberg;

import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.RowLevelOperationMode;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.iceberg.TableProperties.DELETE_ISOLATION_LEVEL;
import static org.apache.iceberg.TableProperties.DELETE_MODE;
import static org.apache.iceberg.TableProperties.MERGE_ISOLATION_LEVEL;
import static org.apache.iceberg.TableProperties.MERGE_ISOLATION_LEVEL_DEFAULT;
import static org.apache.iceberg.TableProperties.MERGE_MODE;
import static org.apache.iceberg.TableProperties.UPDATE_ISOLATION_LEVEL;
import static org.apache.iceberg.TableProperties.UPDATE_MODE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Iceberg connector resolves the copy-on-write vs merge-on-read mode for every row-level
 * operation (DELETE, UPDATE, MERGE) from a single property, {@code write.merge.mode}.
 * {@code write.delete.mode} and {@code write.update.mode} are intentionally ignored so the
 * behaviour is unambiguous when the three properties disagree. The same contract applies to
 * the corresponding isolation-level properties.
 */
final class TestIcebergCopyOnWriteResolvers
{
    private static final String COW = RowLevelOperationMode.COPY_ON_WRITE.modeName();
    private static final String MOR = RowLevelOperationMode.MERGE_ON_READ.modeName();

    // ---------------------------------------------------------------------
    // resolveRowLevelOperationMode -- only write.merge.mode is honoured
    // ---------------------------------------------------------------------

    @Test
    public void testMergeModeSelectsCopyOnWrite()
    {
        assertThat(IcebergMetadata.resolveRowLevelOperationMode(Map.of(MERGE_MODE, COW)))
                .isEqualTo(RowLevelOperationMode.COPY_ON_WRITE);
    }

    @Test
    public void testMergeModeSelectsMergeOnRead()
    {
        assertThat(IcebergMetadata.resolveRowLevelOperationMode(Map.of(MERGE_MODE, MOR)))
                .isEqualTo(RowLevelOperationMode.MERGE_ON_READ);
    }

    @Test
    public void testDefaultsToMergeOnRead()
    {
        assertThat(IcebergMetadata.resolveRowLevelOperationMode(Map.of()))
                .isEqualTo(RowLevelOperationMode.MERGE_ON_READ);
    }

    @Test
    public void testDeleteModeIsIgnored()
    {
        // write.delete.mode must not influence mode selection even when write.merge.mode is absent.
        assertThat(IcebergMetadata.resolveRowLevelOperationMode(Map.of(DELETE_MODE, COW)))
                .isEqualTo(RowLevelOperationMode.MERGE_ON_READ);
    }

    @Test
    public void testUpdateModeIsIgnored()
    {
        // write.update.mode must not influence mode selection even when write.merge.mode is absent.
        assertThat(IcebergMetadata.resolveRowLevelOperationMode(Map.of(UPDATE_MODE, COW)))
                .isEqualTo(RowLevelOperationMode.MERGE_ON_READ);
    }

    @Test
    public void testDeleteAndUpdateModesDoNotOverrideMerge()
    {
        // write.merge.mode=merge-on-read wins over conflicting delete/update properties.
        assertThat(IcebergMetadata.resolveRowLevelOperationMode(
                Map.of(DELETE_MODE, COW, UPDATE_MODE, COW, MERGE_MODE, MOR)))
                .isEqualTo(RowLevelOperationMode.MERGE_ON_READ);
    }

    // ---------------------------------------------------------------------
    // resolveCopyOnWriteIsolationLevel -- only write.merge.isolation-level is honoured
    // ---------------------------------------------------------------------

    @Test
    public void testMergeIsolationLevelSelectsSerializable()
    {
        assertThat(IcebergMetadata.resolveCopyOnWriteIsolationLevel(Map.of(MERGE_ISOLATION_LEVEL, "serializable")))
                .isEqualTo(IsolationLevel.SERIALIZABLE);
    }

    @Test
    public void testMergeIsolationLevelSelectsSnapshot()
    {
        assertThat(IcebergMetadata.resolveCopyOnWriteIsolationLevel(Map.of(MERGE_ISOLATION_LEVEL, "snapshot")))
                .isEqualTo(IsolationLevel.SNAPSHOT);
    }

    @Test
    public void testIsolationDefaultsToMergeIsolationDefault()
    {
        assertThat(IcebergMetadata.resolveCopyOnWriteIsolationLevel(Map.of()))
                .isEqualTo(IsolationLevel.fromName(MERGE_ISOLATION_LEVEL_DEFAULT));
    }

    @Test
    public void testDeleteAndUpdateIsolationLevelsAreIgnored()
    {
        // write.delete.isolation-level and write.update.isolation-level must not influence CoW
        // conflict detection, matching the mode-resolution contract.
        Map<String, String> properties = Map.of(
                DELETE_ISOLATION_LEVEL, "serializable",
                UPDATE_ISOLATION_LEVEL, "serializable",
                MERGE_ISOLATION_LEVEL, "snapshot");

        assertThat(IcebergMetadata.resolveCopyOnWriteIsolationLevel(properties))
                .isEqualTo(IsolationLevel.SNAPSHOT);
    }
}
