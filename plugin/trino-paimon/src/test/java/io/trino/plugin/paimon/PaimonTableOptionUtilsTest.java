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
package io.trino.plugin.paimon;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PaimonTableOptionUtilsTest
{
    @Test
    public void testLatestFileFormatOptionsArePassedThroughAsStrings()
    {
        Schema.Builder builder = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("embedding", DataTypes.ARRAY(DataTypes.FLOAT()));

        PaimonTableOptionUtils.buildOptions(builder, Map.ofEntries(
                entry("file_format", "mosaic"),
                entry("vector_file_format", "lance"),
                entry("variant_shredding_schema", "{\"type\":\"object\"}"),
                entry("variant_infer_shredding_schema", "true"),
                entry("variant_shredding_max_schema_width", "64"),
                entry("variant_shredding_max_schema_depth", "8"),
                entry("variant_shredding_min_field_cardinality_ratio", "0.25"),
                entry("variant_shredding_max_infer_buffer_row", "512"),
                entry("blob_descriptor_field", "payload"),
                entry("blob_view_field", "thumbnail"),
                entry("vector_field", "embedding")));

        assertThat(builder.build().options())
                .containsEntry(CoreOptions.FILE_FORMAT.key(), "mosaic")
                .containsEntry(CoreOptions.VECTOR_FILE_FORMAT.key(), "lance")
                .containsEntry(CoreOptions.VARIANT_SHREDDING_SCHEMA.key(), "{\"type\":\"object\"}")
                .containsEntry(CoreOptions.VARIANT_INFER_SHREDDING_SCHEMA.key(), "true")
                .containsEntry(CoreOptions.VARIANT_SHREDDING_MAX_SCHEMA_WIDTH.key(), "64")
                .containsEntry(CoreOptions.VARIANT_SHREDDING_MAX_SCHEMA_DEPTH.key(), "8")
                .containsEntry(CoreOptions.VARIANT_SHREDDING_MIN_FIELD_CARDINALITY_RATIO.key(), "0.25")
                .containsEntry(CoreOptions.VARIANT_SHREDDING_MAX_INFER_BUFFER_ROW.key(), "512")
                .containsEntry(CoreOptions.BLOB_DESCRIPTOR_FIELD.key(), "payload")
                .containsEntry(CoreOptions.BLOB_VIEW_FIELD.key(), "thumbnail")
                .containsEntry(CoreOptions.VECTOR_FIELD.key(), "embedding");
    }

    @Test
    public void testDocumentedPaimon15OptionsArePassedThroughAsStrings()
    {
        Schema.Builder builder = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("embedding", DataTypes.ARRAY(DataTypes.FLOAT()))
                .column("payload", DataTypes.BYTES());

        PaimonTableOptionUtils.buildOptions(builder, Map.ofEntries(
                entry("blob_as_descriptor", "true"),
                entry("blob_view_resolve_enabled", "false"),
                entry("blob_write_null_on_missing_file", "true"),
                entry("blob_target_file_size", "8 MB"),
                entry("blob_split_by_file_size", "true"),
                entry("vector_target_file_size", "64 MB"),
                entry("vector_search_distribute_enabled", "true"),
                entry("scan_fallback_snapshot_branch", "snapshot_branch"),
                entry("scan_fallback_delta_branch", "delta_branch"),
                entry("scan_fallback_branch_read_fail_fast", "true"),
                entry("scan_primary_branch", "main_branch"),
                entry("row_tracking_partition_group_on_commit", "false"),
                entry("data_evolution_merge_into_file_pruning", "false"),
                entry("data_evolution_merge_into_source_persist", "true")));

        assertThat(builder.build().options())
                .containsEntry(CoreOptions.BLOB_AS_DESCRIPTOR.key(), "true")
                .containsEntry(CoreOptions.BLOB_VIEW_RESOLVE_ENABLED.key(), "false")
                .containsEntry(CoreOptions.BLOB_WRITE_NULL_ON_MISSING_FILE.key(), "true")
                .containsEntry(CoreOptions.BLOB_TARGET_FILE_SIZE.key(), "8 MB")
                .containsEntry(CoreOptions.BLOB_SPLIT_BY_FILE_SIZE.key(), "true")
                .containsEntry(CoreOptions.VECTOR_TARGET_FILE_SIZE.key(), "64 MB")
                .containsEntry(CoreOptions.VECTOR_SEARCH_DISTRIBUTE_ENABLED.key(), "true")
                .containsEntry(CoreOptions.SCAN_FALLBACK_SNAPSHOT_BRANCH.key(), "snapshot_branch")
                .containsEntry(CoreOptions.SCAN_FALLBACK_DELTA_BRANCH.key(), "delta_branch")
                .containsEntry(CoreOptions.SCAN_FALLBACK_BRANCH_READ_FAIL_FAST.key(), "true")
                .containsEntry(CoreOptions.SCAN_PRIMARY_BRANCH.key(), "main_branch")
                .containsEntry(CoreOptions.ROW_TRACKING_PARTITION_GROUP_ON_COMMIT.key(), "false")
                .containsEntry(CoreOptions.DATA_EVOLUTION_MERGE_INTO_FILE_PRUNING.key(), "false")
                .containsEntry(CoreOptions.DATA_EVOLUTION_MERGE_INTO_SOURCE_PERSIST.key(), "true");
    }

    @Test
    public void testCamelCasePaimonOptionsAreExposedAsSnakeCase()
    {
        assertThat(PaimonTableOptionUtils.convertOptionKey(CoreOptions.VARIANT_SHREDDING_SCHEMA.key()))
                .isEqualTo("variant_shredding_schema");
        assertThat(PaimonTableOptionUtils.convertOptionKey(CoreOptions.VARIANT_INFER_SHREDDING_SCHEMA.key()))
                .isEqualTo("variant_infer_shredding_schema");
        assertThat(PaimonTableOptionUtils.convertOptionKey(CoreOptions.VARIANT_SHREDDING_MAX_SCHEMA_WIDTH.key()))
                .isEqualTo("variant_shredding_max_schema_width");
        assertThat(PaimonTableOptionUtils.convertOptionKey(CoreOptions.VARIANT_SHREDDING_MIN_FIELD_CARDINALITY_RATIO.key()))
                .isEqualTo("variant_shredding_min_field_cardinality_ratio");
    }

    @Test
    public void testTrinoTableOptionKeysMapBackToPaimonKeys()
    {
        assertThat(PaimonTableOptionUtils.toPaimonOptionKey("variant_shredding_max_schema_width"))
                .isEqualTo(CoreOptions.VARIANT_SHREDDING_MAX_SCHEMA_WIDTH.key());
        assertThat(PaimonTableOptionUtils.toPaimonOptionKey("vector_file_format"))
                .isEqualTo(CoreOptions.VECTOR_FILE_FORMAT.key());
        assertThat(PaimonTableOptionUtils.toPaimonOptionKey("scan_fallback_branch"))
                .isEqualTo(CoreOptions.SCAN_FALLBACK_BRANCH.key());
        assertThat(PaimonTableOptionUtils.toPaimonOptionKey("scan_fallback_branch_read_fail_fast"))
                .isEqualTo(CoreOptions.SCAN_FALLBACK_BRANCH_READ_FAIL_FAST.key());
        assertThat(PaimonTableOptionUtils.toPaimonOptionKey("blob_view_resolve_enabled"))
                .isEqualTo(CoreOptions.BLOB_VIEW_RESOLVE_ENABLED.key());
        assertThat(PaimonTableOptionUtils.toPaimonOptionKey("vector_search_distribute_enabled"))
                .isEqualTo(CoreOptions.VECTOR_SEARCH_DISTRIBUTE_ENABLED.key());
        assertThat(PaimonTableOptionUtils.toPaimonOptionKey("custom.option"))
                .isEqualTo("custom.option");
    }

    @Test
    public void testRuntimeOnlyTablePropertiesAreIdentifiedFromTrinoKeys()
    {
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyTableProperty("scan_snapshot_id")).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyTableProperty("stream_scan_mode")).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyTableProperty("batch_scan_mode")).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyTableProperty("incremental_between")).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyTableProperty("incremental_to_auto_tag")).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyTableProperty("scan_version")).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyTableProperty("consumer_id")).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyTableProperty("consumer_ignore_progress")).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyTableProperty("path")).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyTableProperty("key_value_sequence_number_enabled")).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyTableProperty("materialized_table_refresh_status")).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyTableProperty(
                CoreOptions.KEY_VALUE_SEQUENCE_NUMBER_ENABLED.key())).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyTableProperty(
                CoreOptions.MATERIALIZED_TABLE_REFRESH_STATUS.key())).isTrue();

        assertThat(PaimonTableOptionUtils.isRuntimeOnlyTableProperty("scan_fallback_branch")).isFalse();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyTableProperty("blob_view_resolve_enabled")).isFalse();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyTableProperty("vector_file_format")).isFalse();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyTableProperty("custom.option")).isFalse();
    }

    @Test
    public void testRuntimeOnlyPaimonOptionKeysAreIdentified()
    {
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKey(CoreOptions.SCAN_SNAPSHOT_ID.key())).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKey(CoreOptions.STREAM_SCAN_MODE.key())).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKey(CoreOptions.BATCH_SCAN_MODE.key())).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKey(CoreOptions.INCREMENTAL_BETWEEN.key())).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKey(CoreOptions.SCAN_IGNORE_LOST_FILE.key())).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKey(CoreOptions.SCAN_MANIFEST_PARALLELISM.key())).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKey(CoreOptions.CONSUMER_ID.key())).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKey(CoreOptions.CONSUMER_IGNORE_PROGRESS.key())).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKey(CoreOptions.PATH.key())).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKey(
                CoreOptions.KEY_VALUE_SEQUENCE_NUMBER_ENABLED.key())).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKey(
                CoreOptions.MATERIALIZED_TABLE_REFRESH_STATUS.key())).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKey(CoreOptions.SCAN_FALLBACK_BRANCH.key())).isFalse();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKey(CoreOptions.BLOB_VIEW_RESOLVE_ENABLED.key())).isFalse();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKey(CoreOptions.VECTOR_FILE_FORMAT.key())).isFalse();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKey("custom.option")).isFalse();
    }

    @Test
    public void testWriteDynamicOnlyPaimonOptionKeysAreIdentified()
    {
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKeyForWrite(CoreOptions.SCAN_SNAPSHOT_ID.key())).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKeyForWrite(CoreOptions.STREAM_SCAN_MODE.key())).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKeyForWrite(CoreOptions.BATCH_SCAN_MODE.key())).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKeyForWrite(CoreOptions.SCAN_FALLBACK_SNAPSHOT_BRANCH.key())).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKeyForWrite(CoreOptions.SCAN_FALLBACK_DELTA_BRANCH.key())).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKeyForWrite(CoreOptions.SCAN_FALLBACK_BRANCH.key())).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKeyForWrite(CoreOptions.SCAN_FALLBACK_BRANCH_READ_FAIL_FAST.key())).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKeyForWrite(CoreOptions.SCAN_PRIMARY_BRANCH.key())).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKeyForWrite(CoreOptions.PATH.key())).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKeyForWrite(
                CoreOptions.KEY_VALUE_SEQUENCE_NUMBER_ENABLED.key())).isTrue();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKeyForWrite(
                CoreOptions.MATERIALIZED_TABLE_REFRESH_STATUS.key())).isTrue();

        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKeyForWrite(CoreOptions.BLOB_VIEW_RESOLVE_ENABLED.key())).isFalse();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKeyForWrite(CoreOptions.VECTOR_FILE_FORMAT.key())).isFalse();
        assertThat(PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKeyForWrite("custom.option")).isFalse();
    }

    @Test
    public void testPaimonOptionsAreExposedAsStrings()
    {
        PaimonTableOptions tableOptions = new PaimonTableOptions();

        assertStringTableProperty(tableOptions, "merge_engine");
        assertStringTableProperty(tableOptions, "vector_field");
        assertStringTableProperty(tableOptions, "vector_target_file_size");
        assertStringTableProperty(tableOptions, "vector_search_distribute_enabled");
        assertStringTableProperty(tableOptions, "scan_fallback_branch");
        assertStringTableProperty(tableOptions, "scan_fallback_branch_read_fail_fast");
        assertStringTableProperty(tableOptions, "scan_primary_branch");
        assertStringTableProperty(tableOptions, "blob_as_descriptor");
        assertStringTableProperty(tableOptions, "blob_view_resolve_enabled");
        assertThat(tableOptions.getTableProperties())
                .noneMatch(property -> property.getName().equals("scan_snapshot_id"));
        assertThat(tableOptions.getTableProperties())
                .noneMatch(property -> property.getName().equals("scan_version"));
        assertThat(tableOptions.getTableProperties())
                .noneMatch(property -> property.getName().equals("incremental_between"));
        assertThat(tableOptions.getTableProperties())
                .noneMatch(property -> property.getName().equals("stream_scan_mode"));
        assertThat(tableOptions.getTableProperties())
                .noneMatch(property -> property.getName().equals("batch_scan_mode"));
        assertThat(tableOptions.getTableProperties())
                .noneMatch(property -> property.getName().equals("consumer_id"));
        assertThat(tableOptions.getTableProperties())
                .noneMatch(property -> property.getName().equals("consumer_ignore_progress"));
        assertThat(tableOptions.getTableProperties())
                .noneMatch(property -> property.getName().equals("path"));
        assertThat(tableOptions.getTableProperties())
                .noneMatch(property -> property.getName().equals("key_value_sequence_number_enabled"));
        assertThat(tableOptions.getTableProperties())
                .noneMatch(property -> property.getName().startsWith("materialized_table_"));
        assertThat(tableOptions.getTableProperties())
                .noneMatch(property -> property.getName().equals("branch"));
    }

    @Test
    public void testBlankTableOptionsAreRejected()
    {
        Schema.Builder builder = Schema.newBuilder()
                .column("id", DataTypes.INT());

        assertThatThrownBy(() -> PaimonTableOptionUtils.buildOptions(builder, Map.of("file_format", " ")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("properties value for property 'file_format' is blank");
    }

    @Test
    public void testKnownDynamicOptionValuesAreNormalized()
    {
        assertThat(PaimonTableOptionUtils.normalizeDynamicOptionValue(CoreOptions.SCAN_SNAPSHOT_ID.key(), " 123 "))
                .isEqualTo("123");
        assertThat(PaimonTableOptionUtils.normalizeDynamicOptionValue(CoreOptions.SCAN_IGNORE_LOST_FILE.key(), " true "))
                .isEqualTo("true");
        assertThat(PaimonTableOptionUtils.normalizeDynamicOptionValue(CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key(), " delta "))
                .isEqualTo("delta");
    }

    @Test
    public void testFreeFormDynamicOptionValuesArePreserved()
    {
        assertThat(PaimonTableOptionUtils.normalizeDynamicOptionValue(CoreOptions.SCAN_TAG_NAME.key(), " tag-1 "))
                .isEqualTo(" tag-1 ");
        assertThat(PaimonTableOptionUtils.normalizeDynamicOptionValue("custom.option", " custom value "))
                .isEqualTo(" custom value ");
    }

    @Test
    public void testTypedAndIdentifierLikeTableOptionValuesAreTrimmed()
    {
        Schema.Builder builder = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("embedding", DataTypes.ARRAY(DataTypes.FLOAT()));

        PaimonTableOptionUtils.buildOptions(builder, Map.ofEntries(
                entry("bucket", " 7 "),
                entry("bucket_append_ordered", " false "),
                entry("merge_engine", " partial-update "),
                entry("sequence_field_sort_order", " descending "),
                entry("target_file_size", " 64 MB "),
                entry("file_format", " parquet "),
                entry("vector_file_format", " lance "),
                entry("file_compression", " zstd "),
                entry("metadata_stats_mode", " truncate(16) "),
                entry("partition_mark_done_action", " done-file ")));

        assertThat(builder.build().options())
                .containsEntry(CoreOptions.BUCKET.key(), "7")
                .containsEntry(CoreOptions.BUCKET_APPEND_ORDERED.key(), "false")
                .containsEntry(CoreOptions.MERGE_ENGINE.key(), "partial-update")
                .containsEntry(CoreOptions.SEQUENCE_FIELD_SORT_ORDER.key(), "descending")
                .containsEntry(CoreOptions.TARGET_FILE_SIZE.key(), "64 MB")
                .containsEntry(CoreOptions.FILE_FORMAT.key(), "parquet")
                .containsEntry(CoreOptions.VECTOR_FILE_FORMAT.key(), "lance")
                .containsEntry(CoreOptions.FILE_COMPRESSION.key(), "zstd")
                .containsEntry(CoreOptions.METADATA_STATS_MODE.key(), "truncate(16)")
                .containsEntry(CoreOptions.PARTITION_MARK_DONE_ACTION.key(), "done-file");
    }

    @Test
    public void testFreeFormStringTableOptionValuesArePreserved()
    {
        Schema.Builder builder = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("payload", DataTypes.BYTES());

        PaimonTableOptionUtils.buildOptions(builder, Map.ofEntries(
                entry("variant_shredding_schema", " {\"type\":\"object\"} "),
                entry("partition_timestamp_pattern", " yyyy-MM-dd HH:mm:ss ")));

        assertThat(builder.build().options())
                .containsEntry(CoreOptions.VARIANT_SHREDDING_SCHEMA.key(), " {\"type\":\"object\"} ")
                .containsEntry(CoreOptions.PARTITION_TIMESTAMP_PATTERN.key(), " yyyy-MM-dd HH:mm:ss ");
    }

    @Test
    public void testNativePaimonOptionKeysCanNormalizeValuesForAlterTable()
    {
        assertThat(PaimonTableOptionUtils.normalizeOptionValue(
                CoreOptions.FILE_FORMAT.key(), CoreOptions.FILE_FORMAT.key(), " avro "))
                .isEqualTo("avro");
    }

    @Test
    public void testBuildOptionsRejectsNonStringOptionValues()
    {
        Schema.Builder builder = Schema.newBuilder()
                .column("id", DataTypes.INT());

        assertThatThrownBy(() -> PaimonTableOptionUtils.buildOptions(builder, Map.of("bucket", List.of("4"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("properties value for property 'bucket' must be a string");
    }

    @Test
    public void testBuildOptionsRejectsNullInputs()
    {
        Schema.Builder builder = Schema.newBuilder()
                .column("id", DataTypes.INT());

        assertThatThrownBy(() -> PaimonTableOptionUtils.buildOptions(null, Map.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("builder is null");
        assertThatThrownBy(() -> PaimonTableOptionUtils.buildOptions(builder, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("properties is null");
    }

    @Test
    public void testBuildOptionsRejectsBlankOptionKeys()
    {
        Schema.Builder builder = Schema.newBuilder()
                .column("id", DataTypes.INT());

        assertThatThrownBy(() -> PaimonTableOptionUtils.buildOptions(
                builder,
                Collections.singletonMap(null, "value")))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("properties contains null option key");
        assertThatThrownBy(() -> PaimonTableOptionUtils.buildOptions(builder, Map.of(" ", "value")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("properties contains blank option key");
    }

    @Test
    public void testOptionKeyConversionRejectsMalformedInputs()
    {
        assertThatThrownBy(() -> PaimonTableOptionUtils.convertOptionKey(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("key is null");
        assertThatThrownBy(() -> PaimonTableOptionUtils.convertOptionKey(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("key is blank");
        assertThatThrownBy(() -> PaimonTableOptionUtils.toPaimonOptionKey(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("trinoOptionKey is null");
        assertThatThrownBy(() -> PaimonTableOptionUtils.toPaimonOptionKey(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("trinoOptionKey is blank");
        assertThatThrownBy(() -> PaimonTableOptionUtils.isRuntimeOnlyTableProperty(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("trinoOptionKey is null");
        assertThatThrownBy(() -> PaimonTableOptionUtils.isRuntimeOnlyTableProperty(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("trinoOptionKey is blank");
        assertThatThrownBy(() -> PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKey(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("paimonOptionKey is null");
        assertThatThrownBy(() -> PaimonTableOptionUtils.isRuntimeOnlyPaimonOptionKey(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("paimonOptionKey is blank");
    }

    @Test
    public void testOptionValueTypeDoesNotLeakBetweenCoreOptions()
    {
        List<PaimonTableOptionUtils.OptionInfo> optionInfos = PaimonTableOptionUtils.getOptionInfos();

        assertThat(optionInfos)
                .filteredOn(option -> option.paimonOptionKey.equals(CoreOptions.FILE_FORMAT_PER_LEVEL.key()))
                .singleElement()
                .satisfies(option -> {
                    assertThat(option.type).isEmpty();
                });
    }

    @Test
    public void testReflectedOptionKeysAreUnique()
    {
        List<PaimonTableOptionUtils.OptionInfo> optionInfos = PaimonTableOptionUtils.getOptionInfos();

        assertThat(optionInfos.stream()
                .map(option -> option.trinoOptionKey)
                .distinct()
                .count())
                .isEqualTo(optionInfos.size());
        assertThat(optionInfos.stream()
                .map(option -> option.paimonOptionKey)
                .distinct()
                .count())
                .isEqualTo(optionInfos.size());
    }

    @Test
    public void testTablePropertiesReflectSchemaOptionsAndLayoutProperties()
    {
        Map<String, Object> properties = PaimonTableOptionUtils.tableProperties(
                Map.ofEntries(
                        entry(CoreOptions.BUCKET.key(), "7"),
                        entry(CoreOptions.BUCKET_KEY.key(), "id"),
                        entry(CoreOptions.VECTOR_FILE_FORMAT.key(), "lance"),
                        entry(CoreOptions.BLOB_VIEW_RESOLVE_ENABLED.key(), "false"),
                        entry(CoreOptions.SCAN_FALLBACK_BRANCH_READ_FAIL_FAST.key(), "true"),
                        entry(CoreOptions.SCAN_PRIMARY_BRANCH.key(), "main_branch"),
                        entry(CoreOptions.SCAN_SNAPSHOT_ID.key(), "7"),
                        entry(CoreOptions.INCREMENTAL_BETWEEN.key(), "1,2"),
                        entry(CoreOptions.CONSUMER_ID.key(), "streaming-job"),
                        entry(CoreOptions.CONSUMER_IGNORE_PROGRESS.key(), "true"),
                        entry(CoreOptions.MATERIALIZED_TABLE_REFRESH_HANDLER_BYTES.key(), "serialized"),
                        entry(CoreOptions.SCAN_FALLBACK_BRANCH.key(), "branch_a")),
                List.of("id"),
                List.of("pt"));

        assertThat(properties)
                .containsEntry(PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, List.of("id"))
                .containsEntry(PaimonTableOptions.PARTITIONED_BY_PROPERTY, List.of("pt"))
                .containsEntry("bucket", "7")
                .containsEntry("bucket_key", "id")
                .containsEntry("vector_file_format", "lance")
                .containsEntry("blob_view_resolve_enabled", "false")
                .containsEntry("scan_fallback_branch", "branch_a")
                .containsEntry("scan_fallback_branch_read_fail_fast", "true")
                .containsEntry("scan_primary_branch", "main_branch")
                .doesNotContainKeys(
                        "scan_snapshot_id",
                        "incremental_between",
                        "consumer_id",
                        "consumer_ignore_progress",
                        "branch",
                        "stream_scan_mode",
                        "batch_scan_mode",
                        "materialized_table_refresh_handler_bytes");
    }

    @Test
    public void testTablePropertiesRejectNullInputs()
    {
        assertThatThrownBy(() -> PaimonTableOptionUtils.tableProperties(null, List.of(), List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("options is null");
        assertThatThrownBy(() -> PaimonTableOptionUtils.tableProperties(Map.of(), null, List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("primaryKeys is null");
        assertThatThrownBy(() -> PaimonTableOptionUtils.tableProperties(Map.of(), List.of(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("partitionKeys is null");
    }

    @Test
    public void testOptionInfoValidationRejectsMalformedAndDuplicateOptions()
    {
        assertThatThrownBy(() -> PaimonTableOptionUtils.validateOptionInfos(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("optionInfos is null");
        assertThatThrownBy(() -> PaimonTableOptionUtils.validateOptionInfos(Collections.singletonList(null)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("optionInfo is null");
        assertThatThrownBy(() -> PaimonTableOptionUtils.validateOptionInfos(List.of(
                new PaimonTableOptionUtils.OptionInfo(null, "paimon.key", "String"))))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("trinoOptionKey is null");
        assertThatThrownBy(() -> PaimonTableOptionUtils.validateOptionInfos(List.of(
                new PaimonTableOptionUtils.OptionInfo("trino_key", null, "String"))))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("paimonOptionKey is null");
        assertThatThrownBy(() -> PaimonTableOptionUtils.validateOptionInfos(List.of(
                new PaimonTableOptionUtils.OptionInfo(" ", "paimon.key", "String"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("trinoOptionKey is blank");
        assertThatThrownBy(() -> PaimonTableOptionUtils.validateOptionInfos(List.of(
                new PaimonTableOptionUtils.OptionInfo("trino_key", " ", "String"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("paimonOptionKey is blank");
        assertThatThrownBy(() -> PaimonTableOptionUtils.validateOptionInfos(List.of(
                new PaimonTableOptionUtils.OptionInfo("same_key", "paimon.first", "String"),
                new PaimonTableOptionUtils.OptionInfo("same_key", "paimon.second", "String"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Duplicate Trino table option key 'same_key' maps to Paimon keys 'paimon.first' and 'paimon.second'");
        assertThatThrownBy(() -> PaimonTableOptionUtils.validateOptionInfos(List.of(
                new PaimonTableOptionUtils.OptionInfo("first_key", "paimon.same", "String"),
                new PaimonTableOptionUtils.OptionInfo("second_key", "paimon.same", "String"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Duplicate Paimon table option key 'paimon.same' maps to Trino keys 'first_key' and 'second_key'");
    }

    @Test
    public void testPrimaryAndPartitionKeysUseExplicitDefaults()
    {
        assertThat(PaimonTableOptions.getPrimaryKeys(Map.of())).isEmpty();
        assertThat(PaimonTableOptions.getPartitionedKeys(Map.of())).isEmpty();
    }

    @Test
    public void testPrimaryAndPartitionKeysRequireTableProperties()
    {
        assertThatThrownBy(() -> PaimonTableOptions.getPrimaryKeys(null))
                .hasMessage("tableProperties is null");
        assertThatThrownBy(() -> PaimonTableOptions.getPartitionedKeys(null))
                .hasMessage("tableProperties is null");
    }

    @Test
    public void testPrimaryAndPartitionKeysRejectNullValues()
    {
        assertThatThrownBy(() -> PaimonTableOptions.getPrimaryKeys(Map.of(
                PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, Collections.singletonList(null))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("primary_key contains null value");

        assertThatThrownBy(() -> PaimonTableOptions.getPartitionedKeys(Map.of(
                PaimonTableOptions.PARTITIONED_BY_PROPERTY, Collections.singletonList(null))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("partitioned_by contains null value");
    }

    @Test
    public void testPrimaryAndPartitionKeysRejectNonListValues()
    {
        assertThatThrownBy(() -> PaimonTableOptions.getPrimaryKeys(Map.of(
                PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, "id")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("primary_key must be a list of strings");

        assertThatThrownBy(() -> PaimonTableOptions.getPartitionedKeys(Map.of(
                PaimonTableOptions.PARTITIONED_BY_PROPERTY, "dt")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("partitioned_by must be a list of strings");
    }

    @Test
    public void testPrimaryAndPartitionKeysRejectNonStringValues()
    {
        assertThatThrownBy(() -> PaimonTableOptions.getPrimaryKeys(Map.of(
                PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, List.of(1))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("primary_key contains non-string value");

        assertThatThrownBy(() -> PaimonTableOptions.getPartitionedKeys(Map.of(
                PaimonTableOptions.PARTITIONED_BY_PROPERTY, List.of(1))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("partitioned_by contains non-string value");
    }

    @Test
    public void testPrimaryAndPartitionKeysRejectBlankValues()
    {
        assertThatThrownBy(() -> PaimonTableOptions.getPrimaryKeys(Map.of(
                PaimonTableOptions.PRIMARY_KEY_IDENTIFIER, List.of(" "))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("primary_key contains blank value");

        assertThatThrownBy(() -> PaimonTableOptions.getPartitionedKeys(Map.of(
                PaimonTableOptions.PARTITIONED_BY_PROPERTY, List.of(" "))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("partitioned_by contains blank value");
    }

    private static void assertStringTableProperty(PaimonTableOptions tableOptions, String propertyName)
    {
        assertThat(tableOptions.getTableProperties())
                .filteredOn(property -> property.getName().equals(propertyName))
                .singleElement()
                .satisfies(property -> {
                    assertThat(property.getSqlType()).isEqualTo(VARCHAR);
                    assertThat(property.getJavaType()).isEqualTo(String.class);
                });
    }
}
