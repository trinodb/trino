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
package io.trino.plugin.deltalake.transactionlog;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Set;

public final class DeltaLakeTableFeatures
{
    public static final int MIN_VERSION_SUPPORTS_READER_FEATURES = 3;
    public static final int MIN_VERSION_SUPPORTS_WRITER_FEATURES = 7;

    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#valid-feature-names-in-table-features
    public static final String APPEND_ONLY_FEATURE_NAME = "appendOnly";
    public static final String CHANGE_DATA_FEED_FEATURE_NAME = "changeDataFeed";
    public static final String CHECK_CONSTRAINTS_FEATURE_NAME = "checkConstraints";
    public static final String COLUMN_MAPPING_FEATURE_NAME = "columnMapping";
    public static final String DELETION_VECTORS_FEATURE_NAME = "deletionVectors";
    public static final String ICEBERG_COMPATIBILITY_V1_FEATURE_NAME = "icebergCompatV1";
    public static final String ICEBERG_COMPATIBILITY_V2_FEATURE_NAME = "icebergCompatV2";
    public static final String IDENTITY_COLUMNS_FEATURE_NAME = "identityColumns";
    public static final String INVARIANTS_FEATURE_NAME = "invariants";
    public static final String TIMESTAMP_NTZ_FEATURE_NAME = "timestampNtz";
    public static final String TYPE_WIDENING_FEATURE_NAME = "typeWidening";
    public static final String TYPE_WIDENING_PREVIEW_FEATURE_NAME = "typeWidening-preview";
    public static final String VACUUM_PROTOCOL_CHECK_FEATURE_NAME = "vacuumProtocolCheck";
    public static final String VARIANT_TYPE_FEATURE_NAME = "variantType";
    public static final String VARIANT_TYPE_PREVIEW_FEATURE_NAME = "variantType-preview";
    public static final String V2_CHECKPOINT_FEATURE_NAME = "v2Checkpoint";

    private static final Set<String> SUPPORTED_READER_FEATURES = ImmutableSet.<String>builder()
            .add(COLUMN_MAPPING_FEATURE_NAME)
            .add(TIMESTAMP_NTZ_FEATURE_NAME)
            .add(TYPE_WIDENING_FEATURE_NAME)
            .add(TYPE_WIDENING_PREVIEW_FEATURE_NAME)
            .add(DELETION_VECTORS_FEATURE_NAME)
            .add(VACUUM_PROTOCOL_CHECK_FEATURE_NAME)
            .add(VARIANT_TYPE_FEATURE_NAME)
            .add(VARIANT_TYPE_PREVIEW_FEATURE_NAME)
            .add(V2_CHECKPOINT_FEATURE_NAME)
            .build();
    private static final Set<String> SUPPORTED_WRITER_FEATURES = ImmutableSet.<String>builder()
            .add(APPEND_ONLY_FEATURE_NAME)
            .add(DELETION_VECTORS_FEATURE_NAME)
            .add(INVARIANTS_FEATURE_NAME)
            .add(CHECK_CONSTRAINTS_FEATURE_NAME)
            .add(CHANGE_DATA_FEED_FEATURE_NAME)
            .add(COLUMN_MAPPING_FEATURE_NAME)
            .add(TIMESTAMP_NTZ_FEATURE_NAME)
            .add(VACUUM_PROTOCOL_CHECK_FEATURE_NAME)
            .build();

    private DeltaLakeTableFeatures() {}

    public static Set<String> unsupportedReaderFeatures(Set<String> features)
    {
        return Sets.difference(features, SUPPORTED_READER_FEATURES);
    }

    public static Set<String> unsupportedWriterFeatures(Set<String> features)
    {
        return Sets.difference(features, SUPPORTED_WRITER_FEATURES);
    }
}
