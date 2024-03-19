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
package io.trino.plugin.hive;

import io.trino.filesystem.FileEntry;

import java.util.function.Predicate;

import static com.google.common.base.Predicates.alwaysTrue;

public enum S3StorageClassFilter {
    READ_ALL,
    READ_NON_GLACIER,
    READ_NON_GLACIER_AND_RESTORED;

    private static final String S3_GLACIER_TAG = "s3:glacier";
    private static final String S3_GLACIER_AND_RESTORED_TAG = "s3:glacierRestored";

    /**
     * Checks if the S3 object is not an object with a storage class of glacier/deep_archive
     *
     * @return boolean that helps identify if FileEntry object contains tags for glacier object
     */
    private static boolean isNotGlacierObject(FileEntry fileEntry)
    {
        return !fileEntry.tags().contains(S3_GLACIER_TAG);
    }

    /**
     * Only restored objects will have the restoreExpiryDate set.
     * Ignore not-restored objects and in-progress restores.
     *
     * @return boolean that helps identify if FileEntry object contains tags for glacier or glacierRestored object
     */
    private static boolean isCompletedRestoredObject(FileEntry fileEntry)
    {
        return isNotGlacierObject(fileEntry) || fileEntry.tags().contains(S3_GLACIER_AND_RESTORED_TAG);
    }

    public Predicate<FileEntry> toFileEntryPredicate()
    {
        return switch (this) {
            case READ_ALL -> alwaysTrue();
            case READ_NON_GLACIER -> S3StorageClassFilter::isNotGlacierObject;
            case READ_NON_GLACIER_AND_RESTORED -> S3StorageClassFilter::isCompletedRestoredObject;
        };
    }
}
