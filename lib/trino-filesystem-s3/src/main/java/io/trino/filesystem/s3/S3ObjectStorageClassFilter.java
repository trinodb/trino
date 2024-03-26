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
package io.trino.filesystem.s3;

import software.amazon.awssdk.services.s3.model.RestoreStatus;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.Optional;
import java.util.function.Function;

public enum S3ObjectStorageClassFilter {
    READ_ALL(o -> true),
    READ_NON_GLACIER(S3ObjectStorageClassFilter::isNotGlacierObject),
    READ_NON_GLACIER_AND_RESTORED(S3ObjectStorageClassFilter::isNonGlacierOrCompletedRestoredObject);

    private final Function<S3Object, Boolean> filter;

    S3ObjectStorageClassFilter(Function<S3Object, Boolean> filter)
    {
        this.filter = filter;
    }

    /**
     * Checks if the s3 object is not an object with a storage class of glacier/deep_archive.
     *
     * @param object s3 object
     * @return if the s3 object is not an object with a storage class of glacier/deep_archive
     */
    private static boolean isNotGlacierObject(S3Object object)
    {
        return switch (object.storageClass()) {
            case GLACIER, DEEP_ARCHIVE -> true;
            default -> false;
        }
    }

    /**
     * Only restored objects will have the restoreExpiryDate set.
     * Ignore not-restored objects and in-progress restores.
     *
     * @param object s3 object
     * @return if the s3 object is completely restored
     */
    private static boolean isNonGlacierOrCompletedRestoredObject(S3Object object)
    {
        return isNotGlacierObject(object) || Optional.ofNullable(object.restoreStatus())
                .map(RestoreStatus::restoreExpiryDate)
                .isPresent();
    }

    public boolean shouldReadObject(S3Object object)
    {
        return switch (this) {
            case READ_ALL -> true;
            case READ_NON_GLACIER -> isNotGlacierObject(object);
            case READ_NON_GLACIER_AND_RESTORED -> isNonGlacierOrCompletedRestoredObject(object);
        };
    }
}
