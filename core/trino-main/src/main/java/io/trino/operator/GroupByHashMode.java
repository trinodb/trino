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
package io.trino.operator;

public enum GroupByHashMode {
    // Hash values are computed by the FlatGroupByHash instance and stored along with the entry. This consumes more
    // memory, but makes re-hashing cheaper by avoiding the need to re-compute each hash code and also makes the
    // valueIdentical check cheaper by avoiding a deep equality check when hashes don't match
    CACHED,
    // Hash values are re-computed for each access on demand. This avoids storing the hash value directly in the
    // table which saves memory, but can be more expensive during rehash.
    ON_DEMAND;

    public boolean isHashCached()
    {
        return switch (this) {
            case CACHED -> true;
            case ON_DEMAND -> false;
        };
    }
}
