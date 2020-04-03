/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
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
package com.starburstdata.presto.plugin.oracle;

import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public enum OracleParallelismType
{
    NO_PARALLELISM,
    PARTITIONS,
    /**/;

    public static OracleParallelismType fromString(String value)
    {
        switch (requireNonNull(value, "value is null").toLowerCase(ENGLISH)) {
            case "no_concurrency":
            case "no_parallelism":
                return NO_PARALLELISM;
            case "partitions":
                return PARTITIONS;
        }

        throw new IllegalArgumentException(format("Unrecognized value: '%s'", value));
    }
}
