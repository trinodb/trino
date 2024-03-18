
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

package io.trino.plugin.warp.gen.constants;

public enum FunctionType
{
    FUNCTION_TYPE_NONE,
    FUNCTION_TYPE_CEIL,
    FUNCTION_TYPE_IS_NAN,
    FUNCTION_TYPE_CAST,
    FUNCTION_TYPE_DAY,
    FUNCTION_TYPE_DAY_OF_WEEK,
    FUNCTION_TYPE_DAY_OF_YEAR,
    FUNCTION_TYPE_WEEK,
    FUNCTION_TYPE_YEAR_OF_WEEK,
    FUNCTION_TYPE_TRANSFORMED,
    FUNCTION_TYPE_NUM_OF;

    FunctionType()
    {
    }
}
