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
package io.trino.plugin.base;

import com.google.errorprone.annotations.Keep;

/**
 * An annotation to put on record parameters as a workaround to
 * <a href="https://github.com/google/error-prone/issues/2713">error-prone's 2713 issue</a>.
 */
@Keep
public @interface RecordParameter {}
