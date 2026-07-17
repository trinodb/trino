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

// Slim Monaco build: the root 'monaco-editor' entry point bundles every language it
// supports plus the TypeScript, CSS and HTML web workers (~12 MB of assets), while the
// UI only ever displays read-only sql, java and json. Import the editor core and just
// those language contributions instead; each language mode lazily loads its tokenizer
// (and for json, its worker) in a separate chunk only when first used.
import 'monaco-editor/esm/vs/basic-languages/sql/sql.contribution.js'
import 'monaco-editor/esm/vs/basic-languages/java/java.contribution.js'
import 'monaco-editor/esm/vs/language/json/monaco.contribution.js'

export * from 'monaco-editor/esm/vs/editor/edcore.main.js'
