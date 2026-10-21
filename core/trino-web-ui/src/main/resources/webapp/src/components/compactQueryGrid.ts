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

// Shared by the compact query rows and the sortable header above them, so every column lines up like a table.
// Columns: created, wall, status, user, separator, memory, peak memory, CPU, "Splits" label, finished,
// running and queued splits. Every column has a fixed width except user, which shrinks from USER_MAX to USER_MIN as
// space runs out. Cells never wrap. Below the width where user hits USER_MIN, the header and rows scroll sideways
// together (see QueryList).
const FIXED_BEFORE_USER = [64, 64, 80]
const FIXED_AFTER_USER = [8, 64, 80, 64, 48, 72, 72, 72]
const USER_MIN = 64
const USER_MAX = 160
const GAP = 10
const PADDING_X = 8

const px = (widths: number[]) => widths.map((width) => `${width}px`).join(' ')
const fixedTotal = [...FIXED_BEFORE_USER, ...FIXED_AFTER_USER].reduce((sum, width) => sum + width, 0)
const columnCount = FIXED_BEFORE_USER.length + 1 + FIXED_AFTER_USER.length

export const COMPACT_QUERY_GRID_SX = {
    display: 'grid',
    gridTemplateColumns: `${px(FIXED_BEFORE_USER)} minmax(${USER_MIN}px, ${USER_MAX}px) ${px(FIXED_AFTER_USER)}`,
    columnGap: `${GAP}px`,
    alignItems: 'center',
    px: `${PADDING_X}px`,
    fontSize: 13,
    whiteSpace: 'nowrap',
} as const

// Inner wrapper of each scroller: fills the visible width, but never gets narrower than the table with the user
// column at its minimum; past that point the scroller scrolls
export const COMPACT_TABLE_CONTENT_SX = {
    width: '100%',
    minWidth: `${fixedTotal + USER_MIN + GAP * (columnCount - 1) + PADDING_X * 2}px`,
} as const

// For content inside the sideways-scrolling table that must stay put (query text, error line): it sticks to the
// left edge and is exactly as wide as the visible part of the table. Needs the scroller's containerType.
export const COMPACT_TABLE_PINNED_SX = {
    position: 'sticky',
    left: 0,
    width: '100cqw',
} as const

export const COMPACT_TABLE_SCROLLER_SX = {
    overflowX: 'auto',
    containerType: 'inline-size',
} as const
