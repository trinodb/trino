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
import { Fragment, useState } from 'react'
import { Box, Popover, TableCell, TableRow, Tooltip } from '@mui/material'

export interface BreakdownSegment {
    label: string
    description: string
    // A real colour (not a palette path): the label on the segment picks its contrast colour from it
    color: string
    value: number
    // Text on the segment; defaults to the initials of the label ("Other off-CPU" -> "OOC")
    short?: string
}

// Table row with a stacked bar of segments (largest first unless `keepOrder`, e.g. used parts before "free");
// clicking anywhere on the row opens a dropdown listing every segment
export const BreakdownRow = ({
    label,
    segments,
    formatValue,
    keepOrder = false,
}: {
    label: string
    segments: BreakdownSegment[]
    formatValue: (value: number) => string
    keepOrder?: boolean
}) => {
    const [barElement, setBarElement] = useState<HTMLDivElement | null>(null)
    const [open, setOpen] = useState(false)
    const labelCell = <TableCell sx={{ fontWeight: 'bold', whiteSpace: 'nowrap' }}>{label}</TableCell>
    const total = segments.reduce((sum, segment) => sum + segment.value, 0)
    if (total === 0) {
        return (
            <TableRow>
                {labelCell}
                <TableCell />
            </TableRow>
        )
    }

    const visibleSegments = segments
        .filter((segment) => segment.value > 0)
        .sort((a, b) => (keepOrder ? 0 : b.value - a.value))
        .map((segment) => ({
            ...segment,
            percent: (segment.value / total) * 100,
            abbreviation:
                segment.short ??
                segment.label
                    .split(/[\s-]+/)
                    .map((word) => word.charAt(0).toUpperCase())
                    .join(''),
        }))

    return (
        <>
            <TableRow
                hover
                role="button"
                tabIndex={0}
                aria-haspopup="true"
                aria-expanded={open}
                onClick={() => setOpen(true)}
                onKeyDown={(event) => {
                    if (event.key === 'Enter' || event.key === ' ') {
                        event.preventDefault()
                        setOpen(true)
                    }
                }}
                sx={{ cursor: 'pointer' }}
            >
                {labelCell}
                <TableCell>
                    <Box ref={setBarElement} sx={{ display: 'flex', height: 16, borderRadius: 1, overflow: 'hidden' }}>
                        {visibleSegments.map((segment) => (
                            <Tooltip
                                key={segment.label}
                                title={`${segment.label}: ${formatValue(segment.value)} (${segment.percent.toFixed(1)}%)`}
                            >
                                <Box
                                    sx={(theme) => ({
                                        width: `${segment.percent}%`,
                                        bgcolor: segment.color,
                                        color: theme.palette.getContrastText(segment.color),
                                        containerType: 'inline-size',
                                        fontSize: 10,
                                        fontWeight: 600,
                                        lineHeight: '16px',
                                        textAlign: 'center',
                                        overflow: 'hidden',
                                        whiteSpace: 'nowrap',
                                        // Hide the abbreviation unless the segment is wide enough to show all of it
                                        [`@container (max-width: ${segment.abbreviation.length * 7 + 6}px)`]: {
                                            '& > span': { display: 'none' },
                                        },
                                    })}
                                >
                                    <span>{segment.abbreviation}</span>
                                </Box>
                            </Tooltip>
                        ))}
                    </Box>
                </TableCell>
            </TableRow>
            {/* Sibling of the row, not a child: clicks inside the portal would otherwise bubble to the row */}
            <Popover
                open={open}
                anchorEl={barElement}
                onClose={() => setOpen(false)}
                anchorOrigin={{ vertical: 'bottom', horizontal: 'left' }}
                transformOrigin={{ vertical: 'top', horizontal: 'left' }}
                slotProps={{ paper: { sx: { mt: 0.5, p: 1.5, maxWidth: 420 } } }}
            >
                <Box
                    sx={{
                        display: 'grid',
                        gridTemplateColumns: 'auto 1fr auto auto',
                        columnGap: 1.5,
                        rowGap: 0.75,
                        alignItems: 'baseline',
                        fontSize: 13,
                    }}
                >
                    {visibleSegments.map((segment) => (
                        <Fragment key={segment.label}>
                            <Box sx={{ width: 10, height: 10, borderRadius: '2px', bgcolor: segment.color }} />
                            <Box>
                                <Box sx={{ fontWeight: 500 }}>{segment.label}</Box>
                                <Box sx={{ color: 'text.secondary', fontSize: 12 }}>{segment.description}</Box>
                            </Box>
                            <Box sx={{ textAlign: 'right', whiteSpace: 'nowrap' }}>{formatValue(segment.value)}</Box>
                            <Box sx={{ textAlign: 'right', whiteSpace: 'nowrap', color: 'text.secondary' }}>
                                {`${segment.percent.toFixed(1)}%`}
                            </Box>
                        </Fragment>
                    ))}
                </Box>
            </Popover>
        </>
    )
}
