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
import { Box, Typography, ToggleButtonGroup, ToggleButton } from '@mui/material'
import React from 'react'
import { LayoutDirectionType } from './types'

interface IHelpMessageProps {
    layoutDirection: LayoutDirectionType
    onLayoutDirectionChange: (layoutDirection: LayoutDirectionType) => void
}

export const HelpMessage = ({ layoutDirection, onLayoutDirectionChange }: IHelpMessageProps) => {
    const handleLayoutChange = (_event: React.MouseEvent<HTMLElement>, newDirection: LayoutDirectionType | null) => {
        if (newDirection !== null) {
            onLayoutDirectionChange(newDirection)
        }
    }

    return (
        <Box
            sx={{
                position: 'absolute',
                top: 10,
                right: 10,
                backgroundColor: 'background.paper',
                p: 1,
                border: '1px solid #e0e0e0',
                borderRadius: 1,
                boxShadow: 3,
                zIndex: 10,
                minWidth: 200,
                textAlign: 'center',
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'center',
            }}
        >
            <Typography variant="body2" color="text.secondary" sx={{ mb: 0 }}>
                Scroll to zoom in/out
            </Typography>

            <ToggleButtonGroup
                value={layoutDirection}
                onChange={handleLayoutChange}
                size="small"
                color="primary"
                exclusive
            >
                <ToggleButton value="BT">
                    <Typography variant="caption">Vertical</Typography>
                </ToggleButton>
                <ToggleButton value="RL">
                    <Typography variant="caption">Horizontal</Typography>
                </ToggleButton>
            </ToggleButtonGroup>
        </Box>
    )
}
