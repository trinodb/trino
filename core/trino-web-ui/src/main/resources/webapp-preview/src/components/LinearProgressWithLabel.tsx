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
import { Box, LinearProgress, Typography } from '@mui/material'
import Chip, { ChipProps } from '@mui/material/Chip'

export interface LinearProgressWithLabelProps {
    value: number
    title: string
    color?: ChipProps['color']
}

export const LinearProgressWithLabel = (props: LinearProgressWithLabelProps) => {
    const { value, title, color } = props

    return (
        <Box display="flex" alignItems="center">
            <Box sx={{ pr: 2 }}>
                <Chip size="small" label={title} color={color} />
            </Box>
            <>
                <Box sx={{ width: '100%', mr: 1 }}>
                    <LinearProgress variant="determinate" color="info" value={value} />
                </Box>
                <Box sx={{ minWidth: 35 }}>
                    <Typography
                        variant="body2"
                        sx={{ color: 'text.secondary' }}
                    >{`${Math.round(props.value)}%`}</Typography>
                </Box>
            </>
        </Box>
    )
}
