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
import ContentCopyIcon from '@mui/icons-material/ContentCopy'
import { IconButton, IconButtonProps, Tooltip } from '@mui/material'
import { useSnackbar } from './SnackbarContext.ts'

interface CopyButtonProps extends Omit<IconButtonProps, 'onClick'> {
    text: string
    successMessage: string
    tooltip: string
}

export const CopyButton = ({ text, successMessage, tooltip, ...iconButtonProps }: CopyButtonProps) => {
    const { showSnackbar } = useSnackbar()

    const copyText = () => {
        if (!navigator.clipboard?.writeText) {
            showSnackbar('Copy to clipboard is not available in this browser context', 'error')
            return
        }

        navigator.clipboard
            .writeText(text)
            .then(() => showSnackbar(successMessage, 'success'))
            .catch(() => showSnackbar('Copy to clipboard failed', 'error'))
    }

    return (
        <Tooltip title={tooltip}>
            <span>
                <IconButton {...iconButtonProps} onClick={copyText}>
                    <ContentCopyIcon fontSize={iconButtonProps.size === 'small' ? 'inherit' : 'medium'} />
                </IconButton>
            </span>
        </Tooltip>
    )
}
