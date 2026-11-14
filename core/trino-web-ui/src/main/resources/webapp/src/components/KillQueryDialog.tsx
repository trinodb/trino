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
import { useRef, useState } from 'react'
import { Button, Dialog, DialogActions, DialogContent, DialogContentText, DialogTitle } from '@mui/material'
import { QueryInfo, killQueryApi } from '../api/webapp/api.ts'
import { Texts } from '../constant.ts'
import { useSnackbar } from './SnackbarContext.ts'

// Confirmation before killing a query from the list, opened by the row's kill button or the Backspace shortcut.
// ←/→ move between Cancel and Kill (Kill starts focused), Enter picks the focused one, Escape cancels.
export const KillQueryDialog = (props: { queryInfo: QueryInfo | null; onClose: () => void }) => {
    const { queryInfo, onClose } = props
    const { showSnackbar } = useSnackbar()
    const cancelRef = useRef<HTMLButtonElement>(null)
    const killRef = useRef<HTMLButtonElement>(null)
    const focusRingSx = { '&.Mui-focusVisible': { outline: '2px solid', outlineOffset: 2 } }
    // The dialog fades out after queryInfo becomes null; keep showing the last query so the text does not change
    // mid-animation
    const [shownQuery, setShownQuery] = useState(queryInfo)
    if (queryInfo !== null && queryInfo !== shownQuery) {
        setShownQuery(queryInfo)
    }

    const killQuery = () => {
        if (!queryInfo) {
            return
        }
        onClose()
        killQueryApi(queryInfo.queryId).then((apiResponse) => {
            if (apiResponse.status === 403) {
                showSnackbar(`${Texts.Error.Forbidden}: You do not have permission to kill this query.`, 'error')
            } else if (apiResponse.status === 202 || apiResponse.status === 409) {
                showSnackbar(Texts.QueryList.Actions.KillRequested, 'success')
            } else {
                showSnackbar(`${Texts.Error.Communication} ${apiResponse.status}: ${apiResponse.message}`, 'error')
            }
        })
    }

    return (
        <Dialog
            open={queryInfo !== null}
            onClose={onClose}
            onKeyDown={(event) => {
                if (event.key === 'ArrowLeft') {
                    event.preventDefault()
                    cancelRef.current?.focus()
                } else if (event.key === 'ArrowRight') {
                    event.preventDefault()
                    killRef.current?.focus()
                } else if (event.key === 'Enter') {
                    event.preventDefault()
                    if (document.activeElement === cancelRef.current) {
                        onClose()
                    } else {
                        killQuery()
                    }
                }
            }}
        >
            <DialogTitle>{Texts.QueryList.Actions.KillConfirmTitle}</DialogTitle>
            <DialogContent>
                <DialogContentText>
                    {shownQuery?.queryId} ({shownQuery?.sessionUser}) will be canceled. This cannot be undone.
                </DialogContentText>
            </DialogContent>
            <DialogActions>
                {/* No ripple: the pulsing focus circle replaced by a plain outline */}
                <Button ref={cancelRef} onClick={onClose} disableRipple sx={focusRingSx}>
                    {Texts.QueryList.Actions.KillConfirmCancel}
                </Button>
                <Button
                    ref={killRef}
                    color="error"
                    variant="contained"
                    onClick={killQuery}
                    autoFocus
                    disableRipple
                    sx={focusRingSx}
                >
                    {Texts.QueryList.Actions.Kill}
                </Button>
            </DialogActions>
        </Dialog>
    )
}
