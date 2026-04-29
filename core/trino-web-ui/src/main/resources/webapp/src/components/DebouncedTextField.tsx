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
import React, { useState, useEffect, useMemo, forwardRef } from 'react'
import TextField, { TextFieldProps } from '@mui/material/TextField'
import { debounce } from 'lodash'

type DebouncedTextFieldProps = TextFieldProps & {
    debounceTime?: number
}

export const DebouncedTextField = forwardRef<HTMLInputElement, DebouncedTextFieldProps>(
    ({ onChange, debounceTime = 500, value: propValue, ...props }, ref) => {
        const [value, setValue] = useState<string>((propValue as string) ?? '')

        useEffect(() => {
            setValue((propValue as string) ?? '')
        }, [propValue])

        const debouncedChangeHandler = useMemo(() => {
            return debounce((event: React.ChangeEvent<HTMLInputElement>) => {
                if (onChange) {
                    onChange(event)
                }
            }, debounceTime)
        }, [debounceTime, onChange])

        const handleChange = (event: React.ChangeEvent<HTMLInputElement>) => {
            setValue(event.target.value)
            debouncedChangeHandler(event)
        }

        return <TextField {...props} value={value} onChange={handleChange} ref={ref} />
    }
)
