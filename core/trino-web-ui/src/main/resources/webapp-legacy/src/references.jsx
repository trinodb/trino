import React from 'react'
import ReactDOM from 'react-dom'
import { PageTitle } from './components/PageTitle'
import { ReferenceDetail } from './components/ReferenceDetail'

ReactDOM.render(<PageTitle title="References" />, document.getElementById('title'))

ReactDOM.render(<ReferenceDetail />, document.getElementById('references'))
