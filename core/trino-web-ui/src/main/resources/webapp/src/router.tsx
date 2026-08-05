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
import { ReactNode } from 'react'
import AppsOutlinedIcon from '@mui/icons-material/AppsOutlined'
import HomeOutlinedIcon from '@mui/icons-material/HomeOutlined'
import DnsOutlinedIcon from '@mui/icons-material/DnsOutlined'
import { RouteProps } from 'react-router-dom'
import { Dashboard } from './components/Dashboard'
import { DemoComponents } from './components/DemoComponents'
import { WorkersList } from './components/WorkersList.tsx'
import { Texts } from './constant'

export interface RouterItem {
    itemKey?: string
    text?: string
    icon?: ReactNode
    hidden?: boolean
    routeProps: RouteProps
}

export type RouterItems = RouterItem[]

export const routers: RouterItems = [
    {
        itemKey: 'dashboard',
        text: Texts.Menu.Drawer.Dashboard,
        icon: <HomeOutlinedIcon />,
        routeProps: {
            path: '/dashboard',
            element: <Dashboard />,
        },
    },
    {
        itemKey: 'workers',
        text: Texts.Menu.Drawer.Workers,
        icon: <DnsOutlinedIcon />,
        routeProps: {
            path: '/workers',
            element: <WorkersList />,
        },
    },
    {
        itemKey: 'demo-components',
        text: Texts.Menu.Drawer.DemoComponents,
        icon: <AppsOutlinedIcon />,
        routeProps: {
            path: '/demo-components',
            element: <DemoComponents />,
        },
        hidden: true,
    },
]

export const routersMapper: Record<string | number, RouterItem> = routers.reduce(
    (mapper, item) => {
        if (item.itemKey && item.routeProps && item.routeProps.path) {
            mapper[item.itemKey] = item
            mapper[item.routeProps.path] = item
        }
        return mapper
    },
    {} as Record<string | number, RouterItem>
)
