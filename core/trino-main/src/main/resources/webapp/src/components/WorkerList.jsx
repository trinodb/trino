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

import React from "react";

const SMALL_SPARKLINE_PROPERTIES = {
    width: '100%',
    height: '57px',
    fillColor: '#3F4552',
    lineColor: '#747F96',
    spotColor: '#1EDCFF',
    tooltipClassname: 'sparkline-tooltip',
    disableHiddenCheck: true,
};

export class WorkerList extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            initialized: false,
            workers: []
        };
        this.refreshLoop = this.refreshLoop.bind(this);
    }

    refreshLoop() {
        clearTimeout(this.timeoutId);
        $.get('/ui/api/worker', function (workers) {
            this.setState({
                initialized: true,
                workers: workers
            })
            this.resetTimer();
        }.bind(this))
                .fail(() => {
                    this.setState({
                        initialized: true,
                    });
                    this.resetTimer();
                });
    }

    resetTimer() {
        clearTimeout(this.timeoutId);
        this.timeoutId = setTimeout(this.refreshLoop.bind(this), 1000);
    }

    componentDidMount() {
        this.refreshLoop();
    }

    render() {
        const workers = this.state.workers;
        if (workers === null) {
            if (this.state.initialized === false) {
                return (
                    <div className="loader">Loading...</div>
                );
            }
            else {
                return (
                    <div className="row error-message">
                        <div className="col-xs-12"><h4>Worker list information could not be loaded</h4></div>
                    </div>
                );
            }
        }
        let workerList = function () {
            let trs = [];
            workers.forEach((worker) => {
                trs.push(
                    <tr>
                        <td className="info-text wrap-text"><a href={"worker.html?" + worker.nodeId} className="font-light" target="_blank">{worker.nodeId}</a></td>
                        <td className="info-text wrap-text"><a href={"worker.html?" + worker.nodeId} className="font-light" target="_blank">{worker.nodeIp}</a></td>
                        <td className="info-text wrap-text">{worker.nodeVersion}</td>
                        <td className="info-text wrap-text">{String(worker.coordinator)}</td>
                        <td className="info-text wrap-text">{worker.state}</td>
                    </tr>
                );
            });
            return trs;
        };

        return (
            <div>
                <div className="row">
                    <div className="col-xs-12">
                        <h3>Overview</h3>
                        <hr className="h3-hr"/>
                        <table className="table">
                            <tbody>
                            <tr>
                                <td className="info-title stage-table-stat-text">Node ID</td>
                                <td className="info-title stage-table-stat-text">Node IP</td>
                                <td className="info-title stage-table-stat-text">Node Version</td>
                                <td className="info-title stage-table-stat-text">Coordinator</td>
                                <td className="info-title stage-table-stat-text">State</td>
                            </tr>
                            {workerList()}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        );
    }
}
