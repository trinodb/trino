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

import {
    addToHistory,
} from "../utils";

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
            workerInfo: null,
            initialized: false,
            ended: false,

            workerid: [],
            workerip: [],
        };
        this.refreshLoop = this.refreshLoop.bind(this);
    }

    refreshLoop() {
        clearTimeout(this.timeoutId); // to stop multiple series of refreshLoop from going on simultaneously
        // const nodeId = getFirstParameter(window.location.search);
        $.get('/v1/worker', function (workerInfo) {
            this.setState({
                initialized: true,
                workerInfo: workerInfo
            })
            for (var index in workerInfo) {
                this.setState({
                    workerid: addToHistory(workerInfo[index].nodeId, this.state.workerid),
                    workerip: addToHistory(workerInfo[index].nodeIp, this.state.workerip)
                });
            }
        }.bind(this))
            .error(function () {
                this.setState({
                    initialized: true,
                });
            }.bind(this));

    }

    componentDidMount() {
        this.refreshLoop();
    }

    render() {
        const workerInfo = this.state.workerInfo;
        const workerid = this.state.workerid;
        const workerip = this.state.workerip;

        if (workerInfo === null) {
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

        var list = function () {
            var trs = [];
            for (var i  in workerid) {
                trs.push(
                    <tr>
                        <td className="info-text wrap-text"><a href={"worker.html?" + workerid[i]} className="font-light" target="_blank">{workerid[i]}</a></td>
                        <td className="info-text wrap-text"><a href={"worker.html?" + workerid[i]} className="font-light" target="_blank">{workerip[i]}</a></td>
                    </tr>
                );
            }
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
                            </tr>
                            {list()}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        );
    }
}
