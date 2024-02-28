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

d3.json('/ui/api/query/' + window.location.search.substring(1), function (query) {
    d3.select('#queryId').text(query.queryId);
    renderTimeline(query);
});

function renderTimeline(data) {
    function getTasks(stage) {
        return [].concat.apply(
                stage.tasks,
                stage.subStages.map(getTasks));
    }
    tasks = getTasks(data.outputStage);
    tasks = tasks.map(function(task) {
        return {
            taskId: task.taskStatus.taskId.substring(task.taskStatus.taskId.indexOf('.') + 1),
            time: {
                create: task.stats.createTime,
                firstStart: task.stats.firstStartTime,
                lastStart: task.stats.lastStartTime,
                lastEnd: task.stats.lastEndTime,
                end: task.stats.endTime,
            },
        };
    });

    var groups = new vis.DataSet();
    var items = new vis.DataSet();
    for (var i = 0; i < tasks.length; i++) {
        var task = tasks[i];
        var stageId = task.taskId.substr(0, task.taskId.indexOf("."));
        var taskNumber = task.taskId.substr(task.taskId.indexOf(".") + 1);
        if (taskNumber == 0) {
            groups.add({
                id: stageId,
                content: stageId,
                sort: stageId,
                subgroupOrder: 'sort',
            });
        }
        items.add({
            group: stageId,
            start: task.time.create,
            end: task.time.firstStart,
            className: 'red',
            subgroup: taskNumber,
            sort: -taskNumber,
        });
        items.add({
            group: stageId,
            start: task.time.firstStart,
            end: task.time.lastStart,
            className: 'green',
            subgroup: taskNumber,
            sort: -taskNumber,
        });
        items.add({
            group: stageId,
            start: task.time.lastStart,
            end: task.time.lastEnd,
            className: 'blue',
            subgroup: taskNumber,
            sort: -taskNumber,
        });
        items.add({
            group: stageId,
            start: task.time.lastEnd,
            end: task.time.end,
            className: 'orange',
            subgroup: taskNumber,
            sort: -taskNumber,
        });
    }

    var options = {
        stack: false,
        groupOrder: 'sort',
        padding: 0,
        margin: 0,
        clickToUse: true,
    };

    var container = document.getElementById('timeline');

    var timeline = new vis.Timeline(container, items, groups, options);
}
