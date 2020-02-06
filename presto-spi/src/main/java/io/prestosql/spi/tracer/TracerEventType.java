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
package io.prestosql.spi.tracer;

public enum TracerEventType
        implements TracerEventTypeSupplier
{
    PLAN_QUERY_START,
    PLAN_QUERY_END,

    QUERY_STATE_CHANGE_QUEUED,
    QUERY_STATE_CHANGE_WAITING_FOR_RESOURCES,
    QUERY_STATE_CHANGE_DISPATCHING,
    QUERY_STATE_CHANGE_PLANNING,
    QUERY_STATE_CHANGE_STARTING,
    QUERY_STATE_CHANGE_RUNNING,
    QUERY_STATE_CHANGE_FINISHING,
    QUERY_STATE_CHANGE_FINISHED,
    QUERY_STATE_CHANGE_FAILED,

    STAGE_STATE_CHANGE_PLANNED,
    STAGE_STATE_CHANGE_SCHEDULING,
    STAGE_STATE_CHANGE_SCHEDULING_SPLITS,
    STAGE_STATE_CHANGE_SCHEDULED,
    STAGE_STATE_CHANGE_RUNNING,
    STAGE_STATE_CHANGE_FINISHED,
    STAGE_STATE_CHANGE_CANCELED,
    STAGE_STATE_CHANGE_ABORTED,
    STAGE_STATE_CHANGE_FAILED,

    ADD_SPLITS_TO_TASK,
    SCHEDULE_TASK_WITH_SPLITS,
    SEND_UPDATE_TASK_REQUEST_START,
    SEND_UPDATE_TASK_REQUEST_END,

    CREATE_LOCAL_PLAN_START,
    CREATE_LOCAL_PLAN_END,

    TASK_STATE_CHANGE_PLANNED,
    TASK_STATE_CHANGE_RUNNING,
    TASK_STATE_CHANGE_FINISHED,
    TASK_STATE_CHANGE_CANCELED,
    TASK_STATE_CHANGE_ABORTED,
    TASK_STATE_CHANGE_FAILED,

    SPLIT_DRIVER_CREATED,
    SPLIT_ADDED,
    SPLIT_SCHEDULED,
    SPLIT_UNSCHEDULED,
    SPLIT_STARTS_WAITING,
    SPLIT_ENDS_WAITING,
    SPLIT_BLOCKED,
    SPLIT_UNBLOCKED,
    SPLIT_FINISHED,
    SPLIT_DESTROY_INVOKED,

    CREATED_DRIVER,

    ADD_SPLIT_TO_OPERATOR;

    @Override
    public String toTracerEventType()
    {
        return name();
    }
}
