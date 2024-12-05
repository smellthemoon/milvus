// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"time"

	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

type SchemaChangeTask interface {
	GetJobID() int64
	GetTaskID() int64
	GetCollectionID() int64
	GetNodeID() int64
	GetSlotUsage() int64
	GetSegmentIDs() []int64
	GetType() internalpb.SchemaChangeType
	GetFieldSchemas() []*schemapb.FieldSchema
	GetState() internalpb.SchemaChangeState
	GetTR() *timerecord.TimeRecorder
	Clone() SchemaChangeTask
}

type addFieldsTask struct {
	*datapb.AddFieldsTask
	tr *timerecord.TimeRecorder
}

func (j *addFieldsTask) GetType() internalpb.SchemaChangeType {
	return internalpb.SchemaChangeType_addField
}

func (t *addFieldsTask) GetTR() *timerecord.TimeRecorder {
	return t.tr
}

func (t *addFieldsTask) GetSlotUsage() int64 {
	// add it in yaml
	return 1
}

func (t *addFieldsTask) Clone() SchemaChangeTask {
	return &addFieldsTask{
		AddFieldsTask: proto.Clone(t.AddFieldsTask).(*datapb.AddFieldsTask),
		tr:            t.tr,
	}
}

func (t *addFieldsTask) MarshalJSON() ([]byte, error) {
	addFieldTask := metricsinfo.AddFieldTask{
		JobID:        t.GetJobID(),
		TaskID:       t.GetTaskID(),
		CollectionID: t.GetCollectionID(),
		NodeID:       t.GetNodeID(),
		State:        t.GetState().String(),
		TaskType:     t.GetType().String(),
		CreatedTime:  t.GetCreatedTime(),
		CompleteTime: t.GetCompleteTime(),
	}
	return json.Marshal(addFieldTask)
}

type schemaChangeTasks struct {
	tasks     map[int64]SchemaChangeTask
	taskStats *expirable.LRU[int64, SchemaChangeTask]
}

func newSchemaChangeTasks() *schemaChangeTasks {
	return &schemaChangeTasks{
		tasks:     make(map[int64]SchemaChangeTask),
		taskStats: expirable.NewLRU[UniqueID, SchemaChangeTask](64, nil, time.Minute*30),
	}
}

func NewAddFieldTasks(segmentGroups [][]int64,
	job SchemaChangeJob,
	alloc allocator.Allocator,
) ([]SchemaChangeTask, error) {
	idStart, _, err := alloc.AllocN(int64(len(segmentGroups)))
	if err != nil {
		return nil, err
	}
	tasks := make([]SchemaChangeTask, 0, len(segmentGroups))
	for i, segments := range segmentGroups {
		task := &addFieldsTask{
			AddFieldsTask: &datapb.AddFieldsTask{
				JobID:        job.GetJobID(),
				TaskID:       idStart + int64(i),
				CollectionID: job.GetCollectionID(),
				State:        internalpb.SchemaChangeState_pending,
				SegmentIDs:   segments,
				CreatedTime:  time.Now().Format("2006-01-02T15:04:05Z07:00"),
			},
			tr: timerecord.NewTimeRecorder("preimport task"),
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func (t *schemaChangeTasks) get(taskID int64) SchemaChangeTask {
	ret, ok := t.tasks[taskID]
	if !ok {
		return nil
	}
	return ret
}

func (t *schemaChangeTasks) add(task SchemaChangeTask) {
	t.tasks[task.GetTaskID()] = task
	t.taskStats.Add(task.GetTaskID(), task)
}

func (t *schemaChangeTasks) remove(taskID int64) {
	task, ok := t.tasks[taskID]
	if ok {
		delete(t.tasks, taskID)
		t.taskStats.Add(task.GetTaskID(), task)
	}
}

func (t *schemaChangeTasks) listTasks() []SchemaChangeTask {
	return maps.Values(t.tasks)
}

func (t *schemaChangeTasks) listTaskStats() []SchemaChangeTask {
	return t.taskStats.Values()
}

type SchemaChangeTaskFilter func(task SchemaChangeTask) bool

func WithTypeOfSCTask(taskType internalpb.SchemaChangeType) SchemaChangeTaskFilter {
	return func(task SchemaChangeTask) bool {
		return task.GetType() == taskType
	}
}

func WithJobOfSCTask(jobID int64) SchemaChangeTaskFilter {
	return func(task SchemaChangeTask) bool {
		return task.GetJobID() == jobID
	}
}

func WithStatesOfSCTask(states ...internalpb.SchemaChangeState) SchemaChangeTaskFilter {
	return func(task SchemaChangeTask) bool {
		for _, state := range states {
			if task.GetState() == state {
				return true
			}
		}
		return false
	}
}

type UpdateSchemaChangeTaskAction func(task SchemaChangeTask)

func UpdateStateOfSCTask(state internalpb.SchemaChangeState) UpdateSchemaChangeTaskAction {
	return func(t SchemaChangeTask) {
		switch t.GetType() {
		case internalpb.SchemaChangeType_addField:
			t.(*addFieldsTask).State = state
		case internalpb.SchemaChangeType_deleteField:
			// not supported now
		}
	}
}

func UpdateCompleteTimeOfSCTask(completeTime string) UpdateSchemaChangeTaskAction {
	return func(t SchemaChangeTask) {
		switch t.GetType() {
		case internalpb.SchemaChangeType_addField:
			t.(*addFieldsTask).CompleteTime = completeTime
		case internalpb.SchemaChangeType_deleteField:
			// not supported now
		}
	}
}

func UpdateNodeIDOfSCTask(nodeID int64) UpdateSchemaChangeTaskAction {
	return func(t SchemaChangeTask) {
		switch t.GetType() {
		case internalpb.SchemaChangeType_addField:
			t.(*addFieldsTask).NodeID = nodeID
		case internalpb.SchemaChangeType_deleteField:
			// not supported now
		}
	}
}

func UpdateSegmentIDsOfSCTask(segmentIDs []UniqueID) UpdateSchemaChangeTaskAction {
	return func(t SchemaChangeTask) {
		switch t.GetType() {
		case internalpb.SchemaChangeType_addField:
			t.(*addFieldsTask).SegmentIDs = segmentIDs
		case internalpb.SchemaChangeType_deleteField:
			// not supported now
		}
	}
}
