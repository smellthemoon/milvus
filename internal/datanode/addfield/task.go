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

package addfield

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util/conc"
)

type FilterOfSCTask func(task Task) bool

func WithStatesOfSCTask(states ...internalpb.SchemaChangeState) FilterOfSCTask {
	return func(task Task) bool {
		for _, state := range states {
			if task.GetState() == state {
				return true
			}
		}
		return false
	}
}

func WithTypeOfSCTask(taskType internalpb.SchemaChangeType) FilterOfSCTask {
	return func(task Task) bool {
		return task.GetType() == taskType
	}
}

type UpdateActionOfSCTask func(task Task)

func UpdateStateOfSCTask(state internalpb.SchemaChangeState) UpdateActionOfSCTask {
	return func(t Task) {
		switch t.GetType() {
		case internalpb.SchemaChangeType_addField:
			t.(*AddFieldTask).State = state
		case internalpb.SchemaChangeType_deleteField:
			// not impl
		}
	}
}

// lxg to do : add reason
//
//	func UpdateReasonOfSCTask(reason string) UpdateActionOfSCTask {
//		return func(t Task) {
//			switch t.GetType() {
//			case PreImportTaskType:
//				t.(*PreImportTask).PreImportTask.Reason = reason
//			case ImportTaskType:
//				t.(*ImportTask).ImportTaskV2.Reason = reason
//			case L0PreImportTaskType:
//				t.(*L0PreImportTask).PreImportTask.Reason = reason
//			case L0ImportTaskType:
//				t.(*L0ImportTask).ImportTaskV2.Reason = reason
//			}
//		}
//	}
func UpdateBinlogsOfSCTask(id int64, binlogs []*datapb.FieldBinlog) UpdateActionOfSCTask {
	return func(t Task) {
		switch t.GetType() {
		case internalpb.SchemaChangeType_addField:
			t.(*AddFieldTask).binlogs[id] = &datapb.AddFieldInfo{
				SegmentID: id,
				Binlogs:   binlogs,
			}
		case internalpb.SchemaChangeType_deleteField:
			// not impl
		}
	}
}

type Task interface {
	Execute() []*conc.Future[any]
	GetJobID() int64
	GetTaskID() int64
	GetCollectionID() int64
	GetPartitionIDs() []int64
	GetVchannels() []string
	GetType() internalpb.SchemaChangeType
	GetState() internalpb.SchemaChangeState
	// GetReason() string
	GetFieldsSchema() []*schemapb.FieldSchema
	GetSlots() int64
	Cancel()
	Clone() Task
}

// func WrapLogFields(task Task, fields ...zap.Field) []zap.Field {
// 	res := []zap.Field{
// 		zap.Int64("taskID", task.GetTaskID()),
// 		zap.Int64("jobID", task.GetJobID()),
// 		zap.Int64("collectionID", task.GetCollectionID()),
// 		zap.String("type", task.GetType().String()),
// 	}
// 	res = append(res, fields...)
// 	return res
// }
