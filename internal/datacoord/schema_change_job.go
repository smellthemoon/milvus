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

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

type SchemaChangeJob interface {
	GetType() internalpb.SchemaChangeType
	GetJobID() int64
	GetDbID() int64
	GetCollectionID() int64
	GetPartitionIDs() []int64
	GetSegmentIDs() []int64
	GetFieldSchemas() []*schemapb.FieldSchema
	GetTimeoutTs() uint64
	GetCleanupTs() uint64
	GetState() internalpb.SchemaChangeState
	GetCompleteTime() string
	GetOptions() []*commonpb.KeyValuePair
	GetTR() *timerecord.TimeRecorder
	Clone() SchemaChangeJob
}

type addFieldsJob struct {
	*datapb.AddFieldsJob

	tr *timerecord.TimeRecorder
}

func (j *addFieldsJob) GetType() internalpb.SchemaChangeType {
	return internalpb.SchemaChangeType_addField
}

func (j *addFieldsJob) GetTR() *timerecord.TimeRecorder {
	return j.tr
}

func (j *addFieldsJob) Clone() SchemaChangeJob {
	return &addFieldsJob{
		AddFieldsJob: proto.Clone(j.AddFieldsJob).(*datapb.AddFieldsJob),
		tr:           j.tr,
	}
}

type SchemaChangeJobFilter func(job SchemaChangeJob) bool

func WithCollectionIDOfSCJob(collectionID int64) SchemaChangeJobFilter {
	return func(job SchemaChangeJob) bool {
		return job.GetCollectionID() == collectionID
	}
}

func WithTypeOfSCJob(typ internalpb.SchemaChangeType) SchemaChangeJobFilter {
	return func(job SchemaChangeJob) bool {
		return job.GetType() == typ
	}
}

func WithStatesOfSCJob(states ...internalpb.SchemaChangeState) SchemaChangeJobFilter {
	return func(job SchemaChangeJob) bool {
		for _, state := range states {
			if job.GetState() == state {
				return true
			}
		}
		return false
	}
}

func WithoutStatesOfSCJob(states ...internalpb.SchemaChangeState) SchemaChangeJobFilter {
	return func(job SchemaChangeJob) bool {
		for _, state := range states {
			if job.GetState() == state {
				return false
			}
		}
		return true
	}
}

type UpdateSchemaChangeJobAction func(job SchemaChangeJob)

func UpdateJobStateOfSCJob(state internalpb.SchemaChangeState) UpdateSchemaChangeJobAction {
	return func(job SchemaChangeJob) {
		switch job.GetType() {
		case internalpb.SchemaChangeType_addField:
			job.(*addFieldsJob).State = state
		case internalpb.SchemaChangeType_deleteField:
			// not supported now
		}
		if state == internalpb.SchemaChangeState_completed || state == internalpb.SchemaChangeState_failed {
			// set cleanup ts
			// lxg change params
			dur := Params.DataCoordCfg.ImportTaskRetention.GetAsDuration(time.Second)
			cleanupTime := time.Now().Add(dur)
			cleanupTs := tsoutil.ComposeTSByTime(cleanupTime, 0)
			switch job.GetType() {
			case internalpb.SchemaChangeType_addField:
				job.(*addFieldsJob).CleanupTs = cleanupTs
			case internalpb.SchemaChangeType_deleteField:
				// not supported now
			}
			log.Info("set schema cahnge job cleanup ts", zap.Int64("jobID", job.GetJobID()), zap.String("type", job.GetType().String()),
				zap.Time("cleanupTime", cleanupTime), zap.Uint64("cleanupTs", cleanupTs))
		}
	}
}

func UpdateCompleteTimeOfSCJob(completeTime string) UpdateSchemaChangeJobAction {
	return func(job SchemaChangeJob) {
		switch job.GetType() {
		case internalpb.SchemaChangeType_addField:
			job.(*addFieldsJob).CompleteTime = completeTime
		case internalpb.SchemaChangeType_deleteField:
			// not supported now
		}
	}
}
