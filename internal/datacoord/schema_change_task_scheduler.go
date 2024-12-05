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
	"context"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/samber/lo"
	"go.uber.org/zap"
)

type SchemaChangeTaskScheduler interface {
	Start()
	Close()
}

type schemaChangeTaskScheduler struct {
	meta             *meta
	schemaChangeMeta SchemaChangeMeta
	cluster          Cluster
	alloc            allocator.Allocator

	closeOnce sync.Once
	closeChan chan struct{}
}

func NewSchemaChangeTaskScheduler(meta *meta,
	schemaChangeMeta SchemaChangeMeta,
	cluster Cluster,
	alloc allocator.Allocator,
) SchemaChangeTaskScheduler {
	return &schemaChangeTaskScheduler{
		meta:             meta,
		schemaChangeMeta: schemaChangeMeta,
		cluster:          cluster,
		alloc:            alloc,
		closeChan:        make(chan struct{}),
	}
}

func (s *schemaChangeTaskScheduler) Start() {
	// pending need assign， 需要get slot一下，然后分配节点
	//
	log.Info("start schema change task scheduler")
	ticker := time.NewTicker(Params.DataCoordCfg.ImportScheduleInterval.GetAsDuration(time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-s.closeChan:
			log.Info("schema change task scheduler exited")
			return
		case <-ticker.C:
			jobs := s.schemaChangeMeta.GetJobBy(context.TODO())
			sort.Slice(jobs, func(i, j int) bool {
				return jobs[i].GetJobID() < jobs[j].GetJobID()
			})
			nodeSlots := s.querySlots()
			for _, job := range jobs {
				tasks := s.schemaChangeMeta.GetTaskBy(context.TODO(), WithJobOfSCTask(job.GetJobID()))
				for _, task := range tasks {
					switch job.GetState() {
					case internalpb.SchemaChangeState_pending:
						nodeID := s.selectNode(task, nodeSlots)
						s.doPendingTask(task, nodeID)
					case internalpb.SchemaChangeState_progressing:
						s.doProcessingTask(task)
					case internalpb.SchemaChangeState_completed:
						s.doCompletedTask(task)
					case internalpb.SchemaChangeState_failed:
						s.doFailedTask(task)
					}
				}
			}
		}
	}
}

func (s *schemaChangeTaskScheduler) selectNode(task SchemaChangeTask, nodeSlots map[int64]int64) int64 {
	var (
		nodeID   int64 = NullNodeID
		maxSlots int64 = -1
	)
	require := task.GetSlotUsage()
	for id, slots := range nodeSlots {
		// find the most idle datanode
		if slots > 0 && slots >= require && slots > maxSlots {
			nodeID = id
			maxSlots = slots
		}
	}
	if nodeID != NullNodeID {
		nodeSlots[nodeID] -= require
	}
	return nodeID
}

func (s *schemaChangeTaskScheduler) querySlots() map[int64]int64 {
	nodeIDs := lo.Map(s.cluster.GetSessions(), func(s *session.Session, _ int) int64 {
		return s.NodeID()
	})
	nodeSlots := make(map[int64]int64)
	mu := &lock.Mutex{}
	wg := &sync.WaitGroup{}
	for _, nodeID := range nodeIDs {
		wg.Add(1)
		go func(nodeID int64) {
			defer wg.Done()
			resp, err := s.cluster.QueryAddField(nodeID, &datapb.QueryAddFieldRequest{QuerySlot: true})
			if err != nil {
				log.Warn("query add field failed", zap.Error(err))
				return
			}
			mu.Lock()
			defer mu.Unlock()
			nodeSlots[nodeID] = resp.GetSlots()
		}(nodeID)
	}
	wg.Wait()
	log.Debug("peek slots done", zap.Any("nodeSlots", nodeSlots))
	return nodeSlots
}

func (s *schemaChangeTaskScheduler) doPendingTask(task SchemaChangeTask, nodeID int64) {
	if nodeID == NullNodeID {
		return
	}
	log.Info("do pending add field task...")
	job := s.schemaChangeMeta.GetJob(context.TODO(), task.GetJobID())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ts, err := s.alloc.AllocTimestamp(ctx)
	if err != nil {
		log.Warn("alloc timestamp failed when do pending add field task")
	}
	req := &datapb.AddFieldRequest{
		JobID:        task.GetJobID(),
		TaskID:       task.GetTaskID(),
		CollectionID: task.GetCollectionID(),
		PartitionIDs: job.GetPartitionIDs(),
		FieldSchema:  job.GetFieldSchemas(),
		Options:      job.GetOptions(),
		Ts:           ts,
	}
	err = s.cluster.AddField(nodeID, req)
	if err != nil {
		log.Warn("add field failed")
		return
	}
	err = s.schemaChangeMeta.UpdateTask(context.TODO(), task.GetTaskID(),
		UpdateStateOfSCTask(internalpb.SchemaChangeState_progressing),
		UpdateNodeIDOfSCTask(nodeID))
	if err != nil {
		log.Warn("update add field task failed")
		return
	}
	pendingDuration := task.GetTR().RecordSpan()
	metrics.ImportTaskLatency.WithLabelValues(metrics.ImportStagePending).Observe(float64(pendingDuration.Milliseconds()))
	log.Info("add field task start to execute")
}

func (s *schemaChangeTaskScheduler) doProcessingTask(task SchemaChangeTask) {
	req := &datapb.QueryAddFieldRequest{
		JobID:  task.GetJobID(),
		TaskID: task.GetTaskID(),
	}
	resp, err := s.cluster.QueryAddField(task.GetNodeID(), req)
	// task if failed, will only change the state
	if err != nil {
		// lxg todo: check err
		updateErr := s.schemaChangeMeta.UpdateTask(context.TODO(), task.GetTaskID(), UpdateStateOfSCTask(internalpb.SchemaChangeState_pending))
		if updateErr != nil {
			log.Warn("failed to update import task state to pending")
		}
		log.Info("reset import task state to pending due to error occurs")
		return
	}
	if resp.GetState() == internalpb.SchemaChangeState_failed {
		// todo:may need add reason here?
		err = s.schemaChangeMeta.UpdateJob(context.TODO(), task.GetJobID(), UpdateJobStateOfSCJob(internalpb.SchemaChangeState_failed))
		if err != nil {
			log.Warn("failed to update job state to Failed", zap.Int64("jobID", task.GetJobID()), zap.Error(err))
		}
		log.Warn("add field task failed")
		return
	}

	// seem no need to do that
	// for _, info := range resp.GetImportSegmentsInfo() {
	// 	segment := s.meta.GetSegment(context.TODO(), info.GetSegmentID())
	// 	if info.GetImportedRows() <= segment.GetNumOfRows() {
	// 		continue // rows not changed, no need to update
	// 	}
	// 	diff := info.GetImportedRows() - segment.GetNumOfRows()
	// 	op := UpdateImportedRows(info.GetSegmentID(), info.GetImportedRows())
	// 	err = s.meta.UpdateSegmentsInfo(context.TODO(), op)
	// 	if err != nil {
	// 		log.Warn("update import segment rows failed", WrapTaskLog(task, zap.Error(err))...)
	// 		return
	// 	}

	// 	metrics.DataCoordBulkVectors.WithLabelValues(
	// 		dbName,
	// 		strconv.FormatInt(task.GetCollectionID(), 10),
	// 	).Add(float64(diff))
	// }
	// to do: get updated binlog from datanode
	// 1. try to parse path and fill logID
	// 2. update it
	if resp.GetState() == internalpb.SchemaChangeState_failed {
		completeTime := time.Now().Format("2006-01-02T15:04:05Z07:00")
		err = s.schemaChangeMeta.UpdateTask(context.TODO(), task.GetTaskID(), UpdateStateOfSCTask(internalpb.SchemaChangeState_completed), UpdateCompleteTimeOfSCTask(completeTime))
		if err != nil {
			log.Warn("update import task failed")
			return
		}
		importDuration := task.GetTR().RecordSpan()
		metrics.ImportTaskLatency.WithLabelValues(metrics.ImportStageImport).Observe(float64(importDuration.Milliseconds()))
		log.Info("add field to complete done")
	}
}

func (s *schemaChangeTaskScheduler) doCompletedTask(task SchemaChangeTask) {
	// 预期task.GetNodeID() == NullNodeID的时候不能为complete？
	// and gc here

	req := &datapb.DropAddFieldRequest{
		JobID:  task.GetJobID(),
		TaskID: task.GetTaskID(),
	}
	err := s.cluster.DropAddField(task.GetNodeID(), req)
	if err != nil && !errors.Is(err, merr.ErrNodeNotFound) {
		log.Warn("drop add field failed", zap.Error(err))
	}
	log.Info("drop add field in datanode done")
	err = s.schemaChangeMeta.RemoveTask(context.TODO(), task.GetTaskID())
	if err != nil && !errors.Is(err, merr.ErrNodeNotFound) {
		log.Warn("drop add field failed", zap.Error(err))
	}
}

func (s *schemaChangeTaskScheduler) doFailedTask(task SchemaChangeTask) {
	// drop and gc
	// and maybe retry?
}

func (s *schemaChangeTaskScheduler) Close() {
	s.closeOnce.Do(func() {
		close(s.closeChan)
	})
}
