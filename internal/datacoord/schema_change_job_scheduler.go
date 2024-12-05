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
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

type SchemaChangeJobScheduler interface {
	Start()
	Close()
}

type schemaChangeJobScheduler struct {
	meta             *meta
	schemaChangeMeta SchemaChangeMeta
	cluster          Cluster
	alloc            allocator.Allocator
	broker           broker.Broker
	sjm              StatsJobManager

	closeOnce sync.Once
	closeChan chan struct{}
}

func NewSchemaChangeJobScheduler(meta *meta,
	schemaChangeMeta SchemaChangeMeta,
	cluster Cluster,
	alloc allocator.Allocator,
	broker broker.Broker,
	sjm StatsJobManager,
) SchemaChangeJobScheduler {
	return &schemaChangeJobScheduler{
		meta:             meta,
		schemaChangeMeta: schemaChangeMeta,
		cluster:          cluster,
		alloc:            alloc,
		broker:           broker,
		sjm:              sjm,
		closeChan:        make(chan struct{}),
	}
}

func (s *schemaChangeJobScheduler) Start() {
	log.Info("start schema change job scheduler")
	// lxg change it
	var (
		ticker1 = time.NewTicker(Params.DataCoordCfg.ImportCheckIntervalHigh.GetAsDuration(time.Second)) // 2s
		ticker2 = time.NewTicker(Params.DataCoordCfg.ImportCheckIntervalLow.GetAsDuration(time.Second))  // 2min
	)
	defer ticker1.Stop()
	defer ticker2.Stop()
	for {
		select {
		case <-s.closeChan:
			log.Info("schema change job scheduler exited")
			return
		case <-ticker1.C:
			jobs := s.schemaChangeMeta.GetJobBy(context.TODO())
			for _, job := range jobs {
				switch job.GetState() {
				case internalpb.SchemaChangeState_pending:
					s.doPendingJob(job)
				case internalpb.SchemaChangeState_progressing:
					s.doProcessingJob(job)
				case internalpb.SchemaChangeState_completed:
					s.doCompletedJob(job)
				case internalpb.SchemaChangeState_failed:
					s.doFailedJob(job)
				}
			}
		case <-ticker2.C:
			jobs := s.schemaChangeMeta.GetJobBy(context.TODO())
			for _, job := range jobs {
				s.doTimeoutJob(job)
				// s.checkGC(job)
			}
			// add it in cheker
			// jobsByColl := lo.GroupBy(jobs, func(job SchemaChangeJob) int64 {
			// 	return job.GetCollectionID()
			// })
			// for collID, collJobs := range jobsByColl {
			// 	s.checkCollection(collID, collJobs)
			// }
			// s.LogStats()
		}
	}
}

func (s *schemaChangeJobScheduler) Close() {
	s.closeOnce.Do(func() {
		close(s.closeChan)
	})
}

// todo:add it in task
// func (c *schemaChangeJobScheduler) LogStats() {
// 	logFunc := func(tasks []ImportTask, taskType TaskType) {
// 		byState := lo.GroupBy(tasks, func(t ImportTask) datapb.ImportTaskStateV2 {
// 			return t.GetState()
// 		})
// 		pending := len(byState[datapb.ImportTaskStateV2_Pending])
// 		inProgress := len(byState[datapb.ImportTaskStateV2_InProgress])
// 		completed := len(byState[datapb.ImportTaskStateV2_Completed])
// 		failed := len(byState[datapb.ImportTaskStateV2_Failed])
// 		log.Info("import task stats", zap.String("type", taskType.String()),
// 			zap.Int("pending", pending), zap.Int("inProgress", inProgress),
// 			zap.Int("completed", completed), zap.Int("failed", failed))
// 		metrics.ImportTasks.WithLabelValues(taskType.String(), datapb.ImportTaskStateV2_Pending.String()).Set(float64(pending))
// 		metrics.ImportTasks.WithLabelValues(taskType.String(), datapb.ImportTaskStateV2_InProgress.String()).Set(float64(inProgress))
// 		metrics.ImportTasks.WithLabelValues(taskType.String(), datapb.ImportTaskStateV2_Completed.String()).Set(float64(completed))
// 		metrics.ImportTasks.WithLabelValues(taskType.String(), datapb.ImportTaskStateV2_Failed.String()).Set(float64(failed))
// 	}
// 	tasks := s.imeta.GetTaskBy(context.TODO(), WithType(PreImportTaskType))
// 	logFunc(tasks, PreImportTaskType)
// 	tasks = s.imeta.GetTaskBy(context.TODO(), WithType(ImportTaskType))
// 	logFunc(tasks, ImportTaskType)
// }

func (s *schemaChangeJobScheduler) getLackSegmentsForJobs(job SchemaChangeJob) []int64 {
	lacks := lo.KeyBy(job.GetSegmentIDs(), func(id int64) int64 {
		return id
	})
	tasks := s.schemaChangeMeta.GetTaskBy(context.TODO(), WithTypeOfSCTask(internalpb.SchemaChangeType_addField), WithJobOfSCTask(job.GetJobID()))
	for _, task := range tasks {
		for _, id := range task.GetSegmentIDs() {
			delete(lacks, id)
		}
	}
	return lo.Values(lacks)
}

func (s *schemaChangeJobScheduler) doPendingJob(job SchemaChangeJob) {
	log := log.With(zap.Int64("jobID", job.GetJobID()))
	// 0. job 在pending，如果 segment ids 为空，需要生成ids列表，同时检查1,2
	// 1. 断电中断，检查一下所有task的segmentids是不是都跟job一样的
	// 2. 如果不一致，说明1. 写了一半 2. 没开始写，这两种情况都可以继续写
	// 3. 如果ids 不为空，说明是超时塞进来的，不生成ids就好了，同时检查1，2
	// 4. 生成ids之后往schema change meta里面写入
	// 5. 生成task
	if len(job.GetSegmentIDs()) == 0 {
		job.(*addFieldsJob).SegmentIDs = s.meta.GetSegmentsIDOfCollection(context.TODO(), job.GetCollectionID())
	}

	lacks := s.getLackSegmentsForJobs(job)
	if len(lacks) == 0 {
		return
	}
	// lxg change it
	segmentGroups := lo.Chunk(lacks, Params.DataCoordCfg.FilesPerPreImportTask.GetAsInt())

	newTasks, err := NewAddFieldTasks(segmentGroups, job, s.alloc)
	if err != nil {
		log.Warn("new add field tasks failed", zap.Error(err))
		return
	}
	for _, t := range newTasks {
		err = s.schemaChangeMeta.AddTask(context.TODO(), t)
		if err != nil {
			log.Warn("add add field task failed")
			return
		}
		// lxg add it, add more log info
		log.Info("add new preimport task")
	}

	err = s.schemaChangeMeta.UpdateJob(context.TODO(), job.GetJobID(), UpdateJobStateOfSCJob(internalpb.SchemaChangeState_progressing))
	if err != nil {
		log.Warn("failed to update job state to processing", zap.Error(err))
		return
	}
	pendingDuration := job.GetTR().RecordSpan()
	metrics.ImportJobLatency.WithLabelValues(metrics.ImportStagePending).Observe(float64(pendingDuration.Milliseconds()))
	log.Info("add field job start to execute", zap.Duration("jobTimeCost/pending", pendingDuration))
}

func (s *schemaChangeJobScheduler) doProcessingJob(job SchemaChangeJob) {
	log := log.With(zap.Int64("jobID", job.GetJobID()))
	tasks := s.schemaChangeMeta.GetTaskBy(context.TODO(), WithTypeOfSCTask(internalpb.SchemaChangeType_addField), WithJobOfSCTask(job.GetJobID()))
	for _, t := range tasks {
		if t.GetState() != internalpb.SchemaChangeState_completed {
			return
		}
	}
	err := s.schemaChangeMeta.UpdateJob(context.TODO(), job.GetJobID(), UpdateJobStateOfSCJob(internalpb.SchemaChangeState_completed))
	if err != nil {
		log.Warn("failed to update job state to completed", zap.Error(err))
		return
	}
	duration := job.GetTR().RecordSpan()
	// lxg change it
	metrics.ImportJobLatency.WithLabelValues(metrics.ImportStageImport).Observe(float64(duration.Milliseconds()))
	log.Info("add job import done", zap.Duration("jobTimeCost/addField", duration))
}

func (s *schemaChangeJobScheduler) doFailedJob(job SchemaChangeJob) {
	tasks := s.schemaChangeMeta.GetTaskBy(context.TODO(), WithStatesOfSCTask(internalpb.SchemaChangeState_pending, internalpb.SchemaChangeState_progressing, internalpb.SchemaChangeState_completed), WithJobOfSCTask(job.GetJobID()))
	// lxg to do, add job failed reason
	log.Warn("add field job has failed, all tasks with the same jobID will be marked as failed",
		zap.Int64("jobID", job.GetJobID()))
	for _, task := range tasks {
		err := s.schemaChangeMeta.UpdateTask(context.TODO(), task.GetTaskID(), UpdateStateOfSCTask(internalpb.SchemaChangeState_failed))
		if err != nil {
			// lxg to do, add log info
			log.Warn("failed to update import task state to failed")
			continue
		}
	}
}

func (s *schemaChangeJobScheduler) doTimeoutJob(job SchemaChangeJob) {
	timeoutTime := tsoutil.PhysicalTime(job.GetTimeoutTs())
	if time.Now().After(timeoutTime) {
		log.Warn("add field timeout, expired the specified time limit",
			zap.Int64("jobID", job.GetJobID()), zap.Time("timeoutTime", timeoutTime))
		// lxg to do, add job timeout reason
		err := s.schemaChangeMeta.UpdateJob(context.TODO(), job.GetJobID(), UpdateJobStateOfSCJob(internalpb.SchemaChangeState_failed))
		if err != nil {
			log.Warn("failed to update job state to Failed", zap.Int64("jobID", job.GetJobID()), zap.Error(err))
		}
	}
}

func (s *schemaChangeJobScheduler) doCompletedJob(job SchemaChangeJob) {
	// to do ,gc here and failed
}

// // to do ,remove it to util.go or checker

// func (c *schemaChangeJobScheduler) checkCollection(collectionID int64, jobs []SchemaChangeJob) {
// 	if len(jobs) == 0 {
// 		return
// 	}

// 	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
// 	defer cancel()
// 	has, err := c.broker.HasCollection(ctx, collectionID)
// 	if err != nil {
// 		log.Warn("verify existence of collection failed", zap.Int64("collection", collectionID), zap.Error(err))
// 		return
// 	}
// 	if !has {
// 		jobs = lo.Filter(jobs, func(job ImportJob, _ int) bool {
// 			return job.GetState() != internalpb.ImportJobState_Failed
// 		})
// 		for _, job := range jobs {
// 			err = c.imeta.UpdateJob(context.TODO(), job.GexxtJobID(), UpdateJobState(internalpb.ImportJobState_Failed),
// 				UpdateJobReason(fmt.Sprintf("collection %d dropped", collectionID)))
// 			if err != nil {
// 				log.Warn("failed to update job state to Failed", zap.Int64("jobID", job.GetJobID()), zap.Error(err))
// 			}
// 		}
// 	}
// }

// // to do ,remove it to util.go
// func (s *schemaChangeJobScheduler) checkGC(job SchemaChangeJob) {
// 	// 1.在这里remove 掉job的etcd记录checkGC，在task那边 gc掉对应的field binlog记录和etcd记录
// 	// complete
// 	if job.GetState() != internalpb.SchemaChangeState_completed &&
// 		job.GetState() != internalpb.SchemaChangeState_failed {
// 		return
// 	}
// 	cleanupTime := tsoutil.PhysicalTime(job.GetCleanupTs())
// 	if time.Now().After(cleanupTime) {
// 		log := log.With(zap.Int64("jobID", job.GetJobID()))
// 		GCRetention := Params.DataCoordCfg.ImportTaskRetention.GetAsDuration(time.Second)
// 		log.Info("job has reached the GC retention",
// 			zap.Time("cleanupTime", cleanupTime), zap.Duration("GCRetention", GCRetention))
// 		tasks := s.schemaChangeMeta.GetTaskBy(context.TODO(), WithJobOfSCTask(job.GetJobID()))
// 		shouldRemoveJob := true
// 		for _, task := range tasks {
// 			if job.GetState() == internalpb.SchemaChangeState_failed {
// 				if len(task.GetSegmentIDs()) != 0 {
// 					shouldRemoveJob = false
// 					continue
// 				}
// 			}
// 			if task.GetNodeID() != NullNodeID {
// 				shouldRemoveJob = false
// 				continue
// 			}
// 			err := s.imeta.RemoveTask(context.TODO(), task.GetTaskID())
// 			if err != nil {
// 				log.Warn("remove task failed during GC", WrapTaskLog(task, zap.Error(err))...)
// 				shouldRemoveJob = false
// 				continue
// 			}
// 			log.Info("reached GC retention, task removed", WrapTaskLog(task)...)
// 		}
// 		if !shouldRemoveJob {
// 			return
// 		}
// 		err := s.imeta.RemoveJob(context.TODO(), job.GetJobID())
// 		if err != nil {
// 			log.Warn("remove import job failed", zap.Error(err))
// 			return
// 		}
// 		log.Info("import job removed")
// 	}
// }
