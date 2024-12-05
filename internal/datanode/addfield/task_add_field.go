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
	"context"
	"math"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/samber/lo"
)

type AddFieldTask struct {
	*datapb.AddFieldsTask
	ctx     context.Context
	cancel  context.CancelFunc
	binlogs map[int64]*datapb.AddFieldInfo
	req     *datapb.AddFieldRequest

	allocator  allocator.Interface
	manager    TaskManager
	syncMgr    syncmgr.SyncManager
	cm         storage.ChunkManager
	metaCaches map[string]metacache.MetaCache
}

func NewAddFieldTask(req *datapb.AddFieldRequest,
	manager TaskManager,
	syncMgr syncmgr.SyncManager,
	cm storage.ChunkManager,
) Task {
	ctx, cancel := context.WithCancel(context.Background())
	// Setting end as math.MaxInt64 to incrementally allocate logID.
	// neccessary? seem it's ok to have repeated logid
	alloc := allocator.NewLocalAllocator(req.GetIDRange().GetBegin(), math.MaxInt64)
	task := &AddFieldTask{
		AddFieldsTask: &datapb.AddFieldsTask{
			JobID:        req.GetJobID(),
			TaskID:       req.GetTaskID(),
			CollectionID: req.GetCollectionID(),
			SegmentIDs:   req.GetSegmentIDs(),
			FieldSchemas: req.FieldSchema,
			State:        internalpb.SchemaChangeState_pending,
		},
		ctx:       ctx,
		cancel:    cancel,
		req:       req,
		allocator: alloc,
		manager:   manager,
		syncMgr:   syncMgr,
		cm:        cm,
	}
	metaCaches := make(map[string]metacache.MetaCache)
	for _, channel := range req.GetVchannels() {
		info := &datapb.ChannelWatchInfo{
			Vchan: &datapb.VchannelInfo{
				CollectionID: req.GetCollectionID(),
				ChannelName:  channel,
			},
			Schema: &schemapb.CollectionSchema{
				Fields: req.GetFieldSchema(),
			},
		}
		metaCache := metacache.NewMetaCache(info, func(segment *datapb.SegmentInfo) pkoracle.PkStat {
			return pkoracle.NewBloomFilterSet()
		}, metacache.NoneBm25StatsFactory)
		metaCaches[channel] = metaCache
	}
	task.metaCaches = metaCaches
	return task
}

func (t *AddFieldTask) GetType() internalpb.SchemaChangeType {
	return internalpb.SchemaChangeType_addField
}

func (t *AddFieldTask) GetPartitionIDs() []int64 {
	return t.req.GetPartitionIDs()
}

func (t *AddFieldTask) GetVchannels() []string {
	return t.req.GetVchannels()
}

func (t *AddFieldTask) GetFieldsSchema() []*schemapb.FieldSchema {
	return t.req.GetFieldSchema()
}

func (t *AddFieldTask) GetSlots() int64 {
	return int64(funcutil.Min(len(t.GetSegmentIDs()), paramtable.Get().DataNodeCfg.MaxTaskSlotNum.GetAsInt()))
}

func (t *AddFieldTask) Cancel() {
	t.cancel()
}

func (t *AddFieldTask) Clone() Task {
	ctx, cancel := context.WithCancel(t.ctx)
	infos := make(map[int64]*datapb.AddFieldInfo)
	for id, info := range t.binlogs {
		infos[id] = typeutil.Clone(info)
	}
	return &AddFieldTask{
		AddFieldsTask: typeutil.Clone(t.AddFieldsTask),
		ctx:           ctx,
		cancel:        cancel,
		binlogs:       infos,
		req:           t.req,
		metaCaches:    t.metaCaches,
	}
}

func (t *AddFieldTask) Execute() []*conc.Future[any] {
	log.Info("start to add field")
	t.manager.Update(t.GetTaskID(), UpdateStateOfSCTask(internalpb.SchemaChangeState_progressing))

	req := t.req

	fn := func(idx int) error {
		start := time.Now()
		err := t.addField(idx)
		if err != nil {
			log.Warn("do add field failed")
			t.manager.Update(t.GetTaskID(), UpdateStateOfSCTask(internalpb.SchemaChangeState_failed))
			return err
		}
		log.Info("add field done", zap.Duration("dur", time.Since(start)))
		return nil
	}

	futures := make([]*conc.Future[any], 0, len(req.GetSegmentIDs()))
	for idx := range req.GetSegmentIDs() {
		f := GetExecPool().Submit(func() (any, error) {
			err := fn(idx)
			return err, err
		})
		futures = append(futures, f)
	}
	return futures
}

func (t *AddFieldTask) addField(idx int) error {
	data, err := GenerateInsertData(t.req.GetFieldSchema(), t.req.NumRows[idx])
	if err != nil {
		return err
	}
	syncTask, err := NewSyncTask(t.ctx, t.allocator, t.metaCaches, t.req.GetTs(),
		t.GetSegmentIDs()[idx], t.GetPartitionIDs()[idx], t.GetCollectionID(), t.GetVchannels()[idx], data)
	if err != nil {
		return err
	}
	// update binlogs
	insertBinlogs, _, _ := syncTask.(*syncmgr.SyncTask).Binlogs()
	t.manager.Update(t.GetTaskID(), UpdateBinlogsOfSCTask(t.GetSegmentIDs()[idx], lo.Values(insertBinlogs)))
	return nil
}

func NewSyncTask(ctx context.Context,
	allocator allocator.Interface,
	metaCaches map[string]metacache.MetaCache,
	ts uint64,
	segmentID, partitionID, collectionID int64, vchannel string,
	insertData *storage.InsertData,
) (syncmgr.Task, error) {
	metaCache := metaCaches[vchannel]
	var serializer syncmgr.Serializer
	var err error
	serializer, err = syncmgr.NewStorageSerializer(
		allocator,
		metaCache,
		nil,
	)
	if err != nil {
		return nil, err
	}

	syncPack := &syncmgr.SyncPack{}
	syncPack.WithInsertData([]*storage.InsertData{insertData}).
		WithCollectionID(collectionID).
		WithPartitionID(partitionID).
		WithChannelName(vchannel).
		WithSegmentID(segmentID).
		WithTimeRange(ts, ts).
		WithLevel(datapb.SegmentLevel_L1).
		WithBatchRows(int64(insertData.GetRowNum()))

	return serializer.EncodeFieldBuffer(ctx, syncPack)
}
