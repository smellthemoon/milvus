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

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

type SchemaChangeMeta interface {
	AddJob(ctx context.Context, job SchemaChangeJob) error
	UpdateJob(ctx context.Context, jobID int64, actions ...UpdateSchemaChangeJobAction) error
	GetJob(ctx context.Context, jobID int64) SchemaChangeJob
	GetJobBy(ctx context.Context, filters ...SchemaChangeJobFilter) []SchemaChangeJob
	CountJobBy(ctx context.Context, filters ...SchemaChangeJobFilter) int
	RemoveJob(ctx context.Context, jobID int64) error

	AddTask(ctx context.Context, task SchemaChangeTask) error
	UpdateTask(ctx context.Context, taskID int64, actions ...UpdateSchemaChangeTaskAction) error
	GetTask(ctx context.Context, taskID int64) SchemaChangeTask
	GetTaskBy(ctx context.Context, filters ...SchemaChangeTaskFilter) []SchemaChangeTask
	RemoveTask(ctx context.Context, taskID int64) error
	TaskStatsJSON(ctx context.Context) string
}

type schemaChangeMeta struct {
	mu      lock.RWMutex // guards jobs and tasks
	jobs    map[int64]SchemaChangeJob
	tasks   *schemaChangeTasks
	catalog metastore.DataCoordCatalog
}

func NewSchemaChangeMeta(ctx context.Context, catalog metastore.DataCoordCatalog) (SchemaChangeMeta, error) {
	addFieldsTasks, err := catalog.ListAddFieldsTasks(ctx)
	if err != nil {
		return nil, err
	}
	addFieldsJobs, err := catalog.ListAddFieldsJobs(ctx)
	if err != nil {
		return nil, err
	}

	tasks := newSchemaChangeTasks()

	for _, task := range addFieldsTasks {
		tasks.add(&addFieldsTask{
			AddFieldsTask: task,
			tr:            timerecord.NewTimeRecorder("add field task"),
		})
	}

	jobs := make(map[int64]SchemaChangeJob)
	for _, job := range addFieldsJobs {
		jobs[job.GetJobID()] = &addFieldsJob{
			AddFieldsJob: job,
			tr:           timerecord.NewTimeRecorder("add field job"),
		}
	}

	return &schemaChangeMeta{
		jobs:    jobs,
		tasks:   tasks,
		catalog: catalog,
	}, nil
}

func (m *schemaChangeMeta) AddJob(ctx context.Context, job SchemaChangeJob) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var err error
	switch job.GetType() {
	case internalpb.SchemaChangeType_addField:
		err = m.catalog.SaveAddFieldsJob(ctx, job.(*addFieldsJob).AddFieldsJob)
	case internalpb.SchemaChangeType_deleteField:
		err = merr.WrapErrOperationNotSupported("DeleteField is not supported now")
	}
	if err != nil {
		return err
	}
	m.jobs[job.GetJobID()] = job
	return nil
}

func (m *schemaChangeMeta) UpdateJob(ctx context.Context, jobID int64, actions ...UpdateSchemaChangeJobAction) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if job, ok := m.jobs[jobID]; ok {
		updatedJob := job.Clone()
		for _, action := range actions {
			action(updatedJob)
		}
		var err error
		switch job.GetType() {
		case internalpb.SchemaChangeType_addField:
			err = m.catalog.SaveAddFieldsJob(ctx, job.(*addFieldsJob).AddFieldsJob)
		case internalpb.SchemaChangeType_deleteField:
			err = merr.WrapErrOperationNotSupported("DeleteField is not supported now")
		}
		if err != nil {
			return err
		}
		m.jobs[updatedJob.GetJobID()] = updatedJob
	}
	return nil
}

func (m *schemaChangeMeta) GetJob(ctx context.Context, jobID int64) SchemaChangeJob {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.jobs[jobID]
}

func (m *schemaChangeMeta) GetJobBy(ctx context.Context, filters ...SchemaChangeJobFilter) []SchemaChangeJob {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.getJobBy(filters...)
}

func (m *schemaChangeMeta) getJobBy(filters ...SchemaChangeJobFilter) []SchemaChangeJob {
	ret := make([]SchemaChangeJob, 0)
OUTER:
	for _, job := range m.jobs {
		for _, f := range filters {
			if !f(job) {
				continue OUTER
			}
		}
		ret = append(ret, job)
	}
	return ret
}

func (m *schemaChangeMeta) CountJobBy(ctx context.Context, filters ...SchemaChangeJobFilter) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.getJobBy(filters...))
}

func (m *schemaChangeMeta) RemoveJob(ctx context.Context, jobID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if job, ok := m.jobs[jobID]; ok {
		var err error
		switch job.GetType() {
		case internalpb.SchemaChangeType_addField:
			err = m.catalog.DropAddFieldsJob(ctx, job.GetJobID())
		case internalpb.SchemaChangeType_deleteField:
			err = merr.WrapErrOperationNotSupported("DeleteField is not supported now")
		}
		if err != nil {
			return err
		}
		delete(m.jobs, jobID)
	}
	return nil
}

func (m *schemaChangeMeta) AddTask(ctx context.Context, task SchemaChangeTask) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var err error
	switch task.GetType() {
	case internalpb.SchemaChangeType_addField:
		err = m.catalog.SaveAddFieldsTask(ctx, task.(*addFieldsTask).AddFieldsTask)
	case internalpb.SchemaChangeType_deleteField:
		err = merr.WrapErrOperationNotSupported("DeleteField is not supported now")
	}
	if err != nil {
		return err
	}
	m.tasks.add(task)
	return nil
}

func (m *schemaChangeMeta) UpdateTask(ctx context.Context, taskID int64, actions ...UpdateSchemaChangeTaskAction) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var err error
	if task := m.tasks.get(taskID); task != nil {
		updatedTask := task.Clone()
		for _, action := range actions {
			action(updatedTask)
		}
		switch task.GetType() {
		case internalpb.SchemaChangeType_addField:
			err = m.catalog.SaveAddFieldsTask(ctx, updatedTask.(*addFieldsTask).AddFieldsTask)
		case internalpb.SchemaChangeType_deleteField:
			err = merr.WrapErrOperationNotSupported("DeleteField is not supported now")
		}
		if err != nil {
			return err
		}
		m.tasks.add(task)
	}
	return nil
}

func (m *schemaChangeMeta) GetTask(ctx context.Context, taskID int64) SchemaChangeTask {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tasks.get(taskID)
}

func (m *schemaChangeMeta) GetTaskBy(ctx context.Context, filters ...SchemaChangeTaskFilter) []SchemaChangeTask {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ret := make([]SchemaChangeTask, 0)
OUTER:
	for _, task := range m.tasks.listTasks() {
		for _, f := range filters {
			if !f(task) {
				continue OUTER
			}
		}
		ret = append(ret, task)
	}
	return ret
}

func (m *schemaChangeMeta) RemoveTask(ctx context.Context, taskID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var err error
	if task := m.tasks.get(taskID); task != nil {
		switch task.GetType() {
		case internalpb.SchemaChangeType_addField:
			err = m.catalog.DropAddFieldsTask(ctx, taskID)
		case internalpb.SchemaChangeType_deleteField:
			err = merr.WrapErrOperationNotSupported("DeleteField is not supported now")
		}
		if err != nil {
			return err
		}
		m.tasks.remove(taskID)
	}
	return nil
}

func (m *schemaChangeMeta) TaskStatsJSON(ctx context.Context) string {
	tasks := m.tasks.listTaskStats()

	ret, err := json.Marshal(tasks)
	if err != nil {
		return ""
	}
	return string(ret)
}
