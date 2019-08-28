package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	uuid "github.com/satori/go.uuid"
)

func TestNewTaskStat(t *testing.T) {
	stat := NewTaskStat("key", 34.5)
	if !stat.Created.Before(time.Now()) {
		t.Fatal("expected task stat created timestamp to be before now")
	}
	if stat.RunTime != 34.5 {
		t.Fatal("expected task stat runtime to be 34.5")
	}
	if stat.Key != "key" {
		t.Fatal("expected task stat key to be 'key'")
	}
}

func TestTaskStatSave(t *testing.T) {
	testErr := errors.New("test error")
	var table = []struct {
		Stat     *TaskStat
		ModelErr error
	}{
		{NewTaskStat("key", 34.5), nil},
		{NewTaskStat("key", 12.0), testErr},
	}

	for _, tt := range table {
		model := new(MockModel)
		model.On("Save", tt.Stat).Return(DocumentMeta{}, tt.ModelErr).Once()
		if _, err := tt.Stat.Save(model); err != tt.ModelErr {
			t.Fatal(err)
		}
		model.AssertExpectations(t)
	}
}

func TestNewTask(t *testing.T) {
	runAt := time.Now()
	var table = []struct {
		Data     string
		Meta     json.RawMessage
		Priority float64
		Key      string
		RunAt    time.Time
	}{
		{Data: `{"meta": {"id": 123}, "priority": 22.5, "key": "tb1"}`, Meta: []byte(`{"id": 123}`), Priority: 22.5, Key: "tb1"},
		{Data: `{"priority": 22.5, "key": "tb1"}`, Priority: 22.5, Key: "tb1"},
		{Data: fmt.Sprintf(`{"RunAt": "%s", "key": "tb1"}`, runAt.Format(time.RFC3339)), RunAt: runAt, Key: "tb1"},
	}

	for _, tt := range table {
		task := NewTask([]byte(tt.Data))
		id, err := uuid.FromString(task.Id)
		if err != nil {
			t.Fatal(err)
		}
		if id.Version() != 1 {
			t.Fatal("expected task id to be uuid version 1")
		}
		if task.Priority != tt.Priority {
			t.Fatalf("expected task priority to be %f", tt.Priority)
		}
		if task.Key != tt.Key {
			t.Fatalf("expected task key to be %s", tt.Key)
		}
		if !task.Created.Before(time.Now()) {
			t.Fatal("expected task created time to be in the past")
		}
		if task.RunAt != nil && task.RunAt.Format(time.RFC3339) != tt.RunAt.Format(time.RFC3339) {
			t.Fatalf("expected task run at time to be %v", tt.RunAt)
		}
		if task.Meta != nil {
			if string(task.Meta) != string(tt.Meta) {
				t.Fatalf("expected task meta to be %s", string(tt.Meta))
			}
		}
	}
}

func TestTaskSave(t *testing.T) {
	testErr := errors.New("test error")
	var table = []struct {
		Task     *Task
		ModelErr error
	}{
		{NewTask([]byte(`{"key": "tb1", "priority": 13141.42}`)), nil},
		{NewTask([]byte(`{"key": "tb1", "priority": 13141.42}`)), testErr},
	}
	for _, tt := range table {
		model := new(MockModel)
		model.On("Save", tt.Task).Return(DocumentMeta{}, tt.ModelErr).Once()
		if _, err := tt.Task.Save(model); err != tt.ModelErr {
			t.Fatal(err)
		}
		model.AssertExpectations(t)
	}
}

func TestTaskChangeStatus(t *testing.T) {
	taskErr := errors.New("test error")
	var table = []struct {
		Status   string
		ModelErr error
	}{
		{StatusQueued, nil},
		{StatusComplete, nil},
		{StatusPending, taskErr},
	}

	task := NewTask([]byte(""))
	for _, tt := range table {
		model := new(MockModel)
		model.On("Save", task).Return(DocumentMeta{}, tt.ModelErr).Once()
		if err := task.ChangeStatus(model, tt.Status); err != tt.ModelErr {
			t.Fatal(err)
		}
		if task.Status != tt.Status {
			t.Fatalf("expected task status to be %s, got %s", tt.Status, task.Status)
		}
		model.AssertExpectations(t)
	}
}

func TestTaskGetAverageRunTime(t *testing.T) {
	testErr := errors.New("test error")
	var table = []struct {
		Key       string
		TaskStats []interface{}
		RunTime   float64
		ModelErr  error
	}{
		{
			"my-task",
			[]interface{}{},
			0.0,
			nil,
		},
		{
			"my-task",
			[]interface{}{
				NewTaskStat("my-task", 1.0),
				NewTaskStat("my-task", 3.0),
				NewTaskStat("my-task", 1.0),
			},
			1.65,
			nil,
		},
		{
			"my-task",
			[]interface{}{
				NewTaskStat("my-task", 1.0),
				NewTaskStat("my-task", 1.0),
				NewTaskStat("my-task", 5.0),
				NewTaskStat("my-task", 1.0),
				NewTaskStat("my-task", 3.0),
				NewTaskStat("my-task", 1.0),
				NewTaskStat("my-task", 7.0),
				NewTaskStat("my-task", 1.0),
				NewTaskStat("my-task", 2.0),
				NewTaskStat("my-task", 1.0),
			},
			2.3,
			nil,
		},
		{
			"my-task",
			[]interface{}{},
			-1,
			testErr,
		},
	}
	q := fmt.Sprintf(
		"FOR t IN %s FILTER t.key == @key SORT t.created DESC LIMIT 10 RETURN t",
		CollectionTaskStats,
	)
	task := NewTask([]byte(`{"key": "my-task"}`))
	for _, tt := range table {
		model := new(MockModel)
		model.On("Query", q, map[string]interface{}{"key": tt.Key}).Return(tt.TaskStats, tt.ModelErr)
		runTime, err := task.GetAverageRunTime(model)
		if err != tt.ModelErr {
			t.Fatal(err)
		}
		if runTime != tt.RunTime {
			t.Fatalf("expected runtime to be %f, got %f", tt.RunTime, runTime)
		}
		model.AssertExpectations(t)
	}
}
