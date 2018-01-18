package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/bitwurx/jrpc2"
)

func TestNewEvent(t *testing.T) {
	evt := NewEvent("test", []byte(`{"id": 123}`))
	if evt.Kind != "test" {
		t.Fatal("expected event type to be 'test'")
	}
}

type AddMethodParams struct {
	X *int `json:"x"`
	Y *int `json:"y"`
}

func (p *AddMethodParams) FromPositional(params []interface{}) error {
	x := params[0].(int)
	y := params[1].(int)
	p.X = &x
	p.Y = &y

	return nil
}

func AddMethod(params json.RawMessage) (interface{}, *jrpc2.ErrorObject) {
	p := new(AddMethodParams)
	if err := jrpc2.ParseParams(params, p); err != nil {
		log.Fatal(err)
	}
	if p.X == nil || p.Y == nil {
		return nil, &jrpc2.ErrorObject{
			Code:    jrpc2.InvalidParamsCode,
			Message: jrpc2.InvalidParamsMsg,
			Data:    "exactly two integers are required",
		}
	}
	return (*p.X + *p.Y), nil
}

func TestServiceBrokerCall(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	var wg sync.WaitGroup
	s := jrpc2.NewServer(":7777", "/rpc")
	s.Register("add", jrpc2.Method{Method: AddMethod})
	go s.Start()
	wg.Add(1)
	go func() {
		for {
			var buf bytes.Buffer
			buf.Write([]byte(""))
			_, err := http.Post("http://127.0.0.1:7777/rpc", "application/json", &buf)
			if err != nil {
				time.Sleep(time.Second)
				continue
			}
			break
		}
		wg.Done()
	}()
	wg.Wait()

	brkCallErrCode := BrokerCallErrorCode
	invPararmCode := jrpc2.InvalidParamsCode
	var table = []struct {
		Method  string
		Params  map[string]interface{}
		Url     string
		Result  interface{}
		ErrCode *jrpc2.ErrorCode
	}{
		{"add", map[string]interface{}{"x": 3, "y": 0}, "127.0.0.1:32050", nil, &brkCallErrCode},
		{"add", map[string]interface{}{"x": 3, "y": 5}, "127.0.0.1:7777", 8, nil},
		{"add", map[string]interface{}{"x": 3}, "127.0.0.1:7777", nil, &invPararmCode},
	}

	for _, tt := range table {
		broker := &JsonRPCServiceBroker{}
		result, errObj := broker.Call(tt.Url, tt.Method, tt.Params)
		if errObj != nil {
			if tt.ErrCode == nil {
				t.Fatal(errObj)
			}
			if tt.ErrCode != nil && errObj.Code != *tt.ErrCode {
				t.Fatalf("invalid error code. expected %v, got %v", *tt.ErrCode, errObj.Code)
			}
		}
		if result == nil && tt.Result != nil {
			t.Fatal("expected result but got nil")
		}
		if result != nil && int(result.(float64)) != tt.Result {
			t.Fatalf("invalid result. expected %f, got %f", result, tt.Result)
		}
	}
}

func TestControllerAddTask(t *testing.T) {
	var table = []struct {
		Task      *Task
		Result    float64
		Status    TaskStatus
		BrokerErr *jrpc2.ErrorObject
		Model     bool
		ModelErr  error
		Err       error
	}{
		{
			NewTask([]byte(`{"key": "test123", priority": 12.3}`)),
			0,
			StatusQueued,
			nil,
			true,
			nil,
			nil,
		},
		{
			NewTask([]byte(fmt.Sprintf(`{"key": "test123", "runAt": "%s"}`, time.Now().Format(time.RFC3339)))),
			0,
			StatusScheduled,
			nil,
			true,
			nil,
			nil,
		},
		{
			NewTask([]byte(fmt.Sprintf(`{"key": "test123", runAt": %s}`, time.Now().Format(time.RFC3339)))),
			-1,
			StatusPending,
			nil,
			false,
			nil,
			TaskAddFailedError,
		},
		{
			NewTask([]byte(`{"key": "test123", priority": 12.3}`)),
			-1,
			StatusPending,
			&jrpc2.ErrorObject{Message: "broker error"},
			false,
			nil,
			errors.New("broker error"),
		},
		{
			NewTask([]byte(`{"key": "test123", priority": 12.3}`)),
			0,
			StatusQueued,
			nil,
			true,
			errors.New("model error"),
			errors.New("model error"),
		},
	}

	for _, tt := range table {
		var model *MockModel
		broker := new(MockServiceBroker)
		params := map[string]interface{}{"key": tt.Task.Key, "id": tt.Task.Id}
		if tt.Task.RunAt != nil {
			params["runAt"] = tt.Task.RunAt.Format(time.RFC3339)
			broker.On("Call", TimetableHost, "insert", params).Return(tt.Result, tt.BrokerErr).Once()
		} else {
			params["priority"] = tt.Task.Priority
			broker.On("Call", PriorityQueueHost, "push", params).Return(tt.Result, tt.BrokerErr).Once()
		}
		if tt.Model {
			model = new(MockModel)
			model.On("Save", tt.Task).Return(DocumentMeta{}, tt.ModelErr)
		}
		ctrl := NewResourceController(broker)
		if err := ctrl.AddTask(tt.Task, model); err != nil && err.Error() != tt.Err.Error() {
			t.Fatal(err)
		}
		if tt.Task.Status != tt.Status {
			t.Fatalf("expected task status to be %d, got %d", tt.Status, tt.Task.Status)
		}
		broker.AssertExpectations(t)

		if tt.Model {
			model.AssertExpectations(t)
		}
	}
}

func TestControllerAddResource(t *testing.T) {
	var table = []struct {
		Name     string
		ModelErr error
	}{
		{"test", nil},
		{"test", errors.New("model error")},
	}

	for _, tt := range table {
		model := &MockModel{}
		model.On("Save", NewResource(tt.Name)).Return(DocumentMeta{}, tt.ModelErr)
		ctrl := NewResourceController(nil)
		if err := ctrl.AddResource(tt.Name, model); err != nil && err.Error() != tt.ModelErr.Error() {
			t.Fatal(err)
		}
		if err := ctrl.AddResource(tt.Name, model); err != ResourceExistsError {
			t.Fatal("expected resource exists error")
		}
		if _, ok := ctrl.resources[tt.Name]; !ok {
			t.Fatalf("expected resource with '%s' to have been added", tt.Name)
		}
	}

}

func TestControllerNotify(t *testing.T) {
	var table = []struct {
		Evt       *Event
		Url       string
		Result    float64
		BrokerErr *jrpc2.ErrorObject
		Err       error
	}{
		{
			NewEvent("taskStatusChanged", []byte(`{"status": 1}`)),
			StatusChangeNotifierHost,
			0,
			nil,
			nil,
		},
		{
			NewEvent("taskStatusChanged", []byte(`{"status": 1}`)),
			StatusChangeNotifierHost,
			-1,
			nil,
			NotificationFailedError,
		},
		{
			NewEvent("taskStatusChanged", []byte(`{"status": 1}`)),
			StatusChangeNotifierHost,
			-1,
			&jrpc2.ErrorObject{Message: "an error occured"},
			errors.New("an error occured"),
		},
	}

	for _, tt := range table {
		params := map[string]interface{}{"created": tt.Evt.Created, "kind": tt.Evt.Kind, "meta": tt.Evt.Meta}
		broker := new(MockServiceBroker)
		broker.On("Call", tt.Url, "notify", params).Return(tt.Result, tt.BrokerErr).Once()
		ctrl := NewResourceController(broker)
		if err := ctrl.Notify(tt.Evt); err != nil && err.Error() != tt.Err.Error() {
			t.Fatal(err)
		}
		broker.AssertExpectations(t)
	}
}

func TestControllerGetTask(t *testing.T) {
	task := NewTask([]byte(`{"key": "test123", priority": 12.3}`))
	var table = []struct {
		TaskId   string
		Tasks    []interface{}
		ModelErr error
		Err      error
	}{
		{
			task.Id,
			[]interface{}{task},
			nil,
			nil,
		},
		{
			"abc123",
			nil,
			errors.New("query error"),
			errors.New("query error"),
		},
		{
			"abc123",
			make([]interface{}, 0),
			nil,
			TaskNotFoundError,
		},
	}

	for _, tt := range table {
		q := fmt.Sprintf(`FOR t IN %s FILTER t._key == @key RETURN t`, CollectionTasks)
		ctrl := NewResourceController(nil)
		model := new(MockModel)
		model.On("Query", q, map[string]interface{}{"key": tt.TaskId}).Return(tt.Tasks, tt.ModelErr).Once()
		task, err := ctrl.GetTask(tt.TaskId, model)
		if err != nil && err.Error() != tt.Err.Error() {
			t.Fatal(err)
		}
		if task != nil && task.Id != tt.TaskId {
			t.Fatalf("expected task key to be %s, got %s", task.Id, tt.TaskId)
		}
		model.AssertExpectations(t)
	}
}

func TestControllerListTimetable(t *testing.T) {
	var table = []struct {
		Key       string
		Result    json.RawMessage
		BrokerErr *jrpc2.ErrorObject
		Err       error
	}{
		{
			"test",
			[]byte(`{"_key":"test","schedule":[{"_key":"test","runAt":"01-01-2017T12:00:00Z"}]}`),
			nil,
			nil,
		},
		{
			"test",
			nil,
			&jrpc2.ErrorObject{Code: -32002, Message: "Timetable not found"},
			TimetableNotFound,
		},
	}

	for _, tt := range table {
		params := map[string]interface{}{"key": tt.Key}
		broker := new(MockServiceBroker)
		broker.On("Call", TimetableHost, "get", params).Return(tt.Result, tt.BrokerErr)
		ctrl := NewResourceController(broker)
		list, err := ctrl.ListTimetable(tt.Key)
		if err != nil && err.Error() != tt.Err.Error() {
			t.Fatal(err)
		}
		if list != nil && string(list) != string(tt.Result) {
			t.Fatalf("expected list to be %s, got %s", tt.Result, list)
		}
		broker.AssertExpectations(t)
	}
}

func TestControllerListPriorityQueue(t *testing.T) {
	var table = []struct {
		Key       string
		Result    json.RawMessage
		BrokerErr *jrpc2.ErrorObject
		Err       error
	}{
		{
			"test",
			[]byte(`{"_key":"test","count":1,"heap":[{"_key":"test","priority":2.4}]}`),
			nil,
			nil,
		},
		{
			"test",
			nil,
			&jrpc2.ErrorObject{Code: 32002, Message: "Queue not found"},
			QueueNotFoundError,
		},
	}

	for _, tt := range table {
		params := map[string]interface{}{"key": tt.Key}
		broker := new(MockServiceBroker)
		broker.On("Call", PriorityQueueHost, "get", params).Return(tt.Result, tt.BrokerErr)
		ctrl := NewResourceController(broker)
		list, err := ctrl.ListPriorityQueue(tt.Key)
		if err != nil && err.Error() != tt.Err.Error() {
			t.Fatal(err)
		}
		if list != nil && string(list) != string(tt.Result) {
			t.Fatalf("expected list to be %s, got %s", tt.Result, list)
		}
		broker.AssertExpectations(t)
	}
}

func TestControllerStartTask(t *testing.T) {
	modelErr := errors.New("model error")
	var table = []struct {
		Key            string
		Task           *Task
		Resource       *Resource
		Err            error
		Model          bool
		ModelErr       error
		TaskStatus     TaskStatus
		ResourceStatus ResourceStatus
	}{
		{
			"test",
			&Task{Status: StatusPending},
			&Resource{Name: "test", Status: ResourceFree},
			nil,
			true,
			nil,
			StatusStarted,
			ResourceLocked,
		},
		{
			"test",
			&Task{Status: StatusPending},
			&Resource{Name: "test", Status: ResourceFree},
			modelErr,
			true,
			modelErr,
			StatusStarted,
			ResourceLocked,
		},
		{
			"test",
			nil,
			nil,
			NoStagedTaskError,
			false,
			nil,
			StatusScheduled,
			ResourceFree,
		},
		{
			"test",
			&Task{Status: StatusStarted},
			&Resource{Name: "test", Status: ResourceLocked},
			TaskAlreadyStartedError,
			false,
			nil,
			StatusStarted,
			ResourceLocked,
		},
		{
			"test",
			&Task{Status: StatusPending},
			&Resource{Name: "test", Status: ResourceLocked},
			ResourceUnavailableError,
			false,
			nil,
			StatusPending,
			ResourceLocked,
		},
	}

	for i, tt := range table {
		ctrl := NewResourceController(nil)
		if tt.Task != nil {
			ctrl.stage[tt.Key] = tt.Task
		}
		if tt.Resource != nil {
			ctrl.resources[tt.Key] = tt.Resource
		}
		model := &MockModel{}
		if tt.Model {
			model.On("Save", tt.Task).Return(DocumentMeta{}, tt.ModelErr)
		}
		if err := ctrl.StartTask(tt.Key, model); err != nil && err != tt.Err {
			t.Fatal(err)
		}
		if ctrl.resources[tt.Key] != nil && ctrl.resources[tt.Key].Status != tt.ResourceStatus {
			t.Fatalf("[%d] expected resource status %d, got %d", i, tt.ResourceStatus, ctrl.resources[tt.Key].Status)
		}
		if ctrl.stage[tt.Key] != nil && ctrl.stage[tt.Key].Status != tt.TaskStatus {
			t.Fatalf("[%d] expected task status %d, got %d", i, tt.TaskStatus, ctrl.stage[tt.Key].Status)
		}
		if tt.Model {
			model.AssertExpectations(t)
		}
	}
}

func TestControllerCompleteTask(t *testing.T) {
	modelErr := errors.New("model error")
	var table = []struct {
		Key            string
		Task           *Task
		Resource       *Resource
		Err            error
		Model          bool
		ModelErr       error
		TaskStatus     TaskStatus
		ResourceStatus ResourceStatus
	}{
		{
			"test",
			&Task{Status: StatusStarted},
			&Resource{Name: "test", Status: ResourceLocked},
			nil,
			true,
			nil,
			StatusComplete,
			ResourceFree,
		},
		{
			"test",
			&Task{Status: StatusStarted},
			&Resource{Name: "test", Status: ResourceFree},
			nil,
			true,
			nil,
			StatusComplete,
			ResourceFree,
		},
		{
			"test",
			nil,
			nil,
			NoStagedTaskError,
			false,
			nil,
			StatusComplete,
			ResourceFree,
		},
		{
			"test",
			&Task{Status: StatusQueued},
			&Resource{Name: "test", Status: ResourceFree},
			TaskNotStartedError,
			false,
			nil,
			StatusQueued,
			ResourceFree,
		},
		{
			"test",
			&Task{Status: StatusStarted},
			&Resource{Name: "test", Status: ResourceLocked},
			modelErr,
			true,
			modelErr,
			StatusComplete,
			ResourceFree,
		},
	}

	for i, tt := range table {
		ctrl := NewResourceController(nil)
		if tt.Task != nil {
			ctrl.stage[tt.Key] = tt.Task
		}
		if tt.Resource != nil {
			ctrl.resources[tt.Key] = tt.Resource
		}
		model := &MockModel{}
		if tt.Model {
			model.On("Save", tt.Task).Return(DocumentMeta{}, tt.ModelErr)
		}
		if err := ctrl.CompleteTask(tt.Key, model); err != nil && err != tt.Err {
			t.Fatal(err)
		}
		if ctrl.resources[tt.Key] != nil && ctrl.resources[tt.Key].Status != tt.ResourceStatus {
			t.Fatalf("[%d] expected resource status %d, got %d", i, tt.ResourceStatus, ctrl.resources[tt.Key].Status)
		}
		if tt.TaskStatus == StatusComplete && ctrl.resources[tt.Key] != nil {
			t.Fatal("expected stage to be nil")
		}
		if ctrl.stage[tt.Key] != nil && ctrl.stage[tt.Key] != nil && ctrl.stage[tt.Key].Status != tt.TaskStatus {
			t.Fatalf("[%d] expected task status %d, got %d", i, tt.TaskStatus, ctrl.stage[tt.Key].Status)
		}
		if tt.Model {
			model.AssertExpectations(t)
		}
	}
}

func TestControllerQueueReady(t *testing.T) {
	var table = []struct {
		Key       string
		Priority  float64
		Result    json.RawMessage
		BrokerErr *jrpc2.ErrorObject
		Err       error
	}{
		{
			"test",
			2.4,
			[]byte(`{"_key":"test","priority":2.4}`),
			nil,
			nil,
		},
		{
			"test",
			0,
			nil,
			&jrpc2.ErrorObject{Message: "broker error"},
			errors.New("broker error"),
		},
	}

	for _, tt := range table {
		params := map[string]interface{}{"key": tt.Key}
		broker := &MockServiceBroker{}
		broker.On("Call", PriorityQueueHost, "pop", params).Return(tt.Result, tt.BrokerErr)
		ctrl := NewResourceController(broker)
		task, err := ctrl.queueReady(tt.Key)
		if err != nil && err.Error() != tt.Err.Error() {
			t.Fatal(err)
		}
		if tt.Result != nil && tt.Priority != task.Priority {
			t.Fatalf("expected task priority to be %f, got %f", tt.Priority, task.Priority)
		}
		broker.AssertExpectations(t)
	}
}

func TestControllerSchedulerReady(t *testing.T) {
	now := time.Now().Format(time.RFC3339)
	var table = []struct {
		Key       string
		RunAt     string
		Result    json.RawMessage
		BrokerErr *jrpc2.ErrorObject
		Err       error
	}{
		{
			"test",
			now,
			[]byte(fmt.Sprintf(`{"_key":"test", "runAt": "%s"}`, now)),
			nil,
			nil,
		},
		{
			"test",
			time.Now().String(),
			nil,
			&jrpc2.ErrorObject{Message: "broker error"},
			errors.New("broker error"),
		},
	}

	for _, tt := range table {
		params := map[string]interface{}{"key": tt.Key}
		broker := &MockServiceBroker{}
		broker.On("Call", TimetableHost, "next", params).Return(tt.Result, tt.BrokerErr)
		ctrl := NewResourceController(broker)
		task, err := ctrl.scheduleReady(tt.Key)
		if err != nil && err.Error() != tt.Err.Error() {
			t.Fatal(err)
		}
		if tt.Result != nil && tt.RunAt != task.RunAt.Format(time.RFC3339) {
			t.Fatalf("expected task run at to be %s, got %s", tt.RunAt, task.RunAt)
		}
		broker.AssertExpectations(t)
	}
}
