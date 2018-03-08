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
	"github.com/stretchr/testify/mock"
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
		Task         *Task
		Result       float64
		Status       string
		BrokerErr    *jrpc2.ErrorObject
		taskModelErr error
		RescModelErr error
		Err          error
	}{
		{
			NewTask([]byte(`{"key": "test123", priority": 12.3}`)),
			0,
			StatusQueued,
			nil,
			nil,
			nil,
			nil,
		},
		{
			NewTask([]byte(fmt.Sprintf(`{"key": "test123", "runAt": "%s"}`, time.Now().Format(time.RFC3339)))),
			0,
			StatusScheduled,
			nil,
			nil,
			nil,
			nil,
		},
		{
			NewTask([]byte(fmt.Sprintf(`{"key": "test123", runAt": %s}`, time.Now().Format(time.RFC3339)))),
			-1,
			StatusPending,
			nil,
			nil,
			nil,
			TaskAddFailedError,
		},
		{
			NewTask([]byte(`{"key": "test123", priority": 12.3}`)),
			-1,
			StatusPending,
			&jrpc2.ErrorObject{Message: "broker error"},
			nil,
			nil,
			errors.New("broker error"),
		},
		{
			NewTask([]byte(`{"key": "test123", priority": 12.3}`)),
			0,
			StatusQueued,
			nil,
			errors.New("model error"),
			nil,
			errors.New("model error"),
		},
		{
			NewTask([]byte(`{"key": "test123", priority": 12.3}`)),
			0,
			StatusQueued,
			nil,
			nil,
			errors.New("model error"),
			errors.New("model error"),
		},
	}

	for _, tt := range table {
		broker := new(MockServiceBroker)
		broker.On(
			"Call",
			StatusChangeNotifierHost,
			"notify",
			mock.MatchedBy(func(p map[string]interface{}) bool { return p["kind"] == "taskStatusChanged" }),
		).Return(float64(0), nil).Maybe()
		params := map[string]interface{}{"key": tt.Task.Key, "id": tt.Task.Id}
		if tt.Task.RunAt != nil {
			params["runAt"] = tt.Task.RunAt.Format(time.RFC3339)
			broker.On("Call", TimetableHost, "insert", params).Return(tt.Result, tt.BrokerErr).Once()
		} else {
			params["priority"] = tt.Task.Priority
			broker.On("Call", PriorityQueueHost, "push", params).Return(tt.Result, tt.BrokerErr).Once()
		}
		taskModel := new(MockModel)
		rescModel := new(MockModel)
		taskModel.On("Save", tt.Task).Return(DocumentMeta{}, tt.taskModelErr).Maybe()
		rescModel.On("Save", mock.AnythingOfType("*main.Resource")).Return(DocumentMeta{}, tt.RescModelErr).Maybe()
		ctrl := NewResourceController(broker)
		if err := ctrl.AddTask(tt.Task, taskModel, rescModel); err != nil && err.Error() != tt.Err.Error() {
			t.Fatal(err)
		}
		if tt.Task.Status != tt.Status {
			t.Fatalf("expected task status to be %d, got %d", tt.Status, tt.Task.Status)
		}
		broker.AssertExpectations(t)
		taskModel.AssertExpectations(t)
		rescModel.AssertExpectations(t)
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
		Result    map[string]interface{}
		BrokerErr *jrpc2.ErrorObject
		Err       error
	}{
		{
			"test",
			map[string]interface{}{"_key": "test"},
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
		if list != nil && list["_key"] != tt.Result["_key"] {
			t.Fatalf("expected list to be %s, got %s", tt.Result["_key"], list["_key"])
		}
		broker.AssertExpectations(t)
	}
}

func TestControllerListPriorityQueue(t *testing.T) {
	var table = []struct {
		Key       string
		Result    map[string]interface{}
		BrokerErr *jrpc2.ErrorObject
		Err       error
	}{
		{
			"test",
			map[string]interface{}{"_key": "test"},
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
		if list != nil && list["_key"] != tt.Result["_key"] {
			t.Fatalf("expected list to be %s, got %s", tt.Result["_key"], list["_key"])
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
		ResourceErr    error
		TaskStatus     string
		ResourceStatus ResourceStatus
	}{
		{
			"test",
			&Task{Status: StatusPending},
			&Resource{Name: "test", Status: ResourceFree},
			nil,
			true,
			nil,
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
			nil,
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
			nil,
			StatusScheduled,
			ResourceFree,
		},
		{
			"test",
			&Task{Status: StatusPending},
			&Resource{Name: "test", Status: ResourceLocked},
			ResourceUnavailableError,
			false,
			nil,
			nil,
			StatusPending,
			ResourceLocked,
		},
	}

	for i, tt := range table {
		broker := &MockServiceBroker{}
		broker.On(
			"Call",
			StatusChangeNotifierHost,
			"notify",
			mock.MatchedBy(func(p map[string]interface{}) bool { return p["kind"] == "taskStatusChanged" }),
		).Return(float64(0), nil).Maybe()
		ctrl := NewResourceController(broker)
		if tt.Task != nil {
			ch := make(chan *Task, StageBuffer)
			ch <- tt.Task
			ctrl.stage.Store(tt.Key, ch)
		}
		if tt.Resource != nil {
			ctrl.resources[tt.Key] = tt.Resource
		}
		taskModel := &MockModel{}
		taskModel.On("Save", tt.Task).Return(DocumentMeta{}, tt.ModelErr).Maybe()
		resourceModel := &MockModel{}
		resourceModel.On("Save", tt.Resource).Return(DocumentMeta{}, tt.ResourceErr).Maybe()
		if err := ctrl.StartTask(tt.Key, taskModel, resourceModel); err != nil && err != tt.Err {
			t.Fatal(err)
		}
		if ctrl.resources[tt.Key] != nil && ctrl.resources[tt.Key].Status != tt.ResourceStatus {
			t.Fatalf("[%d] expected resource status %d, got %d", i, tt.ResourceStatus, ctrl.resources[tt.Key].Status)
		}
		if tt.Model {
			taskModel.AssertExpectations(t)
			resourceModel.AssertExpectations(t)
		}
		broker.AssertExpectations(t)
	}
}

func TestControllerCompleteTask(t *testing.T) {
	modelErr := errors.New("model error")
	queryErr := errors.New("query error")
	var table = []struct {
		TaskId         string
		Tasks          []interface{}
		Resource       *Resource
		Status         string
		Err            error
		Model          bool
		ModelErr       error
		ResourceErr    error
		QueryErr       error
		TaskStatus     string
		ResourceStatus ResourceStatus
	}{
		{
			"test",
			[]interface{}{&Task{Key: "test", Status: StatusStarted}},
			&Resource{Name: "test", Status: ResourceLocked},
			StatusComplete,
			nil,
			true,
			nil,
			nil,
			nil,
			StatusComplete,
			ResourceFree,
		},
		{
			"test",
			[]interface{}{&Task{Key: "test", Status: StatusStarted}},
			&Resource{Name: "test", Status: ResourceFree},
			StatusCancelled,
			nil,
			true,
			nil,
			nil,
			nil,
			StatusComplete,
			ResourceFree,
		},
		{
			"test",
			[]interface{}{&Task{Key: "test", Status: StatusQueued}},
			&Resource{Name: "test", Status: ResourceFree},
			StatusComplete,
			TaskNotStartedError,
			false,
			nil,
			nil,
			nil,
			StatusQueued,
			ResourceFree,
		},
		{
			"test",
			[]interface{}{&Task{Key: "test", Status: StatusStarted}},
			&Resource{Name: "test", Status: ResourceLocked},
			StatusComplete,
			modelErr,
			true,
			nil,
			modelErr,
			nil,
			StatusComplete,
			ResourceFree,
		},
		{
			"test",
			[]interface{}{},
			&Resource{Name: "test", Status: ResourceFree},
			StatusComplete,
			TaskNotFoundError,
			true,
			nil,
			nil,
			nil,
			StatusPending,
			ResourceFree,
		},
		{
			"abc123",
			nil,
			&Resource{Name: "test", Status: ResourceFree},
			StatusComplete,
			queryErr,
			true,
			nil,
			nil,
			queryErr,
			StatusPending,
			ResourceFree,
		},
	}

	for i, tt := range table {
		broker := &MockServiceBroker{}
		broker.On(
			"Call",
			StatusChangeNotifierHost,
			"notify",
			mock.MatchedBy(func(p map[string]interface{}) bool { return p["kind"] == "taskStatusChanged" }),
		).Return(float64(0), nil).Maybe()
		ctrl := NewResourceController(broker)
		if tt.Resource != nil {
			ctrl.resources[tt.TaskId] = tt.Resource
		}
		taskModel := &MockModel{}
		resourceModel := &MockModel{}
		resourceModel.On("Save", tt.Resource).Return(DocumentMeta{}, tt.ResourceErr).Maybe()
		q := fmt.Sprintf(`FOR t IN %s FILTER t._key == @key RETURN t`, CollectionTasks)
		taskModel.On("Query", q, map[string]interface{}{"key": tt.TaskId}).Return(tt.Tasks, tt.QueryErr).Maybe()
		taskModel.On("Save", mock.AnythingOfType("*main.Task")).Return(DocumentMeta{}, tt.ModelErr).Maybe()
		if err := ctrl.CompleteTask(tt.TaskId, tt.Status, taskModel, resourceModel); err != nil && err != tt.Err {
			t.Fatal(err)
		}
		if ctrl.resources[tt.TaskId] != nil && ctrl.resources[tt.TaskId].Status != tt.ResourceStatus {
			t.Fatalf("[%d] expected resource status %d, got %d", i, tt.ResourceStatus, ctrl.resources[tt.TaskId].Status)
		}
		if tt.Model {
			taskModel.AssertExpectations(t)
			resourceModel.AssertExpectations(t)
		}
		broker.AssertExpectations(t)
	}
}

func TestControllerStagedQueuedTask(t *testing.T) {
	var table = []struct {
		Key       string
		Priority  float64
		Result    map[string]interface{}
		BrokerErr *jrpc2.ErrorObject
		Err       error
	}{
		{
			"test",
			2.4,
			map[string]interface{}{"_key": "test", "priority": 2.4},
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
		task, err := ctrl.stageQueuedTask(tt.Key)
		if err != nil && err.Error() != tt.Err.Error() {
			t.Fatal(err)
		}
		if task != nil && tt.Priority != task.Priority {
			t.Fatalf("expected task priority to be %f, got %f", tt.Priority, task.Priority)
		}
		broker.AssertExpectations(t)
	}
}

func TestControllerStageScheduledTask(t *testing.T) {
	var table = []struct {
		Key       string
		RunAt     string
		Result    map[string]interface{}
		BrokerErr *jrpc2.ErrorObject
		Err       error
	}{
		{
			"test",
			time.Now().Format(time.RFC3339),
			map[string]interface{}{"_key": "test", "runAt": time.Now()},
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
		task, err := ctrl.stageScheduledTask(tt.Key)
		if err != nil && err.Error() != tt.Err.Error() {
			t.Fatal(err)
		}
		if task != nil && task.RunAt != nil && tt.RunAt != task.RunAt.Format(time.RFC3339) {
			t.Fatalf("expected task run at to be %s, got %s", tt.RunAt, task.RunAt)
		}
		broker.AssertExpectations(t)
	}
}

func TestControllerStartStageLoop(t *testing.T) {
	table := []struct {
		Key              string
		Resources        []*Resource
		QueueResponse    interface{}
		QueueErr         *jrpc2.ErrorObject
		ScheduleResponse interface{}
		ScheduleErr      *jrpc2.ErrorObject
		QueryResult      []interface{}
		QueryErr         error
		TaskId           string
	}{
		{
			"test",
			[]*Resource{{Name: "test"}},
			nil,
			nil,
			map[string]interface{}{"_key": "abc123", "key": "test"},
			nil,
			[]interface{}{&Task{Id: "abc123", Key: "test"}},
			nil,
			"abc123",
		},
		{
			"test",
			[]*Resource{{Name: "test"}},
			nil,
			nil,
			nil,
			&jrpc2.ErrorObject{Message: "error"},
			nil,
			nil,
			"",
		},
		{
			"test",
			[]*Resource{{Name: "test"}},
			map[string]interface{}{"_key": "abc123", "key": "test"},
			nil,
			nil,
			nil,
			[]interface{}{&Task{Id: "abc123", Key: "test"}},
			nil,
			"abc123",
		},
		{
			"test",
			[]*Resource{{Name: "test"}},
			nil,
			&jrpc2.ErrorObject{Message: "error"},
			nil,
			nil,
			nil,
			nil,
			"",
		},
	}

	for _, tt := range table {
		params := map[string]interface{}{"key": tt.Key}
		model := &MockModel{}
		q := fmt.Sprintf(`FOR t IN %s FILTER t._key == @key RETURN t`, CollectionTasks)
		broker := &MockServiceBroker{}
		ctrl := NewResourceController(broker)
		model.On("Query", q, map[string]interface{}{"key": tt.TaskId}).Return(tt.QueryResult, tt.QueryErr).Maybe().Run(func(args mock.Arguments) {
			if tt.QueryErr != nil {
				ch := make(chan *Task, StageBuffer)
				ch <- nil
				ctrl.stage.Store(tt.Key, ch)
			}
		})
		model.On("Save", mock.AnythingOfType("*main.Task")).Return(DocumentMeta{}, nil).Maybe()
		broker.On(
			"Call",
			StatusChangeNotifierHost,
			"notify",
			mock.MatchedBy(func(p map[string]interface{}) bool { return p["kind"] == "taskStatusChanged" }),
		).Return(float64(0), nil).Maybe()
		broker.On("Call", TimetableHost, "next", params).Return(tt.ScheduleResponse, tt.ScheduleErr).Maybe().Run(func(args mock.Arguments) {
			if tt.ScheduleErr != nil {
				ch := make(chan *Task, StageBuffer)
				ch <- nil
				ctrl.stage.Store(tt.Key, ch)
			}
		})
		broker.On("Call", PriorityQueueHost, "pop", params).Return(tt.QueueResponse, tt.QueueErr).Maybe().Run(func(args mock.Arguments) {
			if tt.QueueErr != nil {
				ch := make(chan *Task, StageBuffer)
				ch <- nil
				ctrl.stage.Store(tt.Key, ch)
			}
		})
		for _, resource := range tt.Resources {
			ctrl.resources[resource.Name] = resource
		}
		go ctrl.StartStageLoop(model)
		var task *Task
		for {
			if ch, ok := ctrl.stage.Load(tt.Key); ok {
				task = <-ch.(chan *Task)
				break
			}
		}
		if task != nil && task.Id != tt.TaskId {
			t.Fatalf("expected task id to be %s", tt.TaskId)
		}
		broker.AssertExpectations(t)
		model.AssertExpectations(t)
	}
}

func TestControllerStageTask(t *testing.T) {
	model := &MockModel{}
	model.On("Save", mock.AnythingOfType("*main.Task")).Return(DocumentMeta{}, nil).Maybe()
	broker := &MockServiceBroker{}
	broker.On(
		"Call",
		StatusChangeNotifierHost,
		"notify",
		mock.MatchedBy(func(p map[string]interface{}) bool { return p["kind"] == "taskStatusChanged" }),
	).Return(float64(0), nil).Maybe()
	ctrl := NewResourceController(broker)
	ctrl.StageTask(&Task{Id: "abc123", Key: "test"}, model, true)
	if _, ok := ctrl.stage.Load("test"); !ok {
		t.Fatal("expected stage abc123 to be ok")
	}
	model.AssertExpectations(t)
	broker.AssertExpectations(t)
}

func TestControllerRemoveTask(t *testing.T) {
	var table = []struct {
		Key         string
		Id          string
		Result      float64
		Status      string
		BrokerErr   *jrpc2.ErrorObject
		ModelErr    error
		QueryResult []interface{}
		QueryErr    error
		Err         error
	}{
		{
			"test123",
			"abc123",
			0,
			StatusCancelled,
			nil,
			nil,
			[]interface{}{&Task{Key: "test123", Id: "abc123", Status: StatusQueued}},
			nil,
			nil,
		},
		{
			"test123",
			"abc123",
			0,
			StatusCancelled,
			nil,
			nil,
			[]interface{}{&Task{Key: "test123", Id: "abc123", Status: StatusScheduled}},
			nil,
			nil,
		},
		{
			"test123",
			"abc123",
			0,
			StatusCancelled,
			nil,
			nil,
			[]interface{}{&Task{Key: "test123", Id: "abc123", Status: StatusPending}},
			nil,
			nil,
		},
		{
			"test123",
			"abc123",
			0,
			StatusStarted,
			nil,
			nil,
			[]interface{}{&Task{Key: "test123", Id: "abc123", Status: StatusStarted}},
			nil,
			TaskRemoveFailedError,
		},
		{
			"test123",
			"abc123",
			-1,
			StatusStarted,
			nil,
			nil,
			[]interface{}{&Task{Key: "test123", Id: "abc123", Status: StatusPending}},
			nil,
			TaskRemoveFailedError,
		},
		{
			"test123",
			"abc123",
			0,
			StatusStarted,
			nil,
			nil,
			[]interface{}{},
			nil,
			TaskNotFoundError,
		},
		{
			"test123",
			"abc123",
			0,
			StatusStarted,
			nil,
			nil,
			[]interface{}{},
			errors.New("query error"),
			errors.New("query error"),
		},
		{
			"test123",
			"abc123",
			0,
			StatusStarted,
			nil,
			errors.New("model error"),
			[]interface{}{&Task{Key: "test123", Id: "abc123", Status: StatusQueued}},
			nil,
			errors.New("model error"),
		},
		{
			"test123",
			"abc123",
			0,
			StatusCancelled,
			nil,
			nil,
			[]interface{}{&Task{Key: "test123", Id: "abc123", Status: StatusPending}},
			nil,
			nil,
		},
	}

	for _, tt := range table {
		model := new(MockModel)
		q := fmt.Sprintf(`FOR t IN %s FILTER t._key == @key RETURN t`, CollectionTasks)
		model.On("Query", q, map[string]interface{}{"key": tt.Id}).Return(tt.QueryResult, tt.QueryErr).Maybe()
		model.On("Remove", mock.AnythingOfType("*main.Task")).Return(tt.ModelErr).Maybe()
		broker := new(MockServiceBroker)
		broker.On(
			"Call",
			StatusChangeNotifierHost,
			"notify",
			mock.MatchedBy(func(p map[string]interface{}) bool { return p["kind"] == "taskStatusChanged" }),
		).Return(float64(0), nil).Maybe()
		params := map[string]interface{}{"key": tt.Key, "id": tt.Id}
		broker.On("Call", TimetableHost, "remove", params).Return(tt.Result, tt.BrokerErr).Maybe()
		broker.On("Call", PriorityQueueHost, "remove", params).Return(tt.Result, tt.BrokerErr).Maybe()
		ctrl := NewResourceController(broker)
		if err := ctrl.RemoveTask(tt.Id, model); err != nil && err.Error() != tt.Err.Error() {
			t.Fatal(err)
		}
		broker.AssertExpectations(t)
		model.AssertExpectations(t)
	}
}
