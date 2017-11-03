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
		BrokerErr *jrpc2.ErrorObject
		Model     bool
		ModelErr  error
		Err       error
	}{
		{
			NewTask([]byte(`{"key": "test123", priority": 12.3}`)),
			0,
			nil,
			true,
			nil,
			nil,
		},
		{
			NewTask([]byte(fmt.Sprintf(`{"key": "test123", "runAt": "%s"}`, time.Now().Format(time.RFC3339)))),
			0,
			nil,
			true,
			nil,
			nil,
		},
		{
			NewTask([]byte(fmt.Sprintf(`{"key": "test123", runAt": %s}`, time.Now().Format(time.RFC3339)))),
			-1,
			nil,
			false,
			nil,
			errors.New("task add failed"),
		},
		{
			NewTask([]byte(`{"key": "test123", priority": 12.3}`)),
			-1,
			&jrpc2.ErrorObject{Message: "broker error"},
			false,
			nil,
			errors.New("broker error"),
		},
		{
			NewTask([]byte(`{"key": "test123", priority": 12.3}`)),
			0,
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
		ctrl := NewController(broker)
		if err := ctrl.AddTask(tt.Task, model); err != nil && err.Error() != tt.Err.Error() {
			t.Fatal(err)
		}
		broker.AssertExpectations(t)

		if tt.Model {
			model.AssertExpectations(t)
		}
	}
}

func TestControllerAddResource(t *testing.T) {
	ctrl := NewController(nil)
	ctrl.AddResource("test")
	if _, ok := ctrl.Resources["test"]; !ok {
		t.Fatal("expected resource with 'test' to have been added")
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
			errors.New("notification failed"),
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
		ctrl := NewController(broker)
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
			errors.New("not found"),
		},
	}

	for _, tt := range table {
		q := fmt.Sprintf(`FOR t IN %s FILTER t._key == @key RETURN t`, CollectionTasks)
		ctrl := NewController(nil)
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
			errors.New("Timetable not found"),
		},
	}

	for _, tt := range table {
		params := map[string]interface{}{"key": tt.Key}
		broker := new(MockServiceBroker)
		broker.On("Call", TimetableHost, "get", params).Return(tt.Result, tt.BrokerErr)
		ctrl := NewController(broker)
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
