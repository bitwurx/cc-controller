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
		T         *Task
		Result    float64
		BrokerErr *jrpc2.ErrorObject
		ModelErr  error
		Err       error
	}{
		{
			NewTask([]byte(`{"key": "test123", priority": 12.3}`)),
			0,
			nil,
			nil,
			nil,
		},
		{
			NewTask([]byte(fmt.Sprintf(`{"key": "test123", "runAt": "%s"}`, time.Now().Format(time.RFC3339)))),
			0,
			nil,
			nil,
			nil,
		},
		{
			NewTask([]byte(fmt.Sprintf(`{"key": "test123", runAt": %s}`, time.Now().Format(time.RFC3339)))),
			-1,
			nil,
			nil,
			errors.New("task add failed"),
		},
		{
			NewTask([]byte(`{"key": "test123", priority": 12.3}`)),
			-1,
			&jrpc2.ErrorObject{Message: "broker error"},
			nil,
			errors.New("broker error"),
		},
		{
			NewTask([]byte(`{"key": "test123", priority": 12.3}`)),
			0,
			nil,
			errors.New("model error"),
			errors.New("model error"),
		},
	}

	for _, tt := range table {
		broker := new(MockServiceBroker)
		params := map[string]interface{}{"key": tt.T.Key, "id": tt.T.Id}
		if tt.T.RunAt != nil {
			params["runAt"] = tt.T.RunAt.Format(time.RFC3339)
			broker.On("Call", TimetableHost, "insert", params).Return(tt.Result, tt.BrokerErr)
		} else {
			params["priority"] = tt.T.Priority
			broker.On("Call", PriorityQueueHost, "push", params).Return(tt.Result, tt.BrokerErr)
		}
		model := new(MockModel)
		model.On("Save", tt.T).Return(DocumentMeta{}, tt.ModelErr)
		ctrl := NewController(broker)
		if err := ctrl.AddTask(tt.T, model); err != nil && err.Error() != tt.Err.Error() {
			t.Fatal(err)
		}
	}

	// ctrl := NewController(nil)
	// task := NewTask([]byte(`{"key": "test123", priority": 12.3}`))w
	// client := &MockControllerAddTaskClient{}
	// model := MockControllerAddTaskModel{MockModel: MockModel{}}
	// if err := ctrl.AddTask(task, client, model); err != nil {
	// 	t.Fatal(err)
	// }
	// if _, ok := ctrl.Resources["test123"]; !ok {
	// 	t.Fatal("expected resource with name 'test123' to exist")
	// }
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
		broker.On("Call", tt.Url, "notify", params).Return(tt.Result, tt.BrokerErr)
		ctrl := NewController(broker)
		if err := ctrl.Notify(tt.Evt); err != nil && err.Error() != tt.Err.Error() {
			t.Fatal(err)
		}
	}
}
