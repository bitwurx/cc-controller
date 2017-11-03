package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"github.com/bitwurx/jrpc2"
)

const (
	TaskStatusChangedEvent = "taskStatusChanged" // task status changed event.
)

var (
	PriorityQueueHost        = os.Getenv("CONCORD_PRIORITY_QUEUE_HOST")         // the hostname of the priority queue service.
	TimetableHost            = os.Getenv("CONCORD_TIMETABLE_HOST")              // the hostname of the timetable service.
	StatusChangeNotifierHost = os.Getenv("CONCORD_STATUS_CHANGE_NOTIFIER_HOST") // the hostname of the status change notifier service.
)

const (
	BrokerCallErrorCode jrpc2.ErrorCode = -32100 // broker call jrpc error code.
)

// ServiceBroker contains method for calling external services.
type ServiceBroker interface {
	Call(string, string, map[string]interface{}) (interface{}, *jrpc2.ErrorObject)
}

// JsonRPCServiceBroker is json-rpc 2.0 service broker.
type JsonRPCServiceBroker struct{}

// Call initiates a remote call of the method with parameters to the
// provided url.
func (t *JsonRPCServiceBroker) Call(url string, method string, params map[string]interface{}) (interface{}, *jrpc2.ErrorObject) {
	p, _ := json.Marshal(params)
	req := bytes.NewBuffer([]byte(fmt.Sprintf(`{"jsonrpc": "2.0", "method": "%s", "params": %s, "id": 0}`, method, string(p))))
	resp, err := http.Post(fmt.Sprintf("http://%s/rpc", url), "application/json", req)
	if err != nil {
		return nil, &jrpc2.ErrorObject{
			Code:    BrokerCallErrorCode,
			Message: jrpc2.ServerErrorMsg,
			Data:    err.Error(),
		}
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, &jrpc2.ErrorObject{
			Code:    BrokerCallErrorCode,
			Message: jrpc2.ServerErrorMsg,
			Data:    err.Error(),
		}
	}

	var respObj jrpc2.ResponseObject
	json.Unmarshal(body, &respObj)

	return respObj.Result, respObj.Error
}

// Event contains the details of a status change event.
type Event struct {
	// Kind is the type of status change event.
	// Created is the time the event occured.
	// Meta is passthrough data about the event.
	Kind    string          `json:"kind"`
	Created time.Time       `json:"created"`
	Meta    json.RawMessage `json:"meta'`
}

// NewEvent create a new event instance from the provided data.
func NewEvent(kind string, meta []byte) *Event {
	return &Event{Kind: kind, Created: time.Now(), Meta: meta}
}

// Controller handles tasks progression and resource allocation.
type Controller struct {
	Resources map[string]*Resource
	stage     map[string]*Task
	broker    ServiceBroker
}

// NewController creates a new Controller instance.
func NewController(broker ServiceBroker) *Controller {
	return &Controller{make(map[string]*Resource), make(map[string]*Task), broker}
}

// AddResource adds the resource to the controller for management.
func (ctrl *Controller) AddResource(name string) {
	if _, ok := ctrl.Resources[name]; !ok {
		ctrl.Resources[name] = NewResource(name)
	}
}

// AddTask adds the task to the correct service.
//
// If the task contains a run at point in time value it is added to
// the timetable service for scheduling.
//
// If the run at point in time is omitted the task is added to the
// priority queue service for priority order execution.
func (ctrl *Controller) AddTask(task *Task, taskModel Model) error {
	var result interface{}
	var errObj *jrpc2.ErrorObject

	params := map[string]interface{}{"key": task.Key, "id": task.Id}
	if task.RunAt != nil {
		params["runAt"] = task.RunAt.Format(time.RFC3339)
		result, errObj = ctrl.broker.Call(TimetableHost, "insert", params)
	} else {
		params["priority"] = task.Priority
		result, errObj = ctrl.broker.Call(PriorityQueueHost, "push", params)
	}
	if errObj != nil {
		return errors.New(string(errObj.Message))
	}
	result = int(result.(float64))
	if result != 0 {
		return errors.New("task add failed")
	}
	if _, err := taskModel.Save(task); err != nil {
		return err
	}
	return nil
}

// GetTask returns the task with the provided id.
func (ctrl *Controller) GetTask(taskId string, taskModel Model) (*Task, error) {
	q := fmt.Sprintf(`FOR t IN %s FILTER t._key == @key RETURN t`, CollectionTasks)
	tasks, err := taskModel.Query(q, map[string]interface{}{"key": taskId})
	if err != nil {
		return nil, err
	}
	if len(tasks) < 1 {
		return nil, errors.New("not found")
	}
	return tasks[0].(*Task), nil
}

// ListTimetable lists the scheduled tasks in the task with the
// provided key.
func (ctrl *Controller) ListTimetable(key string) (json.RawMessage, error) {
	params := map[string]interface{}{"key": key}
	result, errObj := ctrl.broker.Call(TimetableHost, "get", params)
	if errObj != nil {
		return nil, errors.New(string(errObj.Message))
	}
	return result.(json.RawMessage), nil
}

// Notify sends a status change event to the status change notifier.
func (ctrl *Controller) Notify(evt *Event) error {
	params := map[string]interface{}{"created": evt.Created, "kind": evt.Kind, "meta": evt.Meta}
	result, errObj := ctrl.broker.Call(StatusChangeNotifierHost, "notify", params)
	if errObj != nil {
		return errors.New(string(errObj.Message))
	}
	result = int(result.(float64))
	if result != 0 {
		return errors.New("notification failed")
	}
	return nil
}
