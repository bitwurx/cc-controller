package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
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

var (
	NoStagedTaskError        = errors.New("no staged task")
	NotificationFailedError  = errors.New("notification failed")
	QueueNotFoundError       = errors.New("queue not found")
	ResourceUnavailableError = errors.New("resource unavailable")
	ResourceExistsError      = errors.New("resource exists")
	TaskAddFailedError       = errors.New("task add failed")
	TaskAlreadyStartedError  = errors.New("task already started")
	TaskNotFoundError        = errors.New("task not found")
	TaskNotStartedError      = errors.New("task not started")
	TimetableNotFound        = errors.New("timetable not found")
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

type Controller interface {
	AddResource(string, Model) error
	AddTask(*Task, Model) error
	CompleteTask(string, Model) error
	GetTask(string, Model) (*Task, error)
	ListPriorityQueue(string) (json.RawMessage, error)
	ListTimetable(string) (json.RawMessage, error)
	Notify(*Event) error
	StartTask(string, Model) error
}

// ResourceController handles tasks progression and resource allocation.
type ResourceController struct {
	resources map[string]*Resource
	stage     map[string](chan *Task)
	broker    ServiceBroker
}

// NewResourceController creates a new ResourceController instance.
func NewResourceController(broker ServiceBroker) *ResourceController {
	return &ResourceController{make(map[string]*Resource), make(map[string](chan *Task)), broker}
}

// AddResource adds the resource to the ResourceController for management.
func (ctrl *ResourceController) AddResource(name string, taskModel Model) error {
	if _, ok := ctrl.resources[name]; ok {
		return ResourceExistsError
	}
	resource := NewResource(name)
	ctrl.resources[name] = resource
	_, err := taskModel.Save(resource)
	return err
}

// AddTask adds the task to the correct service.
//
// If the task contains a run at point in time value it is added to
// the timetable service for scheduling.
//
// If the run at point in time is omitted the task is added to the
// priority queue service for priority order execution.
func (ctrl *ResourceController) AddTask(task *Task, taskModel Model) error {
	var result interface{}
	var errObj *jrpc2.ErrorObject
	var status TaskStatus

	params := map[string]interface{}{"key": task.Key, "id": task.Id}
	if task.RunAt != nil {
		params["runAt"] = task.RunAt.Format(time.RFC3339)
		result, errObj = ctrl.broker.Call(TimetableHost, "insert", params)
		status = StatusScheduled
	} else {
		params["priority"] = task.Priority
		result, errObj = ctrl.broker.Call(PriorityQueueHost, "push", params)
		status = StatusQueued
	}
	if errObj != nil {
		return errors.New(string(errObj.Message))
	}
	result = int(result.(float64))
	if result != 0 {
		return TaskAddFailedError
	}
	task.Status = status
	if _, err := taskModel.Save(task); err != nil {
		return err
	}
	return nil
}

// CompleteTask marks the staged task as complete.
//
// an error is encountered if no staged task exists for the key or if
// the task is not in the started state.
func (ctrl *ResourceController) CompleteTask(key string, taskModel Model) error {
	q := fmt.Sprintf(`FOR t IN %s FILTER t._key == @key RETURN t`, CollectionTasks)
	tasks, err := taskModel.Query(q, map[string]interface{}{"key": key})
	if err != nil {
		return err
	}
	if len(tasks) < 1 {
		return TaskNotFoundError
	}
	task := tasks[0].(*Task)
	if task.Status != StatusStarted {
		return TaskNotStartedError
	}
	ctrl.resources[key].Status = ResourceFree
	task.Status = StatusComplete
	_, err = taskModel.Save(task)
	ctrl.resources[key] = nil
	return err
}

// GetTask returns the task with the provided id.
func (ctrl *ResourceController) GetTask(taskId string, taskModel Model) (*Task, error) {
	q := fmt.Sprintf(`FOR t IN %s FILTER t._key == @key RETURN t`, CollectionTasks)
	tasks, err := taskModel.Query(q, map[string]interface{}{"key": taskId})
	if err != nil {
		return nil, err
	}
	if len(tasks) < 1 {
		return nil, TaskNotFoundError
	}
	return tasks[0].(*Task), nil
}

// ListPrioriryQueue lists the heap nodes in the priority queue
// with the provided key.
func (ctrl *ResourceController) ListPriorityQueue(key string) (json.RawMessage, error) {
	params := map[string]interface{}{"key": key}
	result, errObj := ctrl.broker.Call(PriorityQueueHost, "get", params)
	if errObj != nil {
		return nil, errors.New(strings.ToLower(string(errObj.Message)))
	}
	return result.(json.RawMessage), nil
}

// ListTimetable lists the scheduled tasks in the timetable with the
// provided key.
func (ctrl *ResourceController) ListTimetable(key string) (json.RawMessage, error) {
	params := map[string]interface{}{"key": key}
	result, errObj := ctrl.broker.Call(TimetableHost, "get", params)
	if errObj != nil {
		return nil, errors.New(strings.ToLower(string(errObj.Message)))
	}
	return result.(json.RawMessage), nil
}

// Notify sends a status change event to the status change notifier.
func (ctrl *ResourceController) Notify(evt *Event) error {
	params := map[string]interface{}{"created": evt.Created, "kind": evt.Kind, "meta": evt.Meta}
	result, errObj := ctrl.broker.Call(StatusChangeNotifierHost, "notify", params)
	if errObj != nil {
		return errors.New(string(errObj.Message))
	}
	result = int(result.(float64))
	if result != 0 {
		return NotificationFailedError
	}
	return nil
}

// StartTask starts the staged task.
//
// an error is encountered if no staged task exists for the key or if
// the resource associated with the task is locked.
func (ctrl *ResourceController) StartTask(key string, taskModel Model) error {
	if task := <-ctrl.stage[key]; task != nil {
		if ctrl.resources[key].Status == ResourceLocked {
			ctrl.stage[key] <- task
			return ResourceUnavailableError
		}
		ctrl.stage[key] <- nil
		if task.Status == StatusStarted {
			return TaskAlreadyStartedError
		}
		ctrl.resources[key].Status = ResourceLocked
		task.Status = StatusStarted
		_, err := taskModel.Save(task)
		return err
	}

	return NoStagedTaskError
}

// queueReady fetches the next task from the priorty queue.
func (ctrl *ResourceController) queueReady(key string) (*Task, error) {
	params := map[string]interface{}{"key": key}
	result, errObj := ctrl.broker.Call(PriorityQueueHost, "pop", params)
	if errObj != nil {
		return nil, errors.New(string(errObj.Message))
	}
	var task *Task
	json.Unmarshal(result.(json.RawMessage), &task)
	return task, nil
}

// scheduleReady fetches the next scheduled task from the timetable.
func (ctrl *ResourceController) scheduleReady(key string) (*Task, error) {
	params := map[string]interface{}{"key": key}
	result, errObj := ctrl.broker.Call(TimetableHost, "next", params)
	if errObj != nil {
		return nil, errors.New(string(errObj.Message))
	}
	var task *Task
	json.Unmarshal(result.(json.RawMessage), &task)
	return task, nil
}
