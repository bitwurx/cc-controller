package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/bitwurx/jrpc2"
	"github.com/mitchellh/mapstructure"
)

const (
	StageBuffer            = 10
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
	TaskRemoveFailedError    = errors.New("task remove failed")
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
	CompleteTask(string, string, Model, Model) error
	GetTask(string, Model) (*Task, error)
	ListPriorityQueue(string) (map[string]interface{}, error)
	ListTimetable(string) (map[string]interface{}, error)
	Notify(*Event) error
	RemoveTask(string, Model) error
	StageTask(*Task, Model, bool)
	StartTask(string, Model, Model) error
}

// ResourceController handles tasks progression and resource allocation.
type ResourceController struct {
	resources map[string]*Resource
	stage     sync.Map
	broker    ServiceBroker
}

// NewResourceController creates a new ResourceController instance.
func NewResourceController(broker ServiceBroker) *ResourceController {
	return &ResourceController{make(map[string]*Resource), sync.Map{}, broker}
}

// AddResource adds the resource to the ResourceController for management.
func (ctrl *ResourceController) AddResource(name string, taskModel Model) error {
	if _, ok := ctrl.resources[name]; ok {
		return ResourceExistsError
	}
	resource := NewResource(name)
	ctrl.resources[name] = resource
	_, err := taskModel.Save(resource)
	log.Printf("resource added [%s]\n", name)
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
	var status string

	params := map[string]interface{}{"key": task.Key, "id": task.Id}
	if task.RunAt != nil {
		params["runAt"] = task.RunAt.Format(time.RFC3339)
		result, errObj = ctrl.broker.Call(TimetableHost, "insert", params)
		status = StatusScheduled
		log.Printf("scheduled task [%s]\n", task)
	} else {
		params["priority"] = task.Priority
		result, errObj = ctrl.broker.Call(PriorityQueueHost, "push", params)
		status = StatusQueued
		log.Printf("queued task [%s]\n", task)
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

	meta := make(map[string]interface{})
	json.Unmarshal(task.Meta, &meta)
	meta["_status"] = status
	meta["_id"] = task.Id
	data, _ := json.Marshal(meta)
	ctrl.Notify(NewEvent(TaskStatusChangedEvent, data))
	log.Printf("created task [%s]\n", task)

	return nil
}

// CompleteTask marks the staged task as complete.
//
// an error is encountered if a task with the provided does not exist
// or if the task is not in the started state.
func (ctrl *ResourceController) CompleteTask(taskId string, status string, taskModel Model, resourceModel Model) error {
	q := fmt.Sprintf(`FOR t IN %s FILTER t._key == @key RETURN t`, CollectionTasks)
	tasks, err := taskModel.Query(q, map[string]interface{}{"key": taskId})
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
	ctrl.resources[task.Key].Status = ResourceFree
	task.Status = status
	if _, err := taskModel.Save(task); err != nil {
		return err
	}
	if _, err := resourceModel.Save(ctrl.resources[task.Key]); err != nil {
		return err
	}

	meta := make(map[string]interface{})
	json.Unmarshal(task.Meta, &meta)
	meta["_status"] = status
	meta["_id"] = taskId
	data, _ := json.Marshal(meta)
	ctrl.Notify(NewEvent(TaskStatusChangedEvent, data))
	log.Printf("completed task [%s]\n", task)

	return nil
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
func (ctrl *ResourceController) ListPriorityQueue(key string) (map[string]interface{}, error) {
	params := map[string]interface{}{"key": key}
	result, errObj := ctrl.broker.Call(PriorityQueueHost, "get", params)
	if errObj != nil {
		return nil, errors.New(strings.ToLower(string(errObj.Message)))
	}
	return result.(map[string]interface{}), nil
}

// ListTimetable lists the scheduled tasks in the timetable with the
// provided key.
func (ctrl *ResourceController) ListTimetable(key string) (map[string]interface{}, error) {
	params := map[string]interface{}{"key": key}
	result, errObj := ctrl.broker.Call(TimetableHost, "get", params)
	if errObj != nil {
		return nil, errors.New(strings.ToLower(string(errObj.Message)))
	}
	return result.(map[string]interface{}), nil
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

func (ctrl *ResourceController) RemoveTask(id string, taskModel Model) error {
	var result interface{}
	var errObj *jrpc2.ErrorObject

	q := fmt.Sprintf(`FOR t IN %s FILTER t._key == @key RETURN t`, CollectionTasks)
	tasks, err := taskModel.Query(q, map[string]interface{}{"key": id})
	if err != nil {
		return err
	}
	if len(tasks) < 1 {
		return TaskNotFoundError
	}
	task := tasks[0].(*Task)
	if task.Status != StatusQueued && task.Status != StatusScheduled && task.Status != StatusPending {
		return TaskRemoveFailedError
	}
	params := map[string]interface{}{"key": task.Key, "id": task.Id}
	switch task.Status {
	case StatusQueued:
		result, errObj = ctrl.broker.Call(PriorityQueueHost, "remove", params)
	case StatusScheduled:
		result, errObj = ctrl.broker.Call(TimetableHost, "remove", params)
	}
	if errObj != nil {
		return errors.New(string(errObj.Message))
	}
	if result != nil {
		result = int(result.(float64))
		if result != 0 {
			return TaskRemoveFailedError
		}
	}
	task.Status = StatusCancelled
	if err := taskModel.Remove(task); err != nil {
		return err
	}

	meta := make(map[string]interface{})
	json.Unmarshal(task.Meta, &meta)
	meta["_status"] = StatusCancelled
	meta["_id"] = task.Id
	data, _ := json.Marshal(meta)
	ctrl.Notify(NewEvent(TaskStatusChangedEvent, data))
	log.Printf("removed task [%s]\n", task)

	return nil
}

// StartTask starts the staged task.
//
// an error is encountered if no staged task exists for the key or if
// the resource associated with the task is locked.
func (ctrl *ResourceController) StartTask(key string, taskModel Model, resourceModel Model) error {
	ch, ok := ctrl.stage.Load(key)
	if !ok {
		return NoStagedTaskError
	}

	// safety nil buffer to prevent deadlock
	ch.(chan *Task) <- nil

	if task := <-ch.(chan *Task); task != nil {
		if ctrl.resources[key].Status == ResourceLocked {
			<-ch.(chan *Task)
			ch.(chan *Task) <- task
			return ResourceUnavailableError
		}
		ctrl.stage.Delete(key)
		if task.Status == StatusStarted {
			return TaskAlreadyStartedError
		}
		ctrl.resources[key].Status = ResourceLocked
		task.Status = StatusStarted
		if _, err := taskModel.Save(task); err != nil {
			return err
		}
		if _, err := resourceModel.Save(ctrl.resources[key]); err != nil {
			return err
		}

		meta := make(map[string]interface{})
		json.Unmarshal(task.Meta, &meta)
		meta["_status"] = StatusStarted
		meta["_id"] = task.Id
		data, _ := json.Marshal(meta)
		ctrl.Notify(NewEvent(TaskStatusChangedEvent, data))
		log.Printf("started task [%s] with resource [%s]\n", task, key)

		return nil
	}

	return NoStagedTaskError
}

// StageTask adds the pending task to the associated task stage key.
func (ctrl *ResourceController) StageTask(task *Task, taskModel Model, changeStatus bool) {
	_, ok := ctrl.stage.Load(task.Key)
	if !ok {
		if changeStatus {
			task.ChangeStatus(taskModel, StatusPending)
		}
		ch := make(chan *Task, StageBuffer)
		ch <- task
		ctrl.stage.Store(task.Key, ch)

		meta := make(map[string]interface{})
		json.Unmarshal(task.Meta, &meta)
		meta["_status"] = StatusPending
		meta["_id"] = task.Id
		meta["_key"] = task.Key
		data, _ := json.Marshal(meta)
		if err := ctrl.Notify(NewEvent(TaskStatusChangedEvent, data)); err != nil {
			log.Println(err)
		}
		log.Printf("staged task [%s]\n", task)
	}
}

// StartStageLoop pulls tasks from the timetable and priority queues
// and stages them for completion.
func (ctrl *ResourceController) StartStageLoop(taskModel Model) {
	for {
		for key := range ctrl.resources {
			if _, ok := ctrl.stage.Load(key); ok {
				continue
			}
			task, _ := ctrl.stageScheduledTask(key)
			if task == nil {
				task, _ = ctrl.stageQueuedTask(key)
			}
			if task != nil {
				q := fmt.Sprintf(`FOR t IN %s FILTER t._key == @key RETURN t`, CollectionTasks)
				tasks, err := taskModel.Query(q, map[string]interface{}{"key": task.Id})
				if err != nil {
					log.Println(err)
					continue
				}
				if len(tasks) < 1 {
					log.Println(TaskNotFoundError, task)
					continue
				}
				task := tasks[0].(*Task)
				ctrl.StageTask(task, taskModel, true)
			}
		}

		time.Sleep(time.Second * 1)
	}
}

// stageQueuedTask fetches the next task from the priorty queue.
func (ctrl *ResourceController) stageQueuedTask(key string) (*Task, error) {
	params := map[string]interface{}{"key": key}
	result, errObj := ctrl.broker.Call(PriorityQueueHost, "pop", params)
	if errObj != nil {
		return nil, errors.New(string(errObj.Message))
	}
	var task *Task
	if result != nil {
		mapstructure.Decode(result.(map[string]interface{}), &task)
	}
	return task, nil
}

// stageScheduledTask fetches the next scheduled task from the timetable.
func (ctrl *ResourceController) stageScheduledTask(key string) (*Task, error) {
	params := map[string]interface{}{"key": key}
	result, errObj := ctrl.broker.Call(TimetableHost, "next", params)
	if errObj != nil {
		return nil, errors.New(string(errObj.Message))
	}
	var task *Task
	if result != nil {
		delete(result.(map[string]interface{}), "runAt")
		mapstructure.Decode(result.(map[string]interface{}), &task)
	}
	return task, nil
}
