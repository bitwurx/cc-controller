package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/bitwurx/jrpc2"
)

const (
	AddTaskErrorCode            jrpc2.ErrorCode = -32003
	AddResourceErrorCode        jrpc2.ErrorCode = -32004
	CompleteTaskErrorCode       jrpc2.ErrorCode = -32005
	GetTaskErrorCode            jrpc2.ErrorCode = -32006
	ListPriorityQueueErrorCode  jrpc2.ErrorCode = -32007
	ListTimetableErrorCode      jrpc2.ErrorCode = -32008
	NotificationFailedErrorCode jrpc2.ErrorCode = -32009
	RemoveTaskErrorCode         jrpc2.ErrorCode = -32010
	StartTaskErrorCode          jrpc2.ErrorCode = -32011
)

const (
	AddTaskErrorMsg            jrpc2.ErrorMsg = "error adding new task"
	AddResourceErrorMsg        jrpc2.ErrorMsg = "error adding resource"
	CompleteTaskErrorMsg       jrpc2.ErrorMsg = "error completing task"
	GetTaskErrorMsg            jrpc2.ErrorMsg = "error getting task"
	ListPriorityQueueErrorMsg  jrpc2.ErrorMsg = "error listing priority queue"
	ListTimetableErrorMsg      jrpc2.ErrorMsg = "error list timetable"
	NotificationFailedErrorMsg jrpc2.ErrorMsg = "error sending notification"
	RemoveTaskErrorMsg         jrpc2.ErrorMsg = "error removing task"
	StartTaskErrorMsg          jrpc2.ErrorMsg = "error starting task"
)

type ApiV1 struct {
	models map[string]Model
	ctrl   Controller
}

type AddResourceParams struct {
	Name *string `json:"name"`
}

func (params *AddResourceParams) FromPositional(args []interface{}) error {
	if len(args) != 1 {
		return errors.New("name parameter is required")
	}
	name := args[0].(string)
	params.Name = &name

	return nil
}

func (api *ApiV1) AddResource(params json.RawMessage) (interface{}, *jrpc2.ErrorObject) {
	p := new(AddResourceParams)
	if err := jrpc2.ParseParams(params, p); err != nil {
		return nil, err
	}
	if p.Name == nil {
		return nil, &jrpc2.ErrorObject{
			Code:    jrpc2.InvalidParamsCode,
			Message: jrpc2.InvalidParamsMsg,
			Data:    "name is required",
		}
	}
	if err := api.ctrl.AddResource(*p.Name, api.models["resources"]); err != nil {
		return nil, &jrpc2.ErrorObject{
			Code:    AddResourceErrorCode,
			Message: AddResourceErrorMsg,
			Data:    err.Error(),
		}
	}
	return 0, nil
}

type AddTaskParams struct {
	Key      *string                 `json:"key"`
	Meta     *map[string]interface{} `json:"meta"`
	Priority *float64                `json:"priority"`
	RunAt    *string                 `json:"runAt"`
}

func (params *AddTaskParams) FromPositional(args []interface{}) error {
	if len(args) < 4 {
		return errors.New("key, meta, priority, and runAt paramters are required")
	}
	key := args[0].(string)
	meta := args[1].(map[string]interface{})
	priority := args[2].(float64)
	runAt := args[3].(string)
	params.Key = &key
	params.Meta = &meta
	params.Priority = &priority
	params.RunAt = &runAt

	return nil
}

func (api *ApiV1) AddTask(params json.RawMessage) (interface{}, *jrpc2.ErrorObject) {
	p := new(AddTaskParams)
	if err := jrpc2.ParseParams(params, p); err != nil {
		return nil, err
	}
	if p.Key == nil {
		return nil, &jrpc2.ErrorObject{
			Code:    jrpc2.InvalidParamsCode,
			Message: jrpc2.InvalidParamsMsg,
			Data:    "key is required",
		}
	}
	if p.Priority == nil && p.RunAt == nil {
		return nil, &jrpc2.ErrorObject{
			Code:    jrpc2.InvalidParamsCode,
			Message: jrpc2.InvalidParamsMsg,
			Data:    "priority or runAt is required",
		}
	}
	data, _ := json.Marshal(p)
	task := NewTask(data)
	if err := api.ctrl.AddTask(task, api.models["tasks"]); err != nil {
		return nil, &jrpc2.ErrorObject{
			Code:    AddTaskErrorCode,
			Message: AddTaskErrorMsg,
			Data:    err.Error(),
		}
	}
	return task.Id, nil
}

type StartTaskParams struct {
	Key *string `json:"key"`
}

func (params *StartTaskParams) FromPositional(args []interface{}) error {
	if len(args) != 1 {
		return errors.New("key parameter is required")
	}
	key := args[0].(string)
	params.Key = &key

	return nil
}

func (api *ApiV1) StartTask(params json.RawMessage) (interface{}, *jrpc2.ErrorObject) {
	p := new(StartTaskParams)
	if err := jrpc2.ParseParams(params, p); err != nil {
		return nil, err
	}
	if p.Key == nil {
		return -1, &jrpc2.ErrorObject{
			Code:    jrpc2.InvalidParamsCode,
			Message: jrpc2.InvalidParamsMsg,
			Data:    "key is required",
		}
	}
	if err := api.ctrl.StartTask(*p.Key, api.models["tasks"], api.models["resources"]); err != nil {
		return -1, &jrpc2.ErrorObject{
			Code:    StartTaskErrorCode,
			Message: StartTaskErrorMsg,
			Data:    err.Error(),
		}
	}
	return 0, nil
}

type CompleteTaskParams struct {
	Id     *string `json:"id"`
	Status *string `json:"status"`
}

func (params *CompleteTaskParams) FromPositional(args []interface{}) error {
	if len(args) != 2 {
		return errors.New("id, status parameters are required")
	}
	id := args[0].(string)
	status := args[1].(string)
	params.Id = &id
	params.Status = &status

	return nil
}

func (api *ApiV1) CompleteTask(params json.RawMessage) (interface{}, *jrpc2.ErrorObject) {
	p := new(CompleteTaskParams)
	if err := jrpc2.ParseParams(params, p); err != nil {
		return nil, err
	}
	if p.Id == nil {
		return nil, &jrpc2.ErrorObject{
			Code:    jrpc2.InvalidParamsCode,
			Message: jrpc2.InvalidParamsMsg,
			Data:    "id is required",
		}
	}
	if p.Status == nil {
		return nil, &jrpc2.ErrorObject{
			Code:    jrpc2.InvalidParamsCode,
			Message: jrpc2.InvalidParamsMsg,
			Data:    "status is required",
		}
	}
	if err := api.ctrl.CompleteTask(*p.Id, *p.Status, api.models["tasks"], api.models["resources"]); err != nil {
		return nil, &jrpc2.ErrorObject{
			Code:    CompleteTaskErrorCode,
			Message: CompleteTaskErrorMsg,
			Data:    err.Error(),
		}
	}
	return 0, nil
}

type GetTaskParams struct {
	Id *string `json:"id"`
}

func (params *GetTaskParams) FromPositional(args []interface{}) error {
	if len(args) != 1 {
		return errors.New("id parameter is required")
	}
	id := args[0].(string)
	params.Id = &id

	return nil
}

func (api *ApiV1) GetTask(params json.RawMessage) (interface{}, *jrpc2.ErrorObject) {
	p := new(GetTaskParams)
	if err := jrpc2.ParseParams(params, p); err != nil {
		return nil, err
	}
	if p.Id == nil {
		return nil, &jrpc2.ErrorObject{
			Code:    jrpc2.InvalidParamsCode,
			Message: jrpc2.InvalidParamsMsg,
			Data:    "id is required",
		}
	}
	task, err := api.ctrl.GetTask(*p.Id, api.models["tasks"])
	if err != nil {
		return nil, &jrpc2.ErrorObject{
			Code:    GetTaskErrorCode,
			Message: GetTaskErrorMsg,
			Data:    err.Error(),
		}
	}
	return task, nil
}

type ListPriorityQueueParams struct {
	Key *string `json:"key"`
}

func (params *ListPriorityQueueParams) FromPositional(args []interface{}) error {
	if len(args) != 1 {
		return errors.New("key parameter is required")
	}
	key := args[0].(string)
	params.Key = &key

	return nil
}

func (api *ApiV1) ListPriorityQueue(params json.RawMessage) (interface{}, *jrpc2.ErrorObject) {
	p := new(ListPriorityQueueParams)
	if err := jrpc2.ParseParams(params, p); err != nil {
		return nil, err
	}
	if p.Key == nil {
		return nil, &jrpc2.ErrorObject{
			Code:    jrpc2.InvalidParamsCode,
			Message: jrpc2.InvalidParamsMsg,
			Data:    "key is required",
		}
	}
	queue, err := api.ctrl.ListPriorityQueue(*p.Key)
	if err != nil {
		return nil, &jrpc2.ErrorObject{
			Code:    ListPriorityQueueErrorCode,
			Message: ListPriorityQueueErrorMsg,
			Data:    err.Error(),
		}
	}
	return queue, nil
}

type ListTimetableParams struct {
	Key *string `json:"key"`
}

func (params *ListTimetableParams) FromPositional(args []interface{}) error {
	if len(args) != 1 {
		return errors.New("key parameter is required")
	}
	key := args[0].(string)
	params.Key = &key

	return nil
}

func (api *ApiV1) ListTimetable(params json.RawMessage) (interface{}, *jrpc2.ErrorObject) {
	p := new(ListTimetableParams)
	if err := jrpc2.ParseParams(params, p); err != nil {
		return nil, err
	}
	if p.Key == nil {
		return nil, &jrpc2.ErrorObject{
			Code:    jrpc2.InvalidParamsCode,
			Message: jrpc2.InvalidParamsMsg,
			Data:    "key is required",
		}
	}
	queue, err := api.ctrl.ListTimetable(*p.Key)
	if err != nil {
		return nil, &jrpc2.ErrorObject{
			Code:    ListTimetableErrorCode,
			Message: ListTimetableErrorMsg,
			Data:    err.Error(),
		}
	}
	return queue, nil
}

type RemoveTaskParams struct {
	Id *string `json:"id"`
}

func (params *RemoveTaskParams) FromPositional(args []interface{}) error {
	if len(args) < 2 {
		return errors.New("key and id paramters are required")
	}
	id := args[3].(string)
	params.Id = &id

	return nil
}

func (api *ApiV1) RemoveTask(params json.RawMessage) (interface{}, *jrpc2.ErrorObject) {
	p := new(RemoveTaskParams)
	if err := jrpc2.ParseParams(params, p); err != nil {
		return nil, err
	}
	if p.Id == nil {
		return nil, &jrpc2.ErrorObject{
			Code:    jrpc2.InvalidParamsCode,
			Message: jrpc2.InvalidParamsMsg,
			Data:    "id is required",
		}
	}
	if err := api.ctrl.RemoveTask(*p.Id, api.models["tasks"]); err != nil {
		return nil, &jrpc2.ErrorObject{
			Code:    RemoveTaskErrorCode,
			Message: RemoveTaskErrorMsg,
			Data:    err.Error(),
		}
	}
	return 0, nil
}

func NewApiV1(models map[string]Model, ctrl Controller, s *jrpc2.Server) *ApiV1 {
	api := &ApiV1{models: models, ctrl: ctrl}
	resources, err := models["resources"].FetchAll()
	if err != nil {
		log.Fatal(err)
	}
	for _, resource := range resources {
		v, _ := resource.(*Resource)
		api.ctrl.AddResource(v.Name, models["resources"])
	}
	q := fmt.Sprintf("FOR t IN %s FILTER t.status == 'pending' RETURN t", CollectionTasks)
	tasks, err := models["tasks"].Query(q, map[string]interface{}{})
	for _, task := range tasks {
		v, _ := task.(*Task)
		api.ctrl.StageTask(v, models["tasks"], false)
	}

	s.Register("addResource", jrpc2.Method{Method: api.AddResource})
	s.Register("addTask", jrpc2.Method{Method: api.AddTask})
	s.Register("completeTask", jrpc2.Method{Method: api.CompleteTask})
	s.Register("getTask", jrpc2.Method{Method: api.GetTask})
	s.Register("listPriorityQueue", jrpc2.Method{Method: api.ListPriorityQueue})
	s.Register("listTimetable", jrpc2.Method{Method: api.ListTimetable})
	s.Register("startTask", jrpc2.Method{Method: api.StartTask})
	s.Register("removeTask", jrpc2.Method{Method: api.StartTask})

	return api
}
