package main

import (
	"fmt"
	"os"
	"time"

	arango "github.com/arangodb/go-driver"
	arangohttp "github.com/arangodb/go-driver/http"
)

const (
	CollectionResources = "resources"  // the name of the resources database collection.
	CollectionTasks     = "tasks"      // the name of the tasks database collection.
	CollectionTaskStats = "task_stats" // the name of the task stats database collection.
)

var db arango.Database // package local arango database instance.

// DocumentMeta contains meta data for an arango document
type DocumentMeta struct {
	Id arango.DocumentID
}

// Model contains methods for interacting with database collections.
type Model interface {
	Create() error
	FetchAll() ([]interface{}, error)
	Query(string, interface{}) ([]interface{}, error)
	Save(interface{}) (DocumentMeta, error)
}

// TaskStatModel represents a task stat collection model.
type TaskStatModel struct{}

// Create creates the task_stats collection and creates a persistent index on
// the Created field in the arangodb database.
func (model *TaskStatModel) Create() error {
	col, err := db.CreateCollection(nil, CollectionTaskStats, nil)
	if err != nil {
		if arango.IsConflict(err) {
			return nil
		}
		return err
	}
	_, _, err = col.EnsurePersistentIndex(nil, []string{"Created"}, nil)
	if err != nil {
		return err
	}
	return err
}

func (model *TaskStatModel) FetchAll() ([]interface{}, error) {
	return make([]interface{}, 0), nil
}

// Query runs the AQL query against the task stat model collection.
func (model *TaskStatModel) Query(q string, vars interface{}) ([]interface{}, error) {
	taskStats := make([]interface{}, 0)
	cursor, err := db.Query(nil, q, vars.(map[string]interface{}))
	if err != nil {
		return nil, err
	}
	defer cursor.Close()
	for {
		taskStat := new(TaskStat)
		_, err := cursor.ReadDocument(nil, taskStat)
		if arango.IsNoMoreDocuments(err) {
			break
		}
		if err != nil {
			return nil, err
		}
		taskStats = append(taskStats, taskStat)
	}
	return taskStats, nil
}

// Save creates a document in the task stats collection.
func (model *TaskStatModel) Save(taskStat interface{}) (DocumentMeta, error) {
	col, err := db.Collection(nil, CollectionTaskStats)
	if err != nil {
		return DocumentMeta{}, err
	}
	meta, err := col.CreateDocument(nil, taskStat)
	if err != nil {
		return DocumentMeta{}, err
	}
	return DocumentMeta{Id: meta.ID}, nil
}

// TaskModel represents a task collection model.
type TaskModel struct{}

// Create creates the tasks collection in the arangodb database.
func (model *TaskModel) Create() error {
	_, err := db.CreateCollection(nil, CollectionTasks, nil)
	if err != nil && arango.IsConflict(err) {
		return nil
	}
	return err
}

func (model *TaskModel) FetchAll() ([]interface{}, error) {
	return make([]interface{}, 0), nil
}

// Query runs the AQL query against the task model collection.
func (model *TaskModel) Query(q string, vars interface{}) ([]interface{}, error) {
	tasks := make([]interface{}, 0)
	cursor, err := db.Query(nil, q, vars.(map[string]interface{}))
	if err != nil {
		return nil, err
	}
	defer cursor.Close()
	for {
		task := new(Task)
		_, err := cursor.ReadDocument(nil, task)
		if arango.IsNoMoreDocuments(err) {
			break
		}
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

// Save creates a document in the tasks collection.
func (model *TaskModel) Save(task interface{}) (DocumentMeta, error) {
	var meta arango.DocumentMeta
	col, err := db.Collection(nil, CollectionTasks)
	if err != nil {
		return DocumentMeta{}, err
	}
	meta, err = col.CreateDocument(nil, task)
	if arango.IsConflict(err) {
		v, _ := task.(*Task)
		patch := map[string]interface{}{"status": v.Status}
		meta, err = col.UpdateDocument(nil, v.Id, patch)
		if err != nil {
			return DocumentMeta{}, err
		}
	} else if err != nil {
		return DocumentMeta{}, err
	}
	return DocumentMeta{Id: meta.ID}, nil
}

type ResourceModel struct{}

func (model *ResourceModel) Create() error {
	_, err := db.CreateCollection(nil, CollectionResources, nil)
	if err != nil && arango.IsConflict(err) {
		return nil
	}
	return err
}

func (model *ResourceModel) FetchAll() ([]interface{}, error) {
	resources := make([]interface{}, 0)
	query := fmt.Sprintf("FOR r in %s RETURN r", CollectionResources)
	cursor, err := db.Query(nil, query, nil)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()
	for {
		r := new(Resource)
		_, err := cursor.ReadDocument(nil, r)
		if arango.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return nil, err
		}
		resources = append(resources, r)
	}
	return resources, nil
}

func (model *ResourceModel) Query(q string, vars interface{}) ([]interface{}, error) {
	return make([]interface{}, 0), nil
}

func (model *ResourceModel) Save(res interface{}) (DocumentMeta, error) {
	var meta arango.DocumentMeta
	col, err := db.Collection(nil, CollectionResources)
	if err != nil {
		return DocumentMeta{}, err
	}
	meta, err = col.CreateDocument(nil, res)
	if arango.IsConflict(err) {
		v, _ := res.(*Resource)
		patch := map[string]interface{}{"status": v.Status}
		meta, err = col.UpdateDocument(nil, v.Name, patch)
		if err != nil {
			return DocumentMeta{}, err
		}
	} else if err != nil {
		return DocumentMeta{}, err
	}
	return DocumentMeta{Id: meta.ID}, nil
}

// InitDatabase connects to the arangodb and creates the collections from the
// provided models.
func InitDatabase() {
	host := os.Getenv("ARANGODB_HOST")
	name := os.Getenv("ARANGODB_NAME")
	user := os.Getenv("ARANGODB_USER")
	pass := os.Getenv("ARANGODB_PASS")

	conn, err := arangohttp.NewConnection(
		arangohttp.ConnectionConfig{Endpoints: []string{host}},
	)
	if err != nil {
		panic(err)
	}
	client, err := arango.NewClient(arango.ClientConfig{
		Connection:     conn,
		Authentication: arango.BasicAuthentication(user, pass),
	})
	if err != nil {
		panic(err)
	}

	for {
		if exists, err := client.DatabaseExists(nil, name); err == nil {
			if !exists {
				db, err = client.CreateDatabase(nil, name, nil)
			} else {
				db, err = client.Database(nil, name)
			}
			if err == nil {
				break
			}
		}
		fmt.Println(err)
		time.Sleep(time.Second * 1)
	}

	models := []Model{
		&TaskModel{},
		&TaskStatModel{},
		&ResourceModel{},
	}
	for _, model := range models {
		if err := model.Create(); err != nil {
			panic(err)
		}
	}
}
