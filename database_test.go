package main

import (
	"flag"
	"fmt"
	"os"
	"testing"

	arango "github.com/arangodb/go-driver"
	arangohttp "github.com/arangodb/go-driver/http"
)

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Short() {
		InitDatabase()
	}
	result := m.Run()
	if !testing.Short() {
		// tearDownDatabase()
	}
	os.Exit(result)
}

func tearDownDatabase() {
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
	if db, err := client.Database(nil, name); err != nil {
		panic(err)
	} else {
		if err = db.Remove(nil); err != nil {
			panic(err)
		}
	}
}

func TestTaskStatModelCreate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	model := new(TaskStatModel)
	if err := model.Create(); err != nil {
		t.Fatal(err)
	}
}

func TestTaskStatModelSave(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	taskStat := NewTaskStat("key", 13.5)
	model := new(TaskStatModel)
	if _, err := model.Save(taskStat); err != nil {
		t.Fatal(err)
	}
}

func TestTaskStatModelQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	taskStat := NewTaskStat("key", 124040.5)
	model := new(TaskStatModel)
	if _, err := model.Save(taskStat); err != nil {
		t.Fatal(err)
	}
	q := fmt.Sprintf("FOR t IN %s RETURN t", CollectionTaskStats)
	taskStats, err := model.Query(q, make(map[string]interface{}))
	if err != nil {
		t.Fatal(err)
	}
	for _, taskStat := range taskStats {
		v, _ := taskStat.(*TaskStat)
		if v.RunTime == 124040.5 {
			return
		}
	}

	t.Fatal("expected task stat with run time 23.5 to exist")
}

func TestTaskModelCreate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	model := new(TaskModel)
	if err := model.Create(); err != nil {
		t.Fatal(err)
	}
}

func TestTaskModelQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	task := NewTask([]byte(`{"priority": 2.1}`))
	model := new(TaskModel)
	if _, err := model.Save(task); err != nil {
		t.Fatal(err)
	}
	q := fmt.Sprintf(`FOR t IN %s FILTER t._key == @key RETURN t`, CollectionTasks)
	tasks, err := model.Query(q, map[string]interface{}{"key": task.Id})
	if err != nil {
		t.Fatal(err)
	}
	task = tasks[0].(*Task)
	if task.Priority != 2.1 {
		t.Fatal("expected task priority to be 2.1")
	}
}

func TestTaskModelSave(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	task := NewTask([]byte(`{"meta": {"id": 123}, "Priority": 22.5, "key": "tb1"}`))
	model := new(TaskModel)
	if _, err := model.Save(task); err != nil {
		t.Fatal(err)
	}
	if _, err := model.Save(task); err != nil {
		t.Fatal(err)
	}
}

func TestResourceModelCreate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	model := new(ResourceModel)
	if err := model.Create(); err != nil {
		t.Fatal(err)
	}
}

func TestResourceModelSave(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	res := NewResource("test")
	model := new(ResourceModel)
	if _, err := model.Save(res); err != nil {
		t.Fatal(err)
	}
	if _, err := model.Save(res); err != nil {
		t.Fatal(err)
	}
}
