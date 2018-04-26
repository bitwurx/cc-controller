# Concord Controller

Concord Controller (CC) handles user requests and manages the interactions of the individual components of the concord service.

The CC supports two modes of task scheduling.

**Priority**

Priority scheduling places a task in priority order. The priorty is represented as a floating point number with low values having the highest priority (ie. 1.4 has higher priority than 2.7). 0 is the lowest possible priority.

The CC tracks metrics of a given task using the task key. The last 10 run times of a task are recorded and averaged when scheduled for auto priority based on lowest run time.

**Point in Time**


### Usage
To build the docker image run:

`make build`

To run the full test suite run:

`make test`

To run the short (dependency free) test suite run:

`make test-short`

### Environment

**`CONCORD_PRIORITY_QUEUE_HOST`**

The `<host>:<port>` of the concord priority queue service.

**`CONCORD_TIMETABLE_HOST`**

The `<host>:<port>` of the concord timetable service.

**`CONCORD_STATUS_CHANGE_NOTIFIER_HOST`**

The `<host>:<port>` of the concord status change notifier service.

**`ARANGODB_HOST`**

The ArangoDB server url in the format `http://<host>:<port(default 8529)>`

*(example -> http://arango:8529)*

**`ARANGODB_NAME`**

The ArangoDB database name.

**`ARANGODB_USER`**

The ArangoDB username.

**`ARANGODB_PASS`**

The ArangoDB user password.

### JSON-RPC 2.0 HTTP API - Method Reference

This service uses the [JSON-RPC 2.0 Spec](http://www.jsonrpc.org/specification) over HTTP for its API.

---
#### addResource(name) : add a resource to be managed by concord
---

#### Returns:
(*Number*) 0 on success or -1 on failure

name - (*String*) the name of the resource.

---
#### addTask(key, meta, priority, runAt) : add a task to be run against a resource
---

#### Parameters:

key - (*String*) task resource key.

meta - (*Object*) user defined task data.

priority - (*Number*) a floating point number indicating the task priority.

runAt - (*String*) the task execution time as an RFC3339 formatted date/time string.

#### Returns:
(*String*) the id of the newly created task

---
#### completeTask(key, status) : complete a start task
---

#### Parameters:

id - (*String*) the id of the task.

status - (*Number*) the task completion status.

#### Returns:
(*Number*) 0 on success or -1 on failure

---
#### getTask(id) : get the task with the provided id
---

#### Parameters:

id - (*String*) the id of the task.

#### Returns:
(*Object*) the task object

---
#### listPriorityQueue(key) : list all tasks in the priority queue
---

#### Parameters:

key - (*String*) the priority queue key.

#### Returns:
(*Array*) the fetched priority queue

---
#### listTimetable(key) - list all tasks in the timetable
---

#### Parameters:

key - (*String*) the timetable key.

#### Returns:

(*Number*) the fetched timetable

---
#### removeTask(id) : remove a task
---

#### Parameters:

id - (*String*) the id of the task.

#### Returns:
(*Number*) 0 on success or -1 on failure


*This method only succeeds on tasks that are not yet started*