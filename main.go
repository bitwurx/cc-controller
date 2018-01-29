package main

import (
	"github.com/bitwurx/jrpc2"
)

func main() {
	InitDatabase()
	s := jrpc2.NewServer(":8080", "/rpc")
	models := map[string]Model{
		"resources": &ResourceModel{},
		"tasks":     &TaskModel{},
	}
	ctrl := NewResourceController(&JsonRPCServiceBroker{})
	NewApiV1(models, ctrl, s)
	go ctrl.StartStageLoop(models["tasks"])
	s.Start()
}
