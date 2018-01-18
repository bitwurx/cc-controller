package main

import (
	"github.com/bitwurx/jrpc2"
)

func main() {
	InitDatabase()
	s := jrpc2.NewServer(":8888", "/rpc")
	models := map[string]Model{
		"resources": &ResourceModel{},
		"tasks":     &TaskModel{},
	}
	NewApiV1(models, NewResourceController(&JsonRPCServiceBroker{}), s)
	s.Start()
}
