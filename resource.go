package main

import (
	"errors"
)

const (
	ResourceFree   ResourceStatus = iota // free resource status.
	ResourceLocked                = iota // locked resource status.
)

type ResourceStatus int // resource status type

// Resource is a unit required by a task that is managed by the controller.
type Resource struct {
	// Name is the name of resource.
	// Status indicates if the resource is locked or free.
	Name   string         `json:"_key"`
	Status ResourceStatus `json:"status"`
}

// NewResource creates a new resource and sets the default free status.
func NewResource(name string) *Resource {
	return &Resource{name, ResourceFree}
}

// Acquire puts the resource in the locked state.
func (resc *Resource) Acquire() error {
	if resc.Status == ResourceLocked {
		return errors.New("resource is already locked")
	}
	resc.Status = ResourceLocked
	return nil
}

// Release puts the resource in the released state.
func (resc *Resource) Release() error {
	if resc.Status == ResourceFree {
		return errors.New("resource is already free")
	}
	resc.Status = ResourceFree
	return nil
}

// Save create a new document for the resource in the database.
func (resc *Resource) Save(resourceModel Model) (DocumentMeta, error) {
	return resourceModel.Save(resc)
}
