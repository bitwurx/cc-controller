package main

import (
	"errors"
	"testing"
)

func TestResourceAcquire(t *testing.T) {
	resc := NewResource("test")
	if resc.Status != ResourceFree {
		t.Fatal("expected resource to be free")
	}
	if err := resc.Acquire(); err != nil {
		t.Fatal(err)
	}
	if resc.Status != ResourceLocked {
		t.Fatal("expected resource to be locked")
	}
	if err := resc.Acquire(); err == nil {
		t.Fatal("expected already acquired error")
	}
}

func TestResourceRelease(t *testing.T) {
	resc := NewResource("test")
	resc.Acquire()
	if resc.Status != ResourceLocked {
		t.Fatal("expected resource to be locked")
	}
	if err := resc.Release(); err != nil {
		t.Fatal(err)
	}
	if resc.Status != ResourceFree {
		t.Fatal("expected resource to be free")
	}
	if err := resc.Release(); err == nil {
		t.Fatal("expected already free error")
	}
}

func TestResourceSave(t *testing.T) {
	testErr := errors.New("test")
	var table = []struct {
		Name     string
		ModelErr error
	}{
		{"test", nil},
		{"test", testErr},
	}
	for _, tt := range table {
		resc := NewResource(tt.Name)
		model := new(MockModel)
		model.On("Save", resc).Return(DocumentMeta{}, tt.ModelErr)
		if _, err := resc.Save(model); err != tt.ModelErr {
			t.Fatal(err)
		}
		model.AssertExpectations(t)
	}
}
