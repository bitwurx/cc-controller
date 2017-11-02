// Code generated by mockery v1.0.0
package main

import jrpc2 "github.com/bitwurx/jrpc2"
import mock "github.com/stretchr/testify/mock"

// MockServiceBroker is an autogenerated mock type for the ServiceBroker type
type MockServiceBroker struct {
	mock.Mock
}

// Call provides a mock function with given fields: _a0, _a1, _a2
func (_m *MockServiceBroker) Call(_a0 string, _a1 string, _a2 map[string]interface{}) (interface{}, *jrpc2.ErrorObject) {
	ret := _m.Called(_a0, _a1, _a2)

	var r0 interface{}
	if rf, ok := ret.Get(0).(func(string, string, map[string]interface{}) interface{}); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(interface{})
		}
	}

	var r1 *jrpc2.ErrorObject
	if rf, ok := ret.Get(1).(func(string, string, map[string]interface{}) *jrpc2.ErrorObject); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).(*jrpc2.ErrorObject)
		}
	}

	return r0, r1
}