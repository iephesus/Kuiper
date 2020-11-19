package main

import (
	"fmt"
	"github.com/DataDog/go-python3"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xstream/api"
)

type pyfunc struct {
}

var PyStr = python3.PyUnicode_FromString
var GoStr = python3.PyUnicode_AsUTF8

func (f *pyfunc) Validate(args []interface{}) error {
	if len(args) < 3 {
		return fmt.Errorf("PythonFunc function only supports 3 or more parameters but got %d", len(args))
	}
	return nil
}

func (f *pyfunc) Exec(args []interface{}, _ api.FunctionContext) (interface{}, bool) {
	python3.Py_Initialize()
	if !python3.Py_IsInitialized() {
		return fmt.Errorf("error initializing the python interpreter"), false
	}
	m := ImportModule(fmt.Sprintf("%v", args[0]))
	if m == nil {
		return fmt.Errorf("could not import '%s' module", args[0]), false
	}
	pf := m.GetAttrString(fmt.Sprintf("%v", args[1]))
	if pf == nil {
		return fmt.Errorf("could not find '%s' function", args[1]), false
	}
	//TODO type conversion
	var result string
	if len(args) > 3 {
		fArgs := python3.PyTuple_New(len(args) - 3)
		for i:=3; i<len(args); i++{
			//TODO type conversion
			python3.PyTuple_SetItem(fArgs, i-3, PyStr(fmt.Sprintf("%v", args[i])))
		}
		//TODO exception handling
		res := pf.Call(fArgs, python3.Py_None)
		result = GoStr(res)
	}//TODO else

	return result, true
}

func (f *pyfunc) IsAggregate() bool {
	return false
}

func ImportModule(name string) *python3.PyObject {
	sysModule := python3.PyImport_ImportModule("sys")
	path := sysModule.GetAttrString("path")

	python3.PyList_Insert(path, 0, PyStr(common.Config.Python.SysModulePath))
	python3.PyList_Insert(path, 0, PyStr(common.Config.Python.CustomModulePath))
	return python3.PyImport_ImportModule(name)
}

var PythonFunc pyfunc