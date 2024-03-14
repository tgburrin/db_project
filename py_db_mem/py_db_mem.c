/*
 * py_db_mem.c
 *
 *  Created on: Oct 29, 2023
 *      Author: tgburrin
 *  https://stackoverflow.com/questions/74101498/cpython-c-c-extension-dealloc-never-called
 */

#include <stddef.h>
#include <stdbool.h>

#include <Python.h>
#include <datetime.h>

#include "py_data_dictionary.h"
#include "py_dd_table.h"
#include "py_dd_table_record.h"

#define VERSION "0.0.1"

static PyObject *
modul_version (PyObject *self, PyObject *args) {
	return PyUnicode_DecodeUTF8(VERSION, strlen(VERSION), NULL);
}

static PyMethodDef ModuleMethods[] = {
    {"version",  modul_version, METH_NOARGS, "Returns the module version."}, //METH_VARARGS
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

static struct PyModuleDef py_db_mem_module = {
    PyModuleDef_HEAD_INIT,
    "py_db_mem",   /* name of module */
    "A memory based tables", /* module documentation, may be NULL */
    -1,       /* size of per-interpreter state of the module,
                 or -1 if the module keeps state in global variables. */
	ModuleMethods
};

PyMODINIT_FUNC
PyInit_py_db_mem(void) {
	PyObject *module = NULL, *uuidmod;
	PyTypeObject *pytype = NULL;
	if ( (module = PyModule_Create(&py_db_mem_module)) == NULL)
		return NULL;

	PyDateTime_IMPORT;
	if (!PyDateTimeAPI) {
		PyErr_SetString(PyExc_ImportError, "datetime initialization failed");
		return NULL;
	}

	if ( init_invalid_data_error(module) != 0)
		return NULL;

	pytype = init_py_data_dictionary_type();
	if (PyType_Ready(pytype) != 0)
		return NULL;
	Py_XINCREF(pytype);

	if (PyModule_AddObject(module, "DataDictionary", (PyObject *)pytype) != 0) {
		fprintf(stderr, "Failed initializing DataDictionary object\n");
		Py_XDECREF(pytype);
		return NULL;
	}

	pytype = init_py_dd_table_type();
	if (PyType_Ready(pytype) != 0)
		return NULL;
	Py_XINCREF(pytype);

	if (PyModule_AddObject(module, "DDTable", (PyObject *)pytype) != 0) {
		fprintf(stderr, "Failed initializing DDTable object\n");
		Py_XDECREF(pytype);
		return NULL;
	}

	if ( (pytype = init_py_dd_table_record_type()) == NULL )
		return NULL;

	if (PyType_Ready(pytype) != 0)
		return NULL;
	Py_XINCREF(pytype);

	if (PyModule_AddObject(module, "DDTableRecord", (PyObject *)pytype) != 0) {
		fprintf(stderr, "Failed initializing DDTableRecord object\n");
		Py_XDECREF(pytype);
		return NULL;
	}

	uuidmod = PyImport_ImportModule("uuid");
	if (!uuidmod) {
		PyErr_Print();
		fprintf(stderr, "Error importing uuid module\n");
	}

	// This allocates resources, but there doesn't appear to be a dealloc for modules
	init_common();

	return module;
}
