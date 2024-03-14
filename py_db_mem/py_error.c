/*
 * py_error.c
 *
 *  Created on: Nov 8, 2023
 *      Author: tgburrin
 */

#include "py_error.h"

static PyObject *InvalidDataError;

int init_invalid_data_error(PyObject *module) {
    InvalidDataError = PyErr_NewException("py_db_mem.InvalidDataError", NULL,NULL);
    Py_XINCREF(InvalidDataError);
    if (PyModule_AddObject(module,"InvalidDataError", InvalidDataError) != 0) {
		Py_XDECREF(InvalidDataError);
		return -1;
    }
    return 0;
}

PyObject *get_invalid_data_error() {
	return InvalidDataError;
}
