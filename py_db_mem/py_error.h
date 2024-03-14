/*
 * py_error.h
 *
 *  Created on: Nov 5, 2023
 *      Author: tgburrin
 */

#ifndef PY_ERROR_H_
#define PY_ERROR_H_

#include <Python.h>

int init_invalid_data_error(PyObject *);
PyObject *get_invalid_data_error();

#endif /* PY_ERROR_H_ */
