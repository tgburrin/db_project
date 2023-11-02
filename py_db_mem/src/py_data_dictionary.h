/*
 * py_data_dictionary.h
 *
 *  Created on: Nov 5, 2023
 *      Author: tgburrin
 */

#ifndef PY_DATA_DICTIONARY_H_
#define PY_DATA_DICTIONARY_H_

#include <stdbool.h>

#include <Python.h>

#include <data_dictionary.h>
#include <db_interface.h>

#include "py_error.h"


typedef struct  {
    PyObject_HEAD
	data_dictionary_t **dd;
    bool tables_loaded;
} DataDictionaryTypeObject;

PyTypeObject *get_py_data_dictionary_type();
PyTypeObject *init_py_data_dictionary_type();

/*
 * https://docs.python.org/3/extending/newtypes_tutorial.html
 * https://stackoverflow.com/questions/55174837/python-c-extensions-adding-attributes-to-an-exception
 */
#endif
