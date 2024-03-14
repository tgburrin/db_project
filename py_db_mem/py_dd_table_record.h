/*
 * py_dd_record.h
 *
 *  Created on: Nov 11, 2023
 *      Author: tgburrin
 */

#ifndef PY_DD_TABLE_RECORD_H_
#define PY_DD_TABLE_RECORD_H_

#include <Python.h>
#include <structmember.h>
#include <datetime.h>

#include <data_dictionary.h>
#include <db_interface.h>
#include <utils.h>

#include "py_data_dictionary.h"
#include "py_dd_table.h"
#include "py_error.h"


typedef struct  {
    PyObject_HEAD
	PyObject *tblobj;
	db_table_t *tbl;
    char *record_data;
} DDTableRecordTypeObject;

PyTypeObject *get_py_dd_table_record_type();
PyTypeObject *init_py_dd_table_record_type();

#endif /* PY_DD_TABLE_RECORD_H_ */
