/*
 * py_table.h
 *
 *  Created on: Nov 9, 2023
 *      Author: tgburrin
 */

#ifndef PY_DD_TABLE_H_
#define PY_DD_TABLE_H_

#include <stdbool.h>

#include <Python.h>
#include "structmember.h"

#include <data_dictionary.h>
#include <db_interface.h>

#include "py_data_dictionary.h"
#include "py_dd_table_record.h"
#include "py_error.h"

typedef struct  {
	PyObject_HEAD
	PyObject *dd;
	PyObject *tblname;
	db_table_t *tbl;
	bool tbl_opened;
} DDTableTypeObject;

PyTypeObject *get_py_dd_table_type();
PyTypeObject *init_py_dd_table_type();


#endif /* PY_DD_TABLE_H_ */
