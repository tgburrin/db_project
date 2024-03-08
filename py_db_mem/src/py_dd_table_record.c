/*
 * py_dd_table_record.c
 *
 *  Created on: Nov 11, 2023
 *      Author: tgburrin
 */

#include "py_dd_table_record.h"

#define OFFSETOF(x) offsetof(DDTableRecordTypeObject, x)


PyObject *
ddtablerecordtype_getfieldlist(DDTableRecordTypeObject *self, PyObject *args) {
	PyObject *rv = PyList_New(0);
	if ( self->tbl != NULL ) {
		dd_table_schema_t *s = self->tbl->schema;
		for(uint8_t i = 0; i < s->num_fields; i++) {
			PyObject *field = PyDict_New();
			dd_datafield_t *f = s->fields[i];
			PyObject *field_name = PyUnicode_FromString(f->field_name);
			Py_XINCREF(field_name);
			PyDict_SetItemString(field, "name", field_name);

			PyObject *field_type = PyUnicode_FromString(map_enum_to_name(f->fieldtype));
			Py_XINCREF(field_type);
			PyDict_SetItemString(field, "type", field_type);

			if ( f->fieldtype == DD_TYPE_STR || f->fieldtype == DD_TYPE_BYTES ) {
				PyObject *field_size = PyLong_FromUnsignedLong((unsigned long)f->field_sz);
				Py_XINCREF(field_size);
				PyDict_SetItemString(field, "size", field_size);
			}

			Py_XINCREF(field);
			PyList_Append(rv, field);
		}
	}
	Py_XINCREF(rv);
	return rv;
}

PyObject *
ddtablerecordtype_setfieldvalue(DDTableRecordTypeObject *self, PyObject *args, PyObject *kwargs) {
	char *field_name = NULL;
	char *v = NULL;
	dd_datafield_t *field = NULL;
	PyObject *value = NULL;

	if (!PyArg_ParseTuple(args, "sO", &field_name, &value))
		Py_RETURN_NONE;

	if (self->tbl != NULL && self->record_data != NULL) {
		if ( (field = get_db_table_field(self->tbl->schema, field_name)) == NULL )
			Py_RETURN_NONE;

		PyTypeObject *t = value->ob_type;

		/*
		if ( field->fieldtype >= I8 && field->fieldtype <= UI64 && PyLong_Check(value))
		if ( field->fieldtype == TIMESTAMP && PyObject_IsInstance(value, &PyDateTime_DateTime))
		*/
		if ( field->fieldtype == DD_TYPE_TIMESTAMP && PyDateTime_Check(value) ) {
			/*
			 * The following would make the timestamp a bit more strict
			 * For now a timezone naive ts will be interpreted as UTC
			 *
			 * (tz = PyObject_GetAttrString(value, "tzinfo")) != NULL
			 * PyObject_RichCompareBool(tz, PyDateTime_TimeZone_UTC, Py_EQ)
			 */
			PyObject *strdt = PyObject_CallMethod(value, "isoformat", NULL);
			v = (char *)PyUnicode_AsUTF8(strdt);
		} else if ( (field->fieldtype >= DD_TYPE_I8 && field->fieldtype <= DD_TYPE_UI64 && PyLong_Check(value)) ) {
			v = (char *)PyUnicode_AsUTF8(PyObject_Str(value));
			// we found a number now how to convert it to a string, messy, but easier

		} else if ( (field->fieldtype == DD_TYPE_UUID && strcmp(t->tp_name, map_enum_to_name(DD_TYPE_UUID)) == 0) ) {
			v = (char *)PyUnicode_AsUTF8(PyObject_Str(value));

		} else if ( PyUnicode_Check(value) ) {
			v = (char *)PyUnicode_AsUTF8(value);

		}

		if ( v != NULL ) {
			if ( set_db_table_record_field_str(self->tbl->schema, field_name, v, self->record_data) )
				Py_RETURN_TRUE;
			else
				Py_RETURN_FALSE;
		}
	}
	Py_RETURN_NONE;
}

PyObject *
ddtablerecordtype_getfieldvalue(DDTableRecordTypeObject *self, PyObject *args, PyObject *kwargs) {
	char *field_name = NULL, *field_value = NULL, *field_data = NULL;

	if (!PyArg_ParseTuple(args, "s", &field_name))
		Py_RETURN_NONE;

	if ( self->tbl != NULL && self->record_data != NULL ) {
		dd_table_schema_t *s = self->tbl->schema;
		dd_datafield_t *field = NULL;

		if ( (field = get_db_table_field(s, field_name)) == NULL )
			Py_RETURN_NONE;

		if ( !get_db_table_record_field_value_str_alloc(s, field_name, self->record_data, &field_data) ) {
			fprintf(stdout, "Could not retrieve field value from record\n");
			free(field_data);
			free(field_value);
			Py_RETURN_NONE;
		}
		if ( !dd_type_to_allocstr(field, field_data, &field_value) ) {
			fprintf(stdout, "Could not convert field value to str\n");
			free(field_data);
			free(field_value);
			Py_RETURN_NONE;
		}

		PyObject *rv = PyUnicode_FromString(field_value);
		free(field_data);
		free(field_value);
		return rv;
	}

	Py_RETURN_NONE;
}

PyObject *
ddtablerecordtype_print(DDTableRecordTypeObject *self, PyObject *args) {
	if (self->tbl != NULL && self->record_data != NULL)
		db_table_record_print(self->tbl->schema, self->record_data);
	Py_RETURN_NONE;
}

static int
ddtablerecordtype_init (DDTableRecordTypeObject *self, PyObject *args, PyObject *kwargs) {
	PyObject *tblobj = NULL;
	db_table_t *tbl = NULL;

	static char *kwlist[] = {"dd_table", NULL};
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", kwlist, &tblobj))
		return -1;

	if (!PyObject_IsInstance(tblobj, (PyObject *)get_py_dd_table_type())) {
		PyObject *err = get_invalid_data_error();
		PyErr_SetString(err, "A dd table object must be provided");
		return -1;
	}

	if ( (tbl = ((DDTableTypeObject *)tblobj)->tbl) == NULL )
		return -1;

	self->tblobj = tblobj;
	self->tbl = tbl;
	self->record_data = new_db_table_record(tbl->schema);

	Py_XINCREF(self->tblobj);

	return 0;
}

PyObject *
ddtablerecordtype_tostr(DDTableRecordTypeObject *self) {
	char *s = db_table_record_print_alloc(self->tbl->schema, self->record_data);
	PyObject *rv = PyUnicode_FromString(s);
	free(s);
	return rv;
}

PyObject *
ddtablerecordtype_torepr(DDTableRecordTypeObject *self) {
	char *s = db_table_record_print_line_alloc(self->tbl->schema, self->record_data);
	PyObject *rv = PyUnicode_FromString(s);
	free(s);
	return rv;
}

static void
ddtablerecordtype_dealloc(DDTableRecordTypeObject *self)
{
    if (self->tbl != NULL) {
    	if (self->record_data != NULL)
    		release_table_record(self->tbl->schema, self->record_data);
    	self->record_data = NULL;
    	self->tbl = NULL;
    }

	Py_XDECREF(self->tblobj);
    Py_TYPE(self)->tp_free((PyObject *) self);
}

static PyMemberDef DDTableRecordTypeMembers[] = {
		{NULL}
};

static PyMethodDef DDTableRecordTypeMethods[] = {
		{"list_fields", (PyCFunction)ddtablerecordtype_getfieldlist, METH_NOARGS, "list the fields and their types on the record"},
		{"set_value", (PyCFunction)ddtablerecordtype_setfieldvalue, METH_VARARGS | METH_KEYWORDS, "Set the value of a record field"},
		{"get_value", (PyCFunction)ddtablerecordtype_getfieldvalue, METH_VARARGS | METH_KEYWORDS, "Get the value of a record field"},
		{"print", (PyCFunction)ddtablerecordtype_print, METH_NOARGS, "list the fields and their types on the record"},
		{NULL, NULL, 0, NULL}        /* Sentinel */
};

static PyTypeObject DDTableRecordType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "DDTableRecord",
    .tp_doc = PyDoc_STR("A Data Dictionary Table Record Object"),
    .tp_basicsize = sizeof(DDTableRecordTypeObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HEAPTYPE,
    //.tp_new = (newfunc) datadictionarytype_init,
	//.tp_finalize = (destructor) datadictionarytype_dealloc,
    .tp_init = (initproc) ddtablerecordtype_init,
	.tp_repr = (reprfunc) ddtablerecordtype_torepr,
	.tp_str = (reprfunc) ddtablerecordtype_tostr,
    .tp_dealloc = (destructor) ddtablerecordtype_dealloc,
    .tp_methods = DDTableRecordTypeMethods,
    .tp_members = DDTableRecordTypeMembers,
};

PyTypeObject *get_py_dd_table_record_type() {
	return &DDTableRecordType;
}

PyTypeObject *init_py_dd_table_record_type() {
    PyDateTime_IMPORT;

    if (!PyDateTimeAPI) {
        PyErr_SetString(PyExc_ImportError, "datetime initialization failed");
        return NULL;
    }

    return get_py_dd_table_record_type();
}

