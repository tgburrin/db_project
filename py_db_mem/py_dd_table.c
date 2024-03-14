/*
 * py_dd_table.c
 *
 *  Created on: Nov 9, 2023
 *      Author: tgburrin
 */

#include "py_dd_table.h"

#define OFFSETOF(x) offsetof(DDTableTypeObject, x)

PyObject *
ddtabletype_write_record(DDTableTypeObject *self, PyObject *args, PyObject *kwargs) {
	PyObject *recobjin = NULL;
	DDTableRecordTypeObject *recobj = NULL;

	if ( self->tbl == NULL )
		Py_RETURN_NONE;

	static char *kwlist[] = {"table_record", NULL};
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", kwlist, &recobjin))
		Py_RETURN_NONE;

	if (!PyObject_IsInstance(recobjin, (PyObject *)get_py_dd_table_record_type())) {
		PyObject *err = get_invalid_data_error();
		PyErr_SetString(err, "A record table object must be provided");
		Py_RETURN_NONE;
	} else
		recobj = (DDTableRecordTypeObject *)recobjin;

	if ( recobj->tbl == NULL || recobj->record_data == NULL ) {
		fprintf(stdout, "record object is not initialized or is empty\n");
		Py_RETURN_NONE;
	}

	if ( self->tbl->schema != recobj->tbl->schema ) {
		fprintf(stdout, "record object does not match the table object\n");
		Py_RETURN_NONE;
	}

	record_num_t rec_num = RECORD_NUM_MAX;
	insert_db_record(self->tbl, recobj->record_data, &rec_num);
	if ( rec_num == RECORD_NUM_MAX )
		Py_RETURN_NONE;

	PyObject *rv = PyLong_FromUnsignedLongLong(rec_num);
	Py_XINCREF(rv);
	return rv;
}

PyObject *
ddtabletype_read_into_record(DDTableTypeObject *self, PyObject *args, PyObject *kwargs) {
	PyObject *recobjout = NULL, *recnum = NULL;
	DDTableRecordTypeObject *recobj = NULL;

	if ( self->tbl == NULL )
		Py_RETURN_NONE;

	static char *kwlist[] = {"record_number", "storage_record", NULL};
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO", kwlist, &recnum, &recobjout))
		Py_RETURN_NONE;

	if (!PyLong_Check(recnum)) {
		PyObject *err = get_invalid_data_error();
		PyErr_SetString(err, "Record number must be an int");
		Py_RETURN_NONE;
	}

	if (!PyObject_IsInstance(recobjout, (PyObject *)get_py_dd_table_record_type())) {
		PyObject *err = get_invalid_data_error();
		PyErr_SetString(err, "A record table object must be provided");
		Py_RETURN_NONE;
	} else
		recobj = (DDTableRecordTypeObject *)recobjout;

	if ( recobj->tbl == NULL || recobj->record_data == NULL ) {
		PyObject *err = get_invalid_data_error();
		PyErr_SetString(err, "The record object is not initialized or is empty");
		Py_RETURN_NONE;
	}

	if ( self->tbl->schema != recobj->tbl->schema ) {
		PyObject *err = get_invalid_data_error();
		PyErr_SetString(err, "The record object does not match the table object");
		Py_RETURN_NONE;
	}

	fprintf(stdout, "Reading record from table\n");
	record_num_t rec_num = (record_num_t)PyLong_AsUnsignedLongLong(recnum);
	char *data = read_db_table_record(self->tbl->mapped_table, rec_num);
	if ( data != NULL ) {
		memcpy(recobj->record_data, data, self->tbl->schema->record_size);
		Py_RETURN_TRUE;
	} else
		Py_RETURN_FALSE;
}

PyObject *
ddtabletype_open_shm_table(PyObject *self, PyObject *args)
{
	DDTableTypeObject *obj = (DDTableTypeObject *)self;
	PyObject *rv = Py_False;
	if (obj->tbl == NULL) {
		PyObject *err = get_invalid_data_error();
		PyErr_SetString(err, "Table table object must be initialized");
		Py_INCREF(rv);
		return rv;
	}

	if ( open_shm_table(obj->tbl) == true ) {
		obj->tbl_opened = true;
		rv = Py_True;
		Py_INCREF(rv);
		return rv;
	}

	PyObject *err = get_invalid_data_error();
	PyErr_SetString(err, "Error opening shared memory table");
	Py_INCREF(rv);
	return rv;
}

PyObject *
ddtabletype_close_shm_table(PyObject *self, PyObject *args)
{
	DDTableTypeObject *obj = (DDTableTypeObject *)self;
	PyObject *rv = Py_False;
	if (obj->tbl == NULL) {
		Py_INCREF(rv);
		return rv;
	}

	if ( close_shm_table(obj->tbl) == true ) {
		obj->tbl_opened = false;
		rv = Py_True;
		Py_INCREF(rv);
		return rv;
	}

	PyObject *err = get_invalid_data_error();
	PyErr_SetString(err, "Error closing shared memory table");

	Py_INCREF(rv);
	return rv;
}

PyObject *
ddtabletype_new_record(PyObject *self, PyObject *args)
{
	DDTableTypeObject *obj = (DDTableTypeObject *)self;
	//PyObject *argList = Py_BuildValue("O", obj);
	PyObject *argList = PyTuple_New(1);
	PyTuple_SET_ITEM(argList, 0, obj);
	PyObject *tbl_record = PyObject_CallObject((PyObject *)get_py_dd_table_record_type(), argList);
	Py_XDECREF(argList);

	Py_INCREF(tbl_record);
	return tbl_record;
}

PyObject *
ddtabletype_find_records(DDTableTypeObject *self, PyObject *args, PyObject *kwargs) {
	// TODO Clean up this function to make it more strict and find multiple records
	PyObject *rv = PyList_New(0);
	DDTableRecordTypeObject *findRec = NULL;
	char *key_name = NULL, *sort_dir = NULL;
	//int8_t sd = 1;
	struct timespec start_tm, end_tm;
	float time_diff;

	static char *kwlist[] = {"find_record", "key_name", "sort_direction", NULL};
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "Os|s", kwlist, &findRec, &key_name, &sort_dir))
		Py_RETURN_NONE;

	db_index_t *idx = find_db_index(self->tbl, key_name);

	clock_gettime(CLOCK_REALTIME, &start_tm);
	db_indexkey_t *findkey = create_key_from_record_data(self->tbl->schema, idx->idx_schema, findRec->record_data);
	findkey->record = RECORD_NUM_MAX;

	db_indexkey_t *foundkey = NULL;
	if ( (foundkey = dbidx_find_record(idx, findkey)) != NULL ) {
		char *found_rec = read_db_table_record(self->tbl->mapped_table, foundkey->record);
		if ( found_rec != NULL ) {
			clock_gettime(CLOCK_REALTIME, &end_tm);
			time_diff = end_tm.tv_sec - start_tm.tv_sec;
			time_diff += (end_tm.tv_nsec / 1000000000.0) - (start_tm.tv_nsec / 1000000000.0);
			fprintf(stdout, "Lookup was %fus\n", time_diff * 1000000.0);

			PyObject *argList = PyTuple_New(1);
			PyTuple_SET_ITEM(argList, 0, self);
			PyObject *rec = PyObject_CallObject((PyObject *)get_py_dd_table_record_type(), argList);

			memcpy(((DDTableRecordTypeObject *)rec)->record_data, found_rec, self->tbl->schema->record_size);
			PyList_Append(rv, rec);
			Py_DECREF(rec);
		}
	}

	free(findkey);
	Py_INCREF(rv);
	return rv;
}

PyObject *
ddtabletype_print_index(DDTableTypeObject *self, PyObject *args, PyObject *kwargs) {
	char *index_name = NULL;
	db_index_t *idx = NULL;
	uint64_t idx_depth = 0;

	static char *kwlist[] = {"index_name", NULL};
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|s", kwlist, &index_name))
		Py_RETURN_NONE;

	if ( index_name != NULL ) {
		if ( (idx = find_db_index(self->tbl, index_name)) != NULL )
			dbidx_print_full_tree(idx, NULL, &idx_depth);

	} else {
		for ( uint8_t i = 0; i < self->tbl->num_indexes; i++ ) {
			idx = self->tbl->indexes[i];
			idx_depth = 0;
			dbidx_print_full_tree(idx, NULL, &idx_depth);
		}
	}
	Py_RETURN_NONE;
}

static int
ddtabletype_init (PyObject *self, PyObject *args, PyObject *kwargs) {
	PyObject *ddobj = NULL;
	data_dictionary_t **dd = NULL;
	db_table_t *tbl = NULL;
	char *tablename = NULL;
	DDTableTypeObject *obj = (DDTableTypeObject *)self;

	static char *kwlist[] = {"dd", "table_name", NULL};
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "Os", kwlist, &ddobj, &tablename))
		return -1;

	if (!PyObject_IsInstance(ddobj, (PyObject *)get_py_data_dictionary_type())) {
		PyObject *err = get_invalid_data_error();
		PyErr_SetString(err, "A data dictionary object must be provided");
		return -1;
	}

	dd = ((DataDictionaryTypeObject *)ddobj)->dd;
	if ( (tbl = find_db_table(dd, tablename)) == NULL ) {
		char errmsg[strlen("The table  could not be loaded ") + DB_OBJECT_NAME_SZ + 1];
		PyObject *err = get_invalid_data_error();
		sprintf(errmsg, "The table %s could not be loaded", tablename);
		//fprintf(stderr, "InvalidDataError: %p\n", err);
		PyErr_SetString(err, errmsg);
		return -1;
	} else
		fprintf(stdout, "Found table %s with %" PRIu8 " indexes\n", tbl->table_name, tbl->num_indexes);

	obj->dd = ddobj;
	obj->tblname = PyUnicode_FromString(tablename);
	obj->tbl = tbl;
	obj->tbl_opened = false;

	Py_XINCREF(obj->tblname);
	Py_XINCREF(obj->dd);

	return 0;
}

PyObject *
ddtabletype_tostr(PyObject *self) {
	char *loaded = ((DDTableTypeObject *)self)->tbl == NULL ? "False" : "True";
	const char *tblanem = ((DDTableTypeObject *)self)->tblname == NULL ? (const char *)"" : PyUnicode_AsUTF8(((DDTableTypeObject *)self)->tblname);
	char repr[2048 + DB_OBJECT_NAME_SZ];
	sprintf(repr, "Table:\nSchema loaded: %s\nName: %s\n", loaded, tblanem);

	return PyUnicode_FromString(repr);
}

static void
ddtabletype_dealloc(DDTableTypeObject *self)
{
	if ( self->tbl_opened && !close_dd_shm_table(self->tbl) )
		fprintf(stderr, "Error shutting shared mem table\n");

	Py_XDECREF(self->dd);
	Py_XDECREF(self->tblname);
	self->tbl = NULL;
	self->tbl_opened = false;

    Py_TYPE(self)->tp_free((PyObject *) self);
}

static PyMemberDef DDTableTypeMembers[] = {
		{"name", T_OBJECT, OFFSETOF(tblname), READONLY, "The name of the data dictionary table"},
		{NULL}
};

static PyMethodDef DDTableTypeMethods[] = {
		{"open_shm_table", (PyCFunction)ddtabletype_open_shm_table, METH_NOARGS, "opens a shared memory only table"},
		{"close_shm_table", (PyCFunction)ddtabletype_close_shm_table, METH_NOARGS, "closes a shared memory only table"},

		{"new_record", (PyCFunction)ddtabletype_new_record, METH_NOARGS, "creates a blank record for the table"},

		{"write_record", (PyCFunctionWithKeywords)ddtabletype_write_record, METH_VARARGS | METH_KEYWORDS, NULL},
		{"read_into_record", (PyCFunctionWithKeywords)ddtabletype_read_into_record, METH_VARARGS | METH_KEYWORDS, NULL},
		{"read_record", (PyCFunctionWithKeywords)NULL, METH_VARARGS | METH_KEYWORDS, NULL},
		{"remove_record", (PyCFunctionWithKeywords)NULL, METH_VARARGS | METH_KEYWORDS, NULL},

		{"find_records", (PyCFunctionWithKeywords)ddtabletype_find_records, METH_VARARGS | METH_KEYWORDS, NULL},

		{"print_index_tree", (PyCFunctionWithKeywords)ddtabletype_print_index, METH_VARARGS | METH_KEYWORDS, NULL},
		{NULL, NULL, 0, NULL}        /* Sentinel */
};

static PyTypeObject DDTableType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "DDTable",
    .tp_doc = PyDoc_STR("A Data Dictionary Table Object"),
    .tp_basicsize = sizeof(DDTableTypeObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HEAPTYPE | Py_TPFLAGS_HAVE_FINALIZE,
    //.tp_new = (newfunc) datadictionarytype_init,
	//.tp_finalize = (destructor) datadictionarytype_dealloc,
    .tp_init = (initproc) ddtabletype_init,
	.tp_str = (reprfunc) ddtabletype_tostr,
    .tp_dealloc = (destructor) ddtabletype_dealloc,
    .tp_methods = DDTableTypeMethods,
    .tp_members = DDTableTypeMembers,
};

PyTypeObject *get_py_dd_table_type() {
	return &DDTableType;
}

PyTypeObject *init_py_dd_table_type() {
    PyDateTime_IMPORT;

    if (!PyDateTimeAPI) {
        PyErr_SetString(PyExc_ImportError, "datetime initialization failed");
        return NULL;
    }

	return get_py_dd_table_type();
}
