/*
 * py_data_dictionary.c
 *
 *  Created on: Nov 8, 2023
 *      Author: tgburrin
 */

#include "py_data_dictionary.h"
#include "py_error.h"

PyObject *
datadictionarytype_display(DataDictionaryTypeObject *self, PyObject *args)
{
	print_data_dictionary(*(self->dd));
	Py_RETURN_NONE;
}

PyObject *
datadictionarytype_load_all_tables(DataDictionaryTypeObject *self, PyObject *args)
{
	if (!self->tables_loaded) {
		load_all_dd_tables(*(self->dd));
		self->tables_loaded = true;
		Py_RETURN_TRUE;
	} else {
		fprintf(stderr, "One or more tables is open\n");
		Py_RETURN_FALSE;
	}
}

static int
datadictionarytype_init (PyObject *self, PyObject *args, PyObject *kwargs) {
	char *dd_filename;
	data_dictionary_t **dd = NULL;
	DataDictionaryTypeObject *obj = (DataDictionaryTypeObject *)self;
	obj->tables_loaded = false;

	static char *kwlist[] = {"filename", NULL};
	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s", kwlist, &dd_filename))
		return -1;

	if ( (dd = build_dd_from_json(dd_filename)) == NULL ) {
		PyObject *err = get_invalid_data_error();
		PyErr_SetString(err, "Data Dictionary could not be loaded");
		return -1;
	} else
		obj->dd = dd;

	return 0;
}

PyObject *
datadictionarytype_tostr(PyObject *self) {
	char *loaded = ((DataDictionaryTypeObject *)self)->dd == NULL ? "False" : "True";
	char *tables_opened = ((DataDictionaryTypeObject *)self)->tables_loaded ?  "True" : "False";

	char repr[1024];
	sprintf(repr, "Data Dictionary:\nDD loaded: %s\nTables Opened: %s", loaded, tables_opened);
	return PyUnicode_FromString(repr);
}

static void
datadictionarytype_dealloc(DataDictionaryTypeObject *self)
{
	if ( self->tables_loaded && self->dd != NULL ) {
		close_all_dd_tables(*(self->dd));
		self->tables_loaded = false;
	}

	if ( self->dd != NULL ) {
		release_data_dictionary(self->dd);
		self->dd = NULL;
	}

	Py_TYPE(self)->tp_free((PyObject *) self);
}

static PyMethodDef DataDictionaryTypeMethods[] = {
    {"display",  (PyCFunction)datadictionarytype_display, METH_NOARGS, "prints the current data dictionary to stdout"},
	{"load_all_tables", (PyCFunction)datadictionarytype_load_all_tables, METH_NOARGS, "load the tables from the data dictionary"},
	{"tables_opened", (PyCFunction)NULL, METH_NOARGS, "the tables are opened"},
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

static PyTypeObject DataDictionaryType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "DataDictionary",
    .tp_doc = PyDoc_STR("A Data Dictionary Object"),
    .tp_basicsize = sizeof(DataDictionaryTypeObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HEAPTYPE | Py_TPFLAGS_HAVE_FINALIZE,
    //.tp_new = (newfunc) datadictionarytype_init,
	//.tp_finalize = (destructor) datadictionarytype_dealloc,
    .tp_init = (initproc) datadictionarytype_init,
	.tp_str = (reprfunc) datadictionarytype_tostr,
    .tp_dealloc = (destructor) datadictionarytype_dealloc,
    //.tp_members = NULL, // datadictionarytype_members,
    .tp_methods = DataDictionaryTypeMethods,
};

PyTypeObject *get_py_data_dictionary_type() {
	return &DataDictionaryType;
}

PyTypeObject *init_py_data_dictionary_type() {
	return get_py_data_dictionary_type();
}
