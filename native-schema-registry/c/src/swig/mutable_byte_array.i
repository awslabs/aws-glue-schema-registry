%module GsrSerDe
%{
#include "mutable_byte_array.h"
%}

#if defined(SWIGPYTHON)
//Converts the unsigned char * to a Python Bytes object.
%typemap(out) mutable_byte_array * %{
        PyObject * obj = PyMemoryView_FromMemory((char *) $1->data, $1->max_len, PyBUF_READ);
        //Copy the contents to a Python byte array
        $result = PyBytes_FromObject(obj);
        //Release the memoryview object
        Py_CLEAR(obj);
        //Delete the underlying mutable_byte_array
        delete_mutable_byte_array($1);
%}
#endif

//Methods that map to the C implementation.
typedef struct mutable_byte_array {
    %extend {
        mutable_byte_array(size_t len);

        ~mutable_byte_array();

        unsigned char * get_data();

        size_t get_max_len();
    }
} mutable_byte_array;
