%module GsrSerDe

%include "exception.i"

//Defines an exception interceptor that checks if an exception happened during method execution and
//throws that exception in the target language.
//err_arg - Argument that represents the type `glue_schema_registry_error**` argument in the method definition.
//Conventions for arguments is arg1, arg2 etc.
//See https://www.swig.org/Doc3.0/Customization.html#Customization_exception on how this works.

%define %glue_schema_registry_exception_interceptor(err_arg) {
    //Initialize the error pointer holder argument. It is expected that the target language caller passes null here.
    err_arg = new_glue_schema_registry_error_holder();

    //Execute the method
    $action

    //Obtain the error pointer post execution.
    glue_schema_registry_error *err = *err_arg;

    //if the error is set to NULL, no exception occurred.
    if (err == NULL) {
        //Release the error pointer holder.
        delete_glue_schema_registry_error_holder(err_arg);
    } else {

        //Throw the exception
        if (err->msg == NULL || strlen(err->msg) == 0) {
            //Make sure memory is released before throwing exception.
            delete_glue_schema_registry_error_holder(err_arg);

            SWIG_exception(SWIG_RuntimeError, "Unknown exception occurred.");
        } else {
            //Temporarily copy the error_msg to stack before releasing the error pointers.
            char msg[MAX_ERROR_MSG_LEN];
            glue_schema_registry_error_get_msg(err, msg, sizeof msg);

            int err_code = err->code;

            //Make sure memory is released before throwing exception.
            delete_glue_schema_registry_error_holder(err_arg);

            if (err_code == ERR_CODE_NULL_PARAMETERS || err_code == ERR_CODE_INVALID_PARAMETERS) {
                SWIG_exception(SWIG_ValueError, msg);
            } else {
                SWIG_exception(SWIG_RuntimeError, msg);
            }
        }
    }
}
%enddef
