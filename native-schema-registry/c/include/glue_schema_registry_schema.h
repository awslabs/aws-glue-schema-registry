#ifndef GLUE_SCHEMA_REGISTRY_SCHEMA_H
#define GLUE_SCHEMA_REGISTRY_SCHEMA_H

/*
 * Glue Schema Registry Schema structure that represents
 * schema object required by Glue Schema Registry Serializers / De-serializers.
 */
typedef struct glue_schema_registry_schema {
    //String name of the schema
    char * schema_name;

    //Complete definition of the schema as String
    char * schema_def;

    //Data format name, JSON, AVRO, PROTOBUF as String
    char * data_format;

} glue_schema_registry_schema;

//Creates a new instance of glue_schema_registry_schema
glue_schema_registry_schema *new_glue_schema_registry_schema(
    const char * schema_name,
    const char * schema_def,
    const char * data_format
);

//Deletes the glue schema registry schema.
void delete_glue_schema_registry_schema(glue_schema_registry_schema * gsr_schema);

//Gets different attributes from glue_schema_registry_schema instance.
//These getter methods are translated into "Getter" methods in target languages.
const char * glue_schema_registry_schema_get_schema_name(glue_schema_registry_schema * gsr_schema);

const char * glue_schema_registry_schema_get_schema_def(glue_schema_registry_schema * gsr_schema);

const char * glue_schema_registry_schema_get_data_format(glue_schema_registry_schema * gsr_schema);

#endif //GLUE_SCHEMA_REGISTRY_SCHEMA_H