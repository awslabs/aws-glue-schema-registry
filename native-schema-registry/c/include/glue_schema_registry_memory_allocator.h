#ifndef GLUE_SCHEMA_REGISTRY_MEMORY_ALLOCATOR_H
#define GLUE_SCHEMA_REGISTRY_MEMORY_ALLOCATOR_H

#include <stdlib.h>

/*
 * Wrapper over AWS SDK Common memory allocator.
 */

void *aws_common_malloc(size_t size);

void *aws_common_calloc(size_t count, size_t size);

void aws_common_free(void *ptr);

#endif /* GLUE_SCHEMA_REGISTRY_MEMORY_ALLOCATOR_H */
