#include "glue_schema_registry_memory_allocator.h"
#include "aws/common/allocator.h"

/*
 * Uses the default memory allocator from AWS SDK Common library to allocate /
 * free memory.
 */
void *aws_common_malloc(size_t size) {
    struct aws_allocator *allocator = aws_default_allocator();
    return aws_mem_acquire(allocator, size);
}

void *aws_common_calloc(size_t count, size_t size) {
    struct aws_allocator *allocator = aws_default_allocator();
    return aws_mem_calloc(allocator, count, size);
}

void aws_common_free(void *ptr) {
    struct aws_allocator *allocator = aws_default_allocator();
    aws_mem_release(allocator, ptr);
}
