// Copyright 2020 Amazon.com, Inc. or its affiliates.
// Licensed under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//  
//     http://www.apache.org/licenses/LICENSE-2.0
//  
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Linq;
using System.Text.Json.Nodes;
using NJsonSchema;

namespace AWSGsrSerDe.serializer.json
{
    /// <summary>
    /// Json validator
    /// </summary>
    public static class JsonValidator
    {
        /// <summary>
        /// Validates data against JsonSchema
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="dataNode"></param>
        /// <exception cref="AwsSchemaRegistryException"></exception>
        public static void ValidateDataWithSchema(JsonSchema schema, JsonNode dataNode)
        {
            var validationErrors = schema.Validate(dataNode.ToJsonString());
            if (validationErrors.Count > 0)
            {
                var errorMessages = validationErrors
                    .Select(validationError => validationError.Path + ": " + validationError.Kind)
                    .ToList();
                var errorMessage = string.Join("\n", errorMessages);
                throw new AwsSchemaRegistryException("Validation failed with: \n" + errorMessage);
            }
        }
    }
}
