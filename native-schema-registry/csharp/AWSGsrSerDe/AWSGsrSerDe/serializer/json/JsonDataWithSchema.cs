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

namespace AWSGsrSerDe.serializer.json
{
    /// <summary>
    /// Wrapper object that contains schema string and json data string
    /// This works similar to the notion of GenericRecord in Avro.
    /// This can be passed as an input to the serializer for json data format.
    /// </summary>
    public class JsonDataWithSchema
    {
        private JsonDataWithSchema(string schema, string payload)
        {
            Schema = schema;
            Payload = payload;
        }

        /// <summary>
        /// Json Schema string
        /// </summary>
        public string Schema { get; }

        /// <summary>
        /// json data/payload/document to be serialized
        /// </summary>
        public string Payload { get; }

        /// <summary>
        /// Build a new instance of the <see cref="JsonDataWithSchema"/> class.
        /// </summary>
        /// <param name="schema">json schema string</param>
        /// <param name="payload">json schema data</param>
        /// <returns> a json </returns>
        public static JsonDataWithSchema Build(string schema, string payload)
        {
            return new JsonDataWithSchema(schema, payload);
        }
    }
}
