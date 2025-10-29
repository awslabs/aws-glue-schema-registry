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

using System.Text;
using System.Text.Json.Nodes;
using AWSGsrSerDe.serializer.json;
using NJsonSchema;
using NUnit.Framework;

namespace AWSGsrSerDe.Tests.serializer.json
{
    [TestFixture]
    public class JsonValidatorTest
    {
        private readonly string _schemaString = "{\n"
                                                + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
                                                + "  \"description\": \"String schema\",\n"
                                                + "  \"type\": \"string\"\n"
                                                + "}";

        [Test]
        public void TestBinaryNode()
        {
            var bytes = Encoding.Default.GetBytes("\"Test String\"");
            var dataNode = JsonNode.Parse(bytes);

            var schema = JsonSchema.FromJsonAsync(_schemaString).Result;
            JsonValidator.ValidateDataWithSchema(schema, dataNode);
        }

        [Test]
        public void TestMissingNode()
        {
            var dataNode = new JsonObject();

            var schema = JsonSchema.FromJsonAsync(_schemaString).Result;

            Assert.Throws(
                typeof(AwsSchemaRegistryException),
                () => { JsonValidator.ValidateDataWithSchema(schema, dataNode); });
        }
    }
}
