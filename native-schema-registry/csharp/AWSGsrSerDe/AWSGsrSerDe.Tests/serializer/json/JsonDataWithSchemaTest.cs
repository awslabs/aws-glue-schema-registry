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

using AWSGsrSerDe.serializer.json;
using NUnit.Framework;

namespace AWSGsrSerDe.Tests.serializer.json
{
    [TestFixture]
    public class JsonDataWithSchemaTest
    {
        [Test]
        public void TestEmptyValidJsonDataWithSchema()
        {
            var schema = "{\n" + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
                               + "  \"$id\": \"http://example.com/product.schema.json\",\n" +
                               "  \"title\": \"Product\",\n"
                               + "  \"description\": \"A product in the catalog\",\n" + "  \"type\": \"string\"\n" +
                               "}";
            var payload = string.Empty;
            var jsonDataWithSchema = JsonDataWithSchema.Build(schema, payload);
            
            Assert.NotNull(jsonDataWithSchema);
        }
        
        [Test]
        public void TestNullJsonDataWithSchema()
        {
            var schema = "{\n" + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
                               + "  \"$id\": \"http://example.com/product.schema.json\",\n" +
                               "  \"title\": \"Product\",\n"
                               + "  \"description\": \"A product in the catalog\",\n" + "  \"type\": \"string\"\n" +
                               "}";
            string payload = null;
            var jsonDataWithSchema = JsonDataWithSchema.Build(schema, payload);
            
            Assert.NotNull(jsonDataWithSchema);
        }
    }
}
