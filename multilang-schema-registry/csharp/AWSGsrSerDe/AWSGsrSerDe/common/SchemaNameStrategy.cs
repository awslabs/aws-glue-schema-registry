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

#pragma warning disable SA1649
namespace AWSGsrSerDe.common
{
    /// <summary>
    /// Interface for Schema Naming strategy
    /// </summary>
    public interface ISchemaNameStrategy
    {
        /// <summary>
        /// Generates the schema name
        /// </summary>
        /// <param name="data">message object to serialize</param>
        /// <param name="topic">Kafka topic name</param>
        /// <returns>schema name</returns>
        string GetSchemaName(object data, string topic);
    }

    /// <summary>
    /// Default Schema Naming Strategy
    /// Name the Schema with topic name
    /// </summary>
    public class DefaultSchemaNameStrategy : ISchemaNameStrategy
    {
        /// <inheritdoc />
        public string GetSchemaName(object data, string topic)
        {
            return topic;
        }
    }
}