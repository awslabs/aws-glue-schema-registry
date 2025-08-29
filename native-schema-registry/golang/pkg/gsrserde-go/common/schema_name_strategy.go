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

package common

// SchemaNameStrategy is the interface for schema naming strategies.
type SchemaNameStrategy interface {
	// GetSchemaName generates the schema name.
	GetSchemaName(data interface{}, topic string) string
}

// DefaultSchemaNameStrategy is the default schema naming strategy.
// It names the schema with the topic name.
type DefaultSchemaNameStrategy struct{}

// GetSchemaName returns the topic name as the schema name.
func (d *DefaultSchemaNameStrategy) GetSchemaName(data interface{}, topic string) string {
	return topic
}
