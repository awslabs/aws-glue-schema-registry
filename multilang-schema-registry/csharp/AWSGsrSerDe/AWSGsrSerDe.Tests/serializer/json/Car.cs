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

using System;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using NJsonSchema;
using NJsonSchema.Annotations;

namespace AWSGsrSerDe.Tests.serializer.json
{
    [Display(
        Name = "Simple Car Schema",
        Description = "This is a car"
    )]
    
    public class Car
    {
        [Required] public string make { get; set; }

        [Required] public string model { get; set; }

        [DefaultValue(true)] public bool used { get; set; }

        [Range(0, 200000)]
        [JsonSchema(JsonObjectType.Integer)]
        public int miles { get; set; }

        public DateTime purchaseDate { get; set; }

        public DateTime listedDate { get; set; }

        [NotNull] public string[] owners { get; set; }
        [NotNull] public float[] serviceCheckes { get; set; }
    }
}
