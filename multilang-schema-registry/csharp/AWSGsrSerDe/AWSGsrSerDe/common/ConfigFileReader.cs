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
using System.Collections.Generic;
using System.IO;

namespace AWSGsrSerDe.common
{
    /// <summary>
    /// Utility class for reading configuration files and extracting key-value pairs.
    /// </summary>
    public static class ConfigFileReader
    {
        /// <summary>
        /// Reads a configuration file and returns the value for the specified key.
        /// Skips empty lines and lines starting with '#' (comments).
        /// </summary>
        /// <param name="configFilePath">Path to the configuration file</param>
        /// <param name="key">The key to search for (e.g., "dataFormat")</param>
        /// <returns>The value associated with the key, or null if not found</returns>
        /// <exception cref="ArgumentNullException">Thrown when configFilePath or key is null</exception>
        /// <exception cref="FileNotFoundException">Thrown when the configuration file is not found</exception>
        public static string GetConfigValue(string configFilePath, string key)
        {
            if (string.IsNullOrEmpty(configFilePath))
                throw new ArgumentNullException(nameof(configFilePath));
            
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));

            if (!File.Exists(configFilePath))
                throw new FileNotFoundException($"Configuration file not found: {configFilePath}");

            var keyPrefix = key + "=";
            var lines = File.ReadAllLines(configFilePath);
            
            foreach (var line in lines)
            {
                var trimmedLine = line.Trim();
                if (string.IsNullOrEmpty(trimmedLine) || trimmedLine.StartsWith("#"))
                    continue;
                    
                if (trimmedLine.StartsWith(keyPrefix, StringComparison.OrdinalIgnoreCase))
                {
                    return trimmedLine.Substring(keyPrefix.Length).Trim();
                }
            }
            
            return null;
        }

        /// <summary>
        /// Loads configuration from properties file and returns as dictionary.
        /// Skips empty lines and lines starting with '#' (comments).
        /// </summary>
        /// <param name="configFilePath">Path to the configuration file</param>
        /// <returns>Dictionary containing configuration key-value pairs</returns>
        /// <exception cref="ArgumentNullException">Thrown when configFilePath is null</exception>
        /// <exception cref="FileNotFoundException">Thrown when the configuration file is not found</exception>
        public static Dictionary<string, dynamic> LoadConfigurationDictionary(string configFilePath)
        {
            if (string.IsNullOrEmpty(configFilePath))
                throw new ArgumentNullException(nameof(configFilePath));

            if (!File.Exists(configFilePath))
                throw new FileNotFoundException($"Configuration file not found: {configFilePath}");
            
            var configDictionary = new Dictionary<string, dynamic>();
            var lines = File.ReadAllLines(configFilePath);
            
            foreach (var line in lines)
            {
                var trimmedLine = line.Trim();
                if (string.IsNullOrEmpty(trimmedLine) || trimmedLine.StartsWith("#"))
                    continue;
                    
                var equalIndex = trimmedLine.IndexOf('=');
                if (equalIndex > 0)
                {
                    var key = trimmedLine.Substring(0, equalIndex).Trim();
                    var value = trimmedLine.Substring(equalIndex + 1).Trim();
                    configDictionary[key] = value;
                }
            }
            
            return configDictionary;
        }
    }
}
