using System.Text.Json.Serialization;
using System.Collections.Generic;

namespace DataFiller.Models
{
    public class Configuration
    {
        [JsonPropertyName("connectionString")]
        public string ConnectionString { get; set; } = string.Empty;

        [JsonPropertyName("batchSize")]
        public int BatchSize { get; set; } = 1000;

        [JsonPropertyName("threadCount")]
        public int ThreadCount { get; set; } = 10;

        [JsonPropertyName("tableMappings")]
        public Dictionary<string, int> TableMappings { get; set; } = new();
    }
}
