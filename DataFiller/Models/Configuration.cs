using System.Text.Json.Serialization;

namespace DataFiller.Models
{
    public class Configuration
    {
        [JsonPropertyName("connectionString")]
        public string ConnectionString { get; set; } = string.Empty;

        [JsonPropertyName("batchSize")]
        public int BatchSize { get; set; } = 1000;

        [JsonPropertyName("tableMappings")]
        public Dictionary<string, int> TableMappings { get; set; } = new();
    }
}
