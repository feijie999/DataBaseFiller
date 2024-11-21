using DataFiller.Models;
using Spectre.Console;

namespace DataFiller.Services
{
    public class DataFillerService
    {
        private readonly DbService _dbService;
        private readonly Configuration _config;
        private readonly Random _random;

        public DataFillerService(DbService dbService, Configuration config)
        {
            _dbService = dbService;
            _config = config;
            _random = new Random();
        }

        public async Task FillDataAsync()
        {
            foreach (var mapping in _config.TableMappings)
            {
                var tableName = mapping.Key;
                var targetCount = mapping.Value;

                if (!await _dbService.TableExistsAsync(tableName))
                {
                    AnsiConsole.MarkupLine($"[red]Table {tableName} does not exist![/]");
                    continue;
                }

                await AnsiConsole.Progress()
                    .StartAsync(async ctx =>
                    {
                        var task = ctx.AddTask($"[green]Filling {tableName}[/]");

                        var sourceData = await _dbService.GetTableDataAsync(tableName);
                        if (!sourceData.Any())
                        {
                            AnsiConsole.MarkupLine($"[yellow]No data found in table {tableName}[/]");
                            return;
                        }

                        var currentCount = sourceData.Count;
                        var remainingCount = targetCount;
                        task.MaxValue = targetCount;

                        while (remainingCount > 0)
                        {
                            var batchSize = Math.Min(remainingCount, _config.BatchSize);
                            var batchData = GenerateBatchData(sourceData, batchSize);

                            try
                            {
                                await _dbService.BulkInsertAsync(tableName, batchData);
                                task.Increment(batchSize);
                                remainingCount -= batchSize;
                            }
                            catch (Exception ex)
                            {
                                AnsiConsole.MarkupLine($"[red]Error while inserting data into {tableName}: {ex.Message}[/]");
                                break;
                            }
                        }
                    });
            }
        }

        private List<Dictionary<string, object>> GenerateBatchData(List<Dictionary<string, object>> sourceData, int batchSize)
        {
            var result = new List<Dictionary<string, object>>();
            for (int i = 0; i < batchSize; i++)
            {
                var randomIndex = _random.Next(sourceData.Count);
                var newRow = new Dictionary<string, object>(sourceData[randomIndex]);
                
                // 处理主键和唯一索引
                foreach (var key in newRow.Keys.ToList())
                {
                    if (key.ToLower().Contains("id") || key.ToLower() == "key")
                    {
                        if (newRow[key] is int intValue)
                        {
                            newRow[key] = intValue + _random.Next(1000000);
                        }
                        else if (newRow[key] is long longValue)
                        {
                            newRow[key] = longValue + _random.Next(1000000);
                        }
                        else if (newRow[key] is string)
                        {
                            newRow[key] = Guid.NewGuid().ToString();
                        }
                    }
                }

                result.Add(newRow);
            }

            // 随机排序
            return result.OrderBy(x => _random.Next()).ToList();
        }
    }
}