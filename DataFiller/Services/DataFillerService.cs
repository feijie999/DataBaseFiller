using DataFiller.Models;
using Spectre.Console;

namespace DataFiller.Services
{
    public class DataFillerService
    {
        private readonly DbService _dbService;
        private readonly Configuration _config;
        private readonly Random _random;
        private HashSet<string> _primaryKeys;

        public DataFillerService(DbService dbService, Configuration config)
        {
            _dbService = dbService;
            _config = config;
            _random = new Random();
            _primaryKeys = new HashSet<string>();
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
                            var batchData = await GenerateBatchData(tableName, sourceData, batchSize);

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

        public async Task<List<Dictionary<string, object>>> GenerateBatchData(string tableName, List<Dictionary<string, object>> sourceData, int batchSize)
        {
            // 获取表的主键信息
            var tableInfo = _dbService._db.DbMaintenance.GetTableInfoList().FirstOrDefault(t => t.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase));
            if (tableInfo != null)
            {
                var primaryKeyColumns = _dbService._db.DbMaintenance.GetPrimaries(tableInfo.Name);
                _primaryKeys = new HashSet<string>(primaryKeyColumns.Select(pk => pk), StringComparer.OrdinalIgnoreCase);
            }

            var result = new List<Dictionary<string, object>>();
            for (int i = 0; i < batchSize; i++)
            {
                var randomIndex = _random.Next(sourceData.Count);
                var newRow = new Dictionary<string, object>(sourceData[randomIndex]);
                
                // 处理主键
                foreach (var key in newRow.Keys.ToList())
                {
                    if (_primaryKeys.Contains(key))
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
                        }else if (newRow[key] is Guid)
                        {
                            newRow[key] = Guid.NewGuid();
                        }
                    }
                }

                result.Add(newRow.ToDictionary(kvp => $"\"{kvp.Key}\"", kvp => kvp.Value));
            }

            // 随机排序
            return result.OrderBy(x => _random.Next()).ToList();
        }
    }
}
