using DataFiller.Models;
using Spectre.Console;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace DataFiller.Services
{
    public class DataFillerService
    {
        private readonly DbService _dbService;
        private readonly Configuration _config;
        private readonly Random _random;
        private HashSet<string> _primaryKeys;
        private readonly ConcurrentDictionary<string, (int Current, int Target, double Progress)> _tableProgress;
        private readonly Status _status;
        private Table _progressTable;

        public DataFillerService(DbService dbService, Configuration config)
        {
            _dbService = dbService;
            _config = config;
            _random = new Random();
            _primaryKeys = new HashSet<string>();
            _tableProgress = new ConcurrentDictionary<string, (int, int, double)>();
            _status = new Status(AnsiConsole.Console);
            _status.AutoRefresh = true;
            InitializeProgressTable();
        }

        private void InitializeProgressTable()
        {
            _progressTable = new Table()
                .Border(TableBorder.Rounded)
                .Expand();

            _progressTable.AddColumn("Status");
            _progressTable.AddColumn("Value");
        }

        private void UpdateProgressDisplay(
            int processedTables,
            int totalTables,
            int processedRecords,
            int totalRecords,
            TimeSpan elapsed,
            TimeSpan estimatedRemaining)
        {
            _progressTable.Rows.Clear();
            
            // Add overall progress
            _progressTable.AddRow(
                new Text("Tables Progress"),
                new Text($"{processedTables}/{totalTables} ({(processedTables * 100.0 / totalTables):F1}%)"));
            _progressTable.AddRow(
                new Text("Records Progress"),
                new Text($"{processedRecords:N0}/{totalRecords:N0} ({(processedRecords * 100.0 / totalRecords):F1}%)"));
            _progressTable.AddRow(
                new Text("Time Elapsed"),
                new Text($"{elapsed.Hours:D2}:{elapsed.Minutes:D2}:{elapsed.Seconds:D2}"));
            _progressTable.AddRow(
                new Text("Estimated Remaining"),
                new Text($"{estimatedRemaining.Hours:D2}:{estimatedRemaining.Minutes:D2}:{estimatedRemaining.Seconds:D2}"));

            // Add active tables progress
            var activeTablesTable = new Table()
                .AddColumn("Table")
                .AddColumn("Progress")
                .AddColumn("Status")
                .Border(TableBorder.Rounded);

            var activeTableProgress = _tableProgress
                .OrderByDescending(x => x.Value.Progress)
                .Take(10);

            foreach (var progress in activeTableProgress)
            {
                var progressText = new Markup($"[green]{new string('=', (int)(progress.Value.Progress / 5))}[/][grey]{new string('-', 20 - (int)(progress.Value.Progress / 5))}[/] {progress.Value.Progress:F1}%");

                activeTablesTable.AddRow(
                    new Text(progress.Key),
                    progressText,
                    new Text($"{progress.Value.Current:N0}/{progress.Value.Target:N0}"));
            }

            _progressTable.AddRow(new Text("Active Tables"), new Panel(activeTablesTable));

            // Update the status
        }

        public async Task FillDataAsync()
        {
            using var semaphore = new SemaphoreSlim(10);
            var tasks = new List<Task>();
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            var totalTables = _config.TableMappings.Count;
            var processedTables = 0;
            var totalRecords = _config.TableMappings.Sum(x => x.Value);
            var processedRecords = 0;

            AnsiConsole.MarkupLine($"[blue]Starting data fill process for {totalTables} tables, total records to generate: {totalRecords:N0}[/]");

            await AnsiConsole.Status()
                .StartAsync("Processing...", async ctx =>
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

                        await semaphore.WaitAsync();

                        _tableProgress[tableName] = (0, targetCount, 0);

                        var task = Task.Run(async () =>
                        {
                            try
                            {
                                var sourceData = await _dbService.GetTableDataAsync(tableName);
                                if (!sourceData.Any())
                                {
                                    AnsiConsole.MarkupLine($"[yellow]No data found in table {tableName}[/]");
                                    return;
                                }

                                var remainingCount = targetCount;
                                var processedCount = 0;

                                while (remainingCount > 0)
                                {
                                    var batchSize = Math.Min(remainingCount, _config.BatchSize);
                                    var batchData = await GenerateBatchData(tableName, sourceData, batchSize);

                                    try
                                    {
                                        await _dbService.BulkInsertAsync(tableName, batchData);
                                        remainingCount -= batchSize;
                                        processedCount += batchSize;
                                        
                                        // Update table progress
                                        _tableProgress[tableName] = (processedCount, targetCount, (processedCount * 100.0 / targetCount));
                                        
                                        // Update overall progress
                                        Interlocked.Add(ref processedRecords, batchSize);
                                        
                                        // Calculate time estimates
                                        var elapsed = stopwatch.Elapsed;
                                        var recordsRemaining = totalRecords - processedRecords;
                                        var recordsPerSecond = processedRecords / Math.Max(1, elapsed.TotalSeconds);
                                        var estimatedSecondsRemaining = recordsRemaining / Math.Max(1, recordsPerSecond);
                                        var estimatedTimeRemaining = TimeSpan.FromSeconds(estimatedSecondsRemaining);

                                        // Update display
                                        UpdateProgressDisplay(
                                            processedTables,
                                            totalTables,
                                            processedRecords,
                                            totalRecords,
                                            elapsed,
                                            estimatedTimeRemaining);
                                    }
                                    catch (Exception ex)
                                    {
                                        AnsiConsole.MarkupLine($"[red]Error while inserting data into {tableName}: {ex.Message}[/]");
                                        break;
                                    }
                                }

                                Interlocked.Increment(ref processedTables);
                                // Remove completed table from progress tracking
                                _tableProgress.TryRemove(tableName, out _);
                            }
                            finally
                            {
                                semaphore.Release();
                            }
                        });

                        tasks.Add(task);
                    }

                    await Task.WhenAll(tasks);
                });

            stopwatch.Stop();
            var finalElapsed = stopwatch.Elapsed;
            AnsiConsole.MarkupLine($"[green]Data fill completed in {finalElapsed.Hours:D2}:{finalElapsed.Minutes:D2}:{finalElapsed.Seconds:D2}[/]");
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
