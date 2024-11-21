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
        private Table _progressTable;
        private readonly Stopwatch _stopwatch;
        private DateTime _lastDisplayUpdate = DateTime.MinValue;

        public DataFillerService(DbService dbService, Configuration config)
        {
            _dbService = dbService;
            _config = config;
            _random = new Random();
            _primaryKeys = new HashSet<string>();
            _tableProgress = new ConcurrentDictionary<string, (int, int, double)>();
            InitializeProgressTable();
            _stopwatch = new Stopwatch();
        }

        private void InitializeProgressTable()
        {
            _progressTable = new Table()
                .Border(TableBorder.Rounded)
                .Expand()
                .Title("[blue]Data Fill Progress[/]");

            _progressTable.AddColumn(new TableColumn("Category").Centered());
            _progressTable.AddColumn(new TableColumn("Details").Centered());
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
            
            var tableProgress = processedTables * 100.0 / totalTables;
            var recordProgress = processedRecords * 100.0 / totalRecords;
            
            // Add overall progress with color coding
            _progressTable.AddRow(
                new Markup("[bold]Tables Progress[/]"),
                new BarChart()
                    .Width(40)
                    .AddItem("Progress", tableProgress, Color.Blue));
            
            _progressTable.AddRow(
                new Text("Tables Count"),
                new Markup($"[green]{processedTables}[/]/[blue]{totalTables}[/] ([yellow]{tableProgress:F1}%[/])"));
            
            _progressTable.AddRow(
                new Markup("[bold]Records Progress[/]"),
                new BarChart()
                    .Width(40)
                    .AddItem("Progress", recordProgress, Color.Green));
            
            _progressTable.AddRow(
                new Text("Records Count"),
                new Markup($"[green]{processedRecords:N0}[/]/[blue]{totalRecords:N0}[/] ([yellow]{recordProgress:F1}%[/])"));

            // Time information
            _progressTable.AddRow(
                new Markup("[bold]Time Information[/]"),
                new Panel(new Rows(
                    new Markup($"[blue]Elapsed:[/] {elapsed.Hours:D2}:{elapsed.Minutes:D2}:{elapsed.Seconds:D2}"),
                    new Markup($"[green]Remaining:[/] {estimatedRemaining.Hours:D2}:{estimatedRemaining.Minutes:D2}:{estimatedRemaining.Seconds:D2}")
                )));

            // Active tables progress
            var activeTablesTable = new Table()
                .Border(TableBorder.Rounded)
                .Title("[blue]Active Tables (Top 10)[/]")
                .AddColumn(new TableColumn("Table").Width(20))
                .AddColumn(new TableColumn("Progress").Width(30))
                .AddColumn(new TableColumn("Count").Width(20));

            var activeTableProgress = _tableProgress
                .OrderByDescending(x => x.Value.Progress)
                .Take(10);

            foreach (var progress in activeTableProgress)
            {
                var progressColor = progress.Value.Progress switch
                {
                    >= 90 => "[green]",
                    >= 60 => "[yellow]",
                    >= 30 => "[blue]",
                    _ => "[red]"
                };

                var progressBar = new string('█', (int)(progress.Value.Progress / 5)) + new string('░', 20 - (int)(progress.Value.Progress / 5));
                
                activeTablesTable.AddRow(
                    new Markup($"[bold]{progress.Key}[/]"),
                    new Markup($"{progressColor}{progressBar}[/] {progress.Value.Progress:F1}%"),
                    new Markup($"[green]{progress.Value.Current:N0}[/]/[blue]{progress.Value.Target:N0}[/]"));
            }

            _progressTable.AddRow(
                new Markup("[bold]Active Tables[/]"),
                new Panel(activeTablesTable));

            AnsiConsole.Clear();
            AnsiConsole.Write(_progressTable);
        }

        public async Task FillDataAsync()
        {
            using var semaphore = new SemaphoreSlim(_config.ThreadCount);
            var tasks = new List<Task>();
            _stopwatch.Start();

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
                                    var batchData = GenerateBatchData(tableName, sourceData, batchSize);

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
                                        var elapsed = _stopwatch.Elapsed;
                                        var recordsRemaining = totalRecords - processedRecords;
                                        var recordsPerSecond = processedRecords / Math.Max(1, elapsed.TotalSeconds);
                                        var estimatedSecondsRemaining = recordsRemaining / Math.Max(1, recordsPerSecond);
                                        var estimatedTimeRemaining = TimeSpan.FromSeconds(estimatedSecondsRemaining);

                                        // Only update display if more than 1 second has passed since last update
                                        var now = DateTime.Now;
                                        if ((now - _lastDisplayUpdate).TotalSeconds >= 1)
                                        {
                                            UpdateProgressDisplay(
                                                processedTables,
                                                totalTables,
                                                processedRecords,
                                                totalRecords,
                                                elapsed,
                                                estimatedTimeRemaining);
                                            _lastDisplayUpdate = now;
                                        }
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

            _stopwatch.Stop();
            var finalElapsed = _stopwatch.Elapsed;
            AnsiConsole.MarkupLine($"[green]Data fill completed in {finalElapsed.Hours:D2}:{finalElapsed.Minutes:D2}:{finalElapsed.Seconds:D2}[/]");
        }

        public List<Dictionary<string, object>> GenerateBatchData(string tableName, List<Dictionary<string, object>> sourceData, int batchSize)
        {
            var connection = _dbService.CreateDbConnection();
            // 获取表的主键信息
            var tableInfo = connection.DbMaintenance.GetTableInfoList().FirstOrDefault(t => t.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase));
            if (tableInfo != null)
            {
                var primaryKeyColumns =connection.DbMaintenance.GetPrimaries(tableInfo.Name);
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
