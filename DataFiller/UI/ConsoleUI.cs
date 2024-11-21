using Spectre.Console;
using System.Text.Json;
using DataFiller.Models;
using DataFiller.Services;

namespace DataFiller.UI
{
    public class ConsoleUI
    {
        private Configuration _config = new();
        private DbService? _dbService;

        public async Task RunAsync()
        {
            AnsiConsole.Write(
                new FigletText("Data Filler")
                    .LeftJustified()
                    .Color(Color.Green));

            await ConfigureConnectionAsync();
            await ConfigureTablesAsync();
            await ConfigureBatchSizeAsync();
            await ConfigureThreadCountAsync();
            await StartFillingAsync();
        }

        private async Task ConfigureConnectionAsync()
        {
            var configFile = "config.json";
            if (File.Exists(configFile))
            {
                var useConfig = AnsiConsole.Confirm("Found existing configuration. Would you like to use it?");
                if (useConfig)
                {
                    try
                    {
                        var json = await File.ReadAllTextAsync(configFile);
                        _config = JsonSerializer.Deserialize<Configuration>(json) ?? new Configuration();
                        _dbService = new DbService(_config);
                        if (await _dbService.ValidateConnectionAsync())
                        {
                            AnsiConsole.MarkupLine("[green]Successfully connected to database![/]");
                            return;
                        }
                    }
                    catch
                    {
                        AnsiConsole.MarkupLine("[red]Error loading configuration file.[/]");
                    }
                }
            }

            while (true)
            {
                _config.ConnectionString = AnsiConsole.Prompt(
                    new TextPrompt<string>("Enter PostgreSQL connection string:")
                    .PromptStyle("green")
                    .Secret());

                _dbService = new DbService(_config);
                if (await _dbService.ValidateConnectionAsync())
                {
                    AnsiConsole.MarkupLine("[green]Successfully connected to database![/]");
                    
                    var saveConfig = AnsiConsole.Confirm("Would you like to save this configuration?");
                    if (saveConfig)
                    {
                        var json = JsonSerializer.Serialize(_config);
                        await File.WriteAllTextAsync(configFile, json);
                    }
                    break;
                }

                AnsiConsole.MarkupLine("[red]Failed to connect to database. Please try again.[/]");
            }
        }

        private async Task ConfigureTablesAsync()
        {
            while (true)
            {
                var input = AnsiConsole.Prompt(
                    new TextPrompt<string>("Enter tables and target rows (format: table1,1000;table2,2000):")
                    .PromptStyle("green"));

                var mappings = new Dictionary<string, int>();
                var isValid = true;

                foreach (var pair in input.Split(';', StringSplitOptions.RemoveEmptyEntries))
                {
                    var parts = pair.Split(',');
                    if (parts.Length != 2 || !int.TryParse(parts[1], out var count) || count <= 0)
                    {
                        AnsiConsole.MarkupLine("[red]Invalid format. Please use: table1,1000;table2,2000[/]");
                        isValid = false;
                        break;
                    }

                    var tableName = parts[0].Trim();
                    if (!await _dbService!.TableExistsAsync(tableName))
                    {
                        AnsiConsole.MarkupLine($"[red]Table {tableName} does not exist![/]");
                        isValid = false;
                        break;
                    }

                    mappings[tableName] = count;
                }

                if (isValid)
                {
                    _config.TableMappings = mappings;
                    break;
                }
            }
        }

        private async Task ConfigureBatchSizeAsync()
        {
            await Task.Run(() =>
            {
                _config.BatchSize = AnsiConsole.Prompt(
                    new TextPrompt<int>("Enter batch size (default: 1000):")
                    .PromptStyle("green")
                    .DefaultValue(1000)
                    .Validate(size =>
                    {
                        return size switch
                        {
                            <= 0 => ValidationResult.Error("[red]Batch size must be greater than 0[/]"),
                            > 10000 => ValidationResult.Error("[red]Batch size must not exceed 10000[/]"),
                            _ => ValidationResult.Success(),
                        };
                    }));
            });
        }

        private async Task ConfigureThreadCountAsync()
        {
            await Task.Run(() =>
            {
                _config.ThreadCount = AnsiConsole.Prompt(
                    new TextPrompt<int>("Enter thread count (default: 10):")
                    .PromptStyle("green")
                    .DefaultValue(10)
                    .Validate(count =>
                    {
                        return count switch
                        {
                            <= 0 => ValidationResult.Error("[red]Thread count must be greater than 0[/]"),
                            > 100 => ValidationResult.Error("[red]Thread count must not exceed 100[/]"),
                            _ => ValidationResult.Success(),
                        };
                    }));
            });
        }

        private async Task StartFillingAsync()
        {
            var service = new DataFillerService(_dbService!, _config);
            
            AnsiConsole.MarkupLine("\n[yellow]Starting data fill process...[/]\n");
            
            var startTime = DateTime.Now;
            await service.FillDataAsync();
            var duration = DateTime.Now - startTime;

            AnsiConsole.MarkupLine($"\n[green]Process completed in {duration.TotalSeconds:F2} seconds![/]");
        }
    }
}
