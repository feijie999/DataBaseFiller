using SqlSugar;
using DataFiller.Models;

namespace DataFiller.Services
{
    public class DbService
    {
        private readonly Configuration configuration;

        public DbService(Configuration config)
        {
            configuration = config;
        }

        public async Task<bool> ValidateConnectionAsync()
        {
            try
            {
                var _db = CreateDbConnection();
                var result = await _db.Ado.GetScalarAsync("SELECT 1");
                return result != null;
            }
            catch
            {
                return false;
            }
        }

        public async Task<bool> TableExistsAsync(string tableName)
        {
            var _db = CreateDbConnection();
            var tables = _db.DbMaintenance.GetTableInfoList();
            return tables.Any(t => t.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase));
        }
        public async Task<List<Dictionary<string, object>>> GetTableDataAsync(string tableName, int limit = 1000)
        {
            var _db = CreateDbConnection();
            var data = await _db.Queryable<dynamic>().AS($"\"{tableName}\"").Take(limit).ToListAsync();
            return data.Select(d => ((System.Dynamic.ExpandoObject)d).ToDictionary(
                kvp => kvp.Key,
                kvp => kvp.Value ?? DBNull.Value
            )).ToList();
        }

        public async Task<int> BulkInsertAsync<T>(string tableName, List<T> data) where T : class, new()
        {
            var _db = CreateDbConnection();
            return await _db.Insertable(data).AS($"\"{tableName}\"").ExecuteCommandAsync();
        }

        public SqlSugarClient CreateDbConnection()
        {
            return new SqlSugarClient(new ConnectionConfig
            {
                ConnectionString = configuration.ConnectionString,
                DbType = DbType.PostgreSQL,
                IsAutoCloseConnection = true,
                InitKeyType = InitKeyType.Attribute
            });
        }
    }
}
