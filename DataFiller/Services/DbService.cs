using SqlSugar;
using DataFiller.Models;

namespace DataFiller.Services
{
    public class DbService
    {
        private readonly ISqlSugarClient _db;

        public DbService(Configuration config)
        {
            _db = new SqlSugarClient(new ConnectionConfig()
            {
                DbType = DbType.PostgreSQL,
                ConnectionString = config.ConnectionString,
                IsAutoCloseConnection = true
            });
        }

        public async Task<bool> ValidateConnectionAsync()
        {
            try
            {
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
            var tables = await _db.DbMaintenance.GetTableInfoListAsync();
            return tables.Any(t => t.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase));
        }

        public async Task<List<Dictionary<string, object>>> GetTableDataAsync(string tableName, int limit = 1000)
        {
            var data = await _db.Queryable<dynamic>().AS(tableName).Take(limit).ToListAsync();
            return data.Select(d => ((System.Dynamic.ExpandoObject)d).ToDictionary(
                kvp => kvp.Key,
                kvp => kvp.Value ?? DBNull.Value
            )).ToList();
        }

        public async Task<int> BulkInsertAsync<T>(string tableName, List<T> data) where T : class, new()
        {
            return await _db.Insertable(data).AS(tableName).ExecuteCommandAsync();
        }
    }
}
