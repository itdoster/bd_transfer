using System.Diagnostics;

namespace DatabaseTransfer.Console
{
    class Program
    {
        static void Main(string[] args)
        {
            var dbPath = @"test.bim";
            var sql = "sqlite_data.sql";
            string cs = @"URI=file: C:\Users\dimon\Source\Repos\bd_transfer\DatabaseTransfer\DatabaseTransfer.Console\test.bim";
            var connString = "Server=127.0.0.1;Port=5432;Database=dd;User Id=postgres;Password = 1111; ";
            var transfer = new DatabaseMigrator(cs, connString);
            transfer.CreateDatabaseSchemaScript(dbPath, sql);
            var res = transfer.SyncDatabases(sql);
        }
    }
}
