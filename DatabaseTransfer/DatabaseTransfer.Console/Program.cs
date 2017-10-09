namespace DatabaseTransfer.Console
{
    class Program
    {
        static void Main(string[] args)
        {
            //add to app.config
            var sqlConnection = @"URI=file: C:\Users\dimon\Source\Repos\bd_transfer\DatabaseTransfer\DatabaseTransfer.Console\test.bim";
            var postgreConnection = "Server=127.0.0.1;Port=5432;Database=test;User Id=postgres;Password = 1111;";
            var sqlFilePath = "script.sql";
            var transfer = new DatabaseMigrator(sqlConnection, postgreConnection, sqlFilePath);
            transfer.MigrateSqlToPostgre(true);
            System.Console.WriteLine("Done");
            System.Console.ReadLine();
        }
    }
}
