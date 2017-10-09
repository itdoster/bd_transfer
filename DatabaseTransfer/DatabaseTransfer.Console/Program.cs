namespace DatabaseTransfer.Console
{
    class Program
    {
        static void Main(string[] args)
        {
            var sqlConnection = @"URI=file: C:\Users\dimon\Source\Repos\bd_transfer\DatabaseTransfer\DatabaseTransfer.Console\test.bim";
            var postgreConnection = "Server=127.0.0.1;Port=5432;Database=dd;User Id=postgres;Password = 1111; ";
            var transfer = new DatabaseMigrator(sqlConnection, postgreConnection);
            transfer.MigrateSqlToPostgre();
            System.Console.ReadLine();
        }
    }
}
