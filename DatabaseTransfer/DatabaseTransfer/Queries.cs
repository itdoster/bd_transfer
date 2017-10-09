namespace DatabaseTransfer
{
    public static class Queries
    {
        public const string SelectAllTablesQuery = "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY 1";
    }
}
