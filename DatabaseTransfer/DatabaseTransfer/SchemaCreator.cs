using System;
using System.Diagnostics;

namespace DatabaseTransfer
{
    public class SchemaCreator
    {
        public bool CreateSqlDatabaseSchemaScript(string databasePath, string sqlName)
        {
            try
            {
                var process = new Process();
                var startInfo = new ProcessStartInfo
                {
                    WindowStyle = ProcessWindowStyle.Hidden,
                    FileName = "cmd",
                    Arguments = $"/C sqlite3 {databasePath} .schema > {sqlName}"
                };
                process.StartInfo = startInfo;
                process.Start();
                process.WaitForExit();
                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
        }
    }
}
