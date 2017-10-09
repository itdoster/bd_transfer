using System.IO;
using System.Text;

namespace DatabaseTransfer
{
    public class SqlFormatter
    {
        /// <summary>
        /// Convert sequences.
        /// </summary>
        /// <param name="sqlName"></param>
        /// <returns></returns>
        public string GetSchemaSql(string sqlName)
        {
            var sql = File.ReadAllLines(sqlName);
            var schema = new StringBuilder();
            foreach (var line in sql)
            {
                if (!line.Contains("sqlite_sequence"))
                {
                    schema.AppendLine(line);
                }
            }
            var result = schema.ToString().ToLower().Replace('`', '"')
                .Replace("integer primary key autoincrement", "serial primary key");
            return result;
        }
    }
}
