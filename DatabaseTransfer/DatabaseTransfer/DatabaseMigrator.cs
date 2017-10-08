using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace DatabaseTransfer
{
    public class DatabaseMigrator
    {
        private const string _selectAllTablesQuery = "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY 1";
        private string _sqlLiteConnectionString;
        private string _postgreConnectionString;

        public DatabaseMigrator(string sqliteConnection, string postgreSqlConnection)
        {
            this._sqlLiteConnectionString = sqliteConnection;
            this._postgreConnectionString = postgreSqlConnection;
        }

        /// <summary>
        /// create .sql file with database schema
        /// </summary>
        /// <param name="databasePath"></param>
        /// <param name="sqlName"></param>
        public void CreateDatabaseSchemaScript(string databasePath, string sqlName)
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
        }

        /// <summary>
        /// Convert sequences.
        /// </summary>
        /// <param name="sqlName"></param>
        /// <returns></returns>
        public string GetFormattedSqlScript(string sqlName)
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

        /// <summary>
        /// create tables drom scheme
        /// </summary>
        /// <param name="sqlName"></param>
        public void CreateDatabaseFromSchema(string sqlName, NpgsqlConnection postgreConnection)
        {
            try
            {
                var sqlSchema = this.GetFormattedSqlScript(sqlName);
                using (var cmd = new NpgsqlCommand())
                {
                    cmd.Connection = postgreConnection;
                    cmd.CommandText = sqlSchema;
                    cmd.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                //tables is already exist
            }

        }

        /// <summary>
        /// insert in table
        /// </summary>
        /// <param name="postgreConnection"></param>
        /// <param name="table"></param>
        public void InsertInPostgreSqlTable(NpgsqlConnection postgreConnection, DataTable table)
        {
            try
            {
                var tableQueries = new List<string>();

                //get column names
                var columnNames = table.Columns.Cast<DataColumn>()
                                 .ToArray();
                foreach (DataRow row in table.Rows)
                {
                    var insertValues = new List<string>();
                    foreach (var column in columnNames)
                    {
                        insertValues.Add($"'{row[column.ColumnName]}'");
                    }
                    tableQueries.Add($"INSERT INTO {table.TableName} VALUES ({String.Join(",", insertValues.ToArray())});");
                }
                var query = $"BEGIN; ALTER TABLE {table} DISABLE TRIGGER ALL; {String.Join("", tableQueries.ToArray())}; ALTER TABLE {table} ENABLE TRIGGER ALL; COMMIT; ";
                using (var cmd = new NpgsqlCommand())
                {
                    cmd.Connection = postgreConnection;
                    cmd.CommandText = query;
                    cmd.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                //tables is already exist
            }
        }

        /// <summary>
        /// database synchronization
        /// </summary>
        /// <param name="sqlName"></param>
        /// <returns></returns>
        public DataSet SyncDatabases(string sqlName)
        {
            using (var connection = new SQLiteConnection(this._sqlLiteConnectionString))
            {
                connection.Open();
                var tables = this.GetTables(connection);
                var dataset = new DataSet();
                foreach (var tableName in tables)
                {
                    dataset.Tables.Add(this.GetDataTable("select * from " + tableName, connection));
                }

                using (var conn = new NpgsqlConnection(this._postgreConnectionString))
                {
                    conn.Open();
                    foreach (DataTable table in dataset.Tables)
                    {
                        this.InsertInPostgreSqlTable(conn, table);
                    }
                }
                //Console.WriteLine(dataset.GetXmlSchema());
                Console.ReadLine();
                return dataset;
            }
        }

        /// <summary>
        /// get list of tables
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        public IEnumerable<string> GetTables(SQLiteConnection connection)
        {
            var tables = new List<string>();
            try
            {
                var table = this.GetDataTable(_selectAllTablesQuery, connection);
                foreach (DataRow row in table.Rows)
                {
                    tables.Add(row.ItemArray[0].ToString());
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            return tables;
        }

        /// <summary>
        /// load table
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="connection"></param>
        /// <returns></returns>
        public DataTable GetDataTable(string sql, SQLiteConnection connection)
        {
            try
            {
                var dt = new DataTable();
                using (var cmd = new SQLiteCommand(sql, connection))
                {
                    using (var rdr = cmd.ExecuteReader())
                    {
                        dt.Load(rdr);
                        return dt;
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return null;
            }
        }
    }
}
