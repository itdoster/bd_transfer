namespace DatabaseTransfer
{
    using Npgsql;
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SQLite;
    using System.Linq;


    public class DatabaseMigrator
    {
        private const string SelectAllTablesQuery = "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY 1";
        private const string SqlSchemaFile = "schema.sql";
        private readonly string _sqlLiteConnectionString;
        private readonly string _postgreConnectionString;

        public DatabaseMigrator(string sqliteConnection, string postgreSqlConnection)
        {
            this._sqlLiteConnectionString = sqliteConnection;
            this._postgreConnectionString = postgreSqlConnection;
        }

        /// <summary>
        /// database synchronization
        /// </summary>
        /// <returns></returns>
        public void MigrateSqlToPostgre()
        {
            var schemaCreator = new SchemaCreator();
            var schemaExportResult = schemaCreator.CreateSqlDatabaseSchemaScript(this._sqlLiteConnectionString, SqlSchemaFile);
            if (schemaExportResult)
            {
                using (var conn = new NpgsqlConnection(this._postgreConnectionString))
                {
                    conn.Open();
                    this.CreateDatabaseFromSchema(conn);
                    this.ImportDataFromSqlToPostgre(conn);
                }

            }
        }

        #region Import and export operations

        /// <summary>
        /// import all data from sql db to postgre
        /// </summary>
        private void ImportDataFromSqlToPostgre(NpgsqlConnection npgsqlConnection)
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
                foreach (DataTable table in dataset.Tables)
                {
                    this.InsertInPostgreSqlTable(npgsqlConnection, table);
                }
            }
        }

        /// <summary>
        /// insert in table
        /// </summary>
        /// <param name="postgreConnection"></param>
        /// <param name="table"></param>
        private void InsertInPostgreSqlTable(NpgsqlConnection postgreConnection, DataTable table)
        {
            var transaction = postgreConnection.BeginTransaction();
            try
            {
                var insertQueries = new List<string>();
                var columnNames = table.Columns.Cast<DataColumn>().ToArray();
                foreach (DataRow row in table.Rows)
                {
                    var rowInsertQuery = columnNames.Select(column => $"'{row[column.ColumnName]}'").ToArray();
                    insertQueries.Add($"INSERT INTO {table.TableName} VALUES ({string.Join(",", rowInsertQuery)});");
                }
                var query = $"ALTER TABLE {table} DISABLE TRIGGER ALL; {string.Join("", insertQueries.ToArray())}; ALTER TABLE {table} ENABLE TRIGGER ALL;";

                using (var cmd = new NpgsqlCommand())
                {
                    cmd.Connection = postgreConnection;
                    cmd.CommandText = query;
                    cmd.ExecuteNonQuery();
                }
                transaction.Commit();
            }
            catch (Exception ex)
            {
                transaction.Rollback();
            }
        }

        #endregion Import and export operations

        #region Sql schema script operations

        /// <summary>
        /// create tables scheme
        /// </summary>
        /// <param name="postgreConnection"></param>
        public bool CreateDatabaseFromSchema(NpgsqlConnection postgreConnection)
        {
            var transaction = postgreConnection.BeginTransaction();
            try
            {
                var sqlFormatter = new SqlFormatter();
                var sqlSchema = sqlFormatter.GetSchemaSql(SqlSchemaFile);
                using (var cmd = new NpgsqlCommand())
                {
                    cmd.Connection = postgreConnection;
                    cmd.CommandText = sqlSchema;
                    cmd.ExecuteNonQuery();
                }
                transaction.Commit();
                return true;
            }
            catch (Exception ex)
            {
                transaction.Rollback();
                return false;
            }
        }

        #endregion Sql schema script operations

        #region Table operations

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
                var table = this.GetDataTable(SelectAllTablesQuery, connection);
                foreach (DataRow row in table.Rows)
                {
                    tables.Add(row.ItemArray[0].ToString());
                }
            }
            catch (Exception ex)
            {
                return null;
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
                return null;
            }
        }

        #endregion Table operations
    }
}
