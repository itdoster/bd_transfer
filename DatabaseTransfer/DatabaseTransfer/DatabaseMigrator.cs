namespace DatabaseTransfer
{
    using System.IO;
    using Npgsql;
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SQLite;
    using System.Linq;

    public class DatabaseMigrator
    {
        private const int QuerycountPerTransaction = 3000;
        private readonly string _scriptPath;
        private readonly string _sqlLiteConnectionString;
        private readonly string _postgreConnectionString;
       
        public DatabaseMigrator(string sqliteConnection, string postgreSqlConnection, string scriptPath)
        {
            this._sqlLiteConnectionString = sqliteConnection;
            this._postgreConnectionString = postgreSqlConnection;
            this._scriptPath = scriptPath;
        }

        /// <summary>
        /// database synchronization
        /// </summary>
        /// <returns></returns>
        public void MigrateSqlToPostgre(bool needToApplySchema)
        {
            using (var conn = new NpgsqlConnection(this._postgreConnectionString))
            {
                conn.Open();
                if (needToApplySchema)
                {
                    this.CreateDatabaseFromSchema(conn);
                }
                this.ImportDataFromSqlToPostgre(conn);
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
            var insertQueries = new List<string>();
            var columnNames = table.Columns.Cast<DataColumn>().ToArray();
            foreach (DataRow row in table.Rows)
            {
                var rowInsertQuery = columnNames.Select(column => this.GetInsertValueByType(column.DataType, row[column.ColumnName]));
                insertQueries.Add($"insert into {table.TableName} values ({string.Join(",", rowInsertQuery)});");
            }
            //insert in database
            for (var i = 0; i < insertQueries.Count; i += QuerycountPerTransaction)
            {
                var queries = insertQueries.Skip(i).Take(QuerycountPerTransaction);
                var transaction = postgreConnection.BeginTransaction();
                try
                {
                    var query = $"alter table {table.TableName} disable trigger all; {string.Join("", queries.ToArray())}; alter table {table.TableName} enable trigger all;";
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
        }

        /// <summary>
        /// get value to insert
        /// </summary>
        /// <param name="type"></param>
        /// <param name="item"></param>
        /// <returns></returns>
        private string GetInsertValueByType(Type type, object item)
        {
            if (item is DBNull)
            {
                return "null";
            }
            switch (Type.GetTypeCode(type))
            {
                case TypeCode.String:
                    var str = Convert.ToString(item).Replace("'", "''");
                    return $@"'{str}'";
                default:
                    return $"{Convert.ChangeType(item, type)}";
            }
        }

        #endregion Import and export operations

        #region Sql schema script operations

        /// <summary>
        /// create tables scheme
        /// </summary>
        /// <param name="postgreConnection"></param>
        private bool CreateDatabaseFromSchema(NpgsqlConnection postgreConnection)
        {
            var transaction = postgreConnection.BeginTransaction();
            try
            {
                var sqlSchema = File.ReadAllText(this._scriptPath);
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
        private IEnumerable<string> GetTables(SQLiteConnection connection)
        {
            var tables = new List<string>();
            try
            {
                var table = this.GetDataTable(Queries.SelectAllTablesQuery, connection);
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
        private DataTable GetDataTable(string sql, SQLiteConnection connection)
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
