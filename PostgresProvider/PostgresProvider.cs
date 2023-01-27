using Migratable.Interfaces;
using Npgsql;

namespace Migratable.Providers
{
    /// <summary>Database provider for use with Migratable.</summary>
    public class PostgresProvider : IProvider
    {
        private readonly string connectionString;

        /// <summary>Create a database provider for use with Migratable.</summary>
        public PostgresProvider(string connectionString)
        {
            this.connectionString = connectionString;
        }

        /// <summary>
        /// User-friendly description of this connection.
        /// No connection string credentials are included.
        /// </summary>
        public string Describe()
        {
            using (var conn = new NpgsqlConnection(connectionString))
            {
                return $"Postgres against {conn.DataSource} {conn.Database}";
            }
        }

        /// <summary>
        /// Run a given SQL statement (in a transaction).
        /// If there is an error the transaction is rolled back.
        /// </summary>
        public void Execute(string instructions)
        {
            using (var conn = new NpgsqlConnection(connectionString))
            {
                conn.Open();
                var transaction = conn.BeginTransaction();
                var cmd = new NpgsqlCommand(instructions, conn);
                cmd.Transaction = transaction;
                try
                {
                    cmd.ExecuteNonQuery();
                    transaction.Commit();
                }
                catch
                {
                    transaction.Rollback();
                    throw;
                }
                conn.Close();
            }
        }

        /// <summary>Gets the migration version number.</summary>
        public int GetVersion()
        {
            var v = ExecuteScalar(
                "CREATE TABLE IF NOT EXISTS migratable_version" +
                "(" +
                "    id integer NOT NULL GENERATED ALWAYS AS IDENTITY(INCREMENT 1 START 1), " +
                "    version_number integer NOT NULL, " +
                "    actioned timestamp with time zone NOT NULL DEFAULT NOW(), " +
                "    PRIMARY KEY(id) " +
                ");"
                );
            v = ExecuteScalar("select version_number from migratable_version order by id desc limit 1");
            if (v == null)
            {
                SetVersion(0);
                return 0;
            }
            return (int)v;
        }

        /// <summary>Sets the migration version number.</summary>
        public void SetVersion(int versionNumber)
        {
            var sql = "insert into migratable_version (version_number) values ({0})";
            Execute(string.Format(sql, versionNumber));
        }

        private object ExecuteScalar(string instructions)
        {
            using (var conn = new NpgsqlConnection(connectionString))
            {
                conn.Open();
                var cmd = new NpgsqlCommand(instructions, conn);
                var result = cmd.ExecuteScalar();
                conn.Close();
                return result;
            }
        }
    }
}
