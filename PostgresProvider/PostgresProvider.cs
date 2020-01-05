using Migratable.Interfaces;
using Npgsql;

namespace Migratable.Providers
{
    public class PostgresProvider : IProvider
    {
        private readonly string connectionString;

        public PostgresProvider(string connectionString)
        {
            this.connectionString = connectionString;
        }

        public string Describe()
        {
            using (var conn = new NpgsqlConnection(connectionString))
            {
                return $"Postgres against {conn.DataSource} {conn.Database}";
            }
        }

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
