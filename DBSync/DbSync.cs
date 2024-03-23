using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Extensions.Sql;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Npgsql;

namespace DBSync;

public class DbSync
{
    private readonly ILogger _logger;

    public DbSync(ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<DbSync>();
    }

    [Function("DbSync")]
    public async Task Run(
        [SqlTrigger("[dbo].[RegistroPontos]", "SqlServerConnectionString")] IReadOnlyList<SqlChange<RegistroPonto>> changes,
            FunctionContext context)
    {
        _logger.LogInformation("SQL Changes: " + JsonConvert.SerializeObject(changes));

        var postgresConnectionString = Environment.GetEnvironmentVariable("PostgresConnectionString");

        using (var conn = new NpgsqlConnection(postgresConnectionString))
        {
            conn.Open();

            foreach (var change in changes)
            {
                using (var cmd = new NpgsqlCommand())
                {
                    cmd.Connection = conn;
                    cmd.CommandText =
                        "INSERT INTO registropontos (Id, UserName, Registro) VALUES (@Id, @UserName, @Registro)";

                    var registro = change.Item;
                    cmd.Parameters.AddWithValue("Id", registro.Id);
                    cmd.Parameters.AddWithValue("UserName", registro.UserName);
                    cmd.Parameters.AddWithValue("Registro", registro.Registro);

                    await cmd.ExecuteNonQueryAsync();
                }
            }
        }

    }
}

public class RegistroPonto
{
    public int Id { get; set; }
    public string UserName { get; set; }
    public DateTime Registro { get; set; }
}
