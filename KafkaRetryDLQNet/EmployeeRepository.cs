using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;

namespace KafkaRetryDLQNet;

public class EmployeeRepository
{
    private readonly string _connectionString;
    private readonly ILogger<EmployeeRepository> _logger;

    public EmployeeRepository(IConfiguration configuration, ILogger<EmployeeRepository> logger)
    {
        _connectionString = configuration.GetConnectionString("Northwind") 
            ?? throw new InvalidOperationException("Northwind connection string not found");
        _logger = logger;
    }

    public async Task<bool> UpdateEmployeeAsync(EmployeeMessage message)
    {
        const string sql = @"
            UPDATE Employees 
            SET FirstName = @FirstName, SyncTime = @SyncTime 
            WHERE EmployeeID = @EmployeeID AND SyncTime <= @SyncTime";

        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var command = new SqlCommand(sql, connection);
            command.Parameters.AddWithValue("@EmployeeID", message.EmployeeID);
            command.Parameters.AddWithValue("@FirstName", message.FirstName);
            command.Parameters.AddWithValue("@SyncTime", message.SyncTime);

            var rowsAffected = await command.ExecuteNonQueryAsync();
            
            if (rowsAffected == 0)
            {
                _logger.LogWarning("No rows updated for EmployeeID {EmployeeID} - either not found or SyncTime too old", 
                    message.EmployeeID);
                return false;
            }

            _logger.LogInformation("Updated Employee {EmployeeID} with FirstName '{FirstName}' at SyncTime {SyncTime}", 
                message.EmployeeID, message.FirstName, message.SyncTime);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating employee {EmployeeID}", message.EmployeeID);
            throw;
        }
    }
}
