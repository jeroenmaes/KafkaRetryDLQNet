namespace KafkaRetryDLQNet;

public class EmployeeMessage
{
    public int EmployeeID { get; set; }
    public string FirstName { get; set; } = string.Empty;
    public long SyncTime { get; set; }
}
