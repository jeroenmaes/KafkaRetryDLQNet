using Confluent.Kafka;
using System.Text;

namespace KafkaRetryDLQNet;

public static class HeaderHelper
{
    public const string RetryStage = "x-retry-stage";
    public const string NotBeforeEpochMs = "x-not-before-epoch-ms";
    public const string OriginTopic = "x-origin-topic";
    public const string LastError = "x-last-error";

    public static void SetHeader(this Headers headers, string key, int value)
    {
        headers.Remove(key);
        headers.Add(key, BitConverter.GetBytes(value));
    }

    public static void SetHeader(this Headers headers, string key, long value)
    {
        headers.Remove(key);
        headers.Add(key, BitConverter.GetBytes(value));
    }

    public static void SetHeader(this Headers headers, string key, string value)
    {
        headers.Remove(key);
        headers.Add(key, Encoding.UTF8.GetBytes(value));
    }

    public static int? GetIntHeader(this Headers headers, string key)
    {
        var header = headers.FirstOrDefault(h => h.Key == key);
        if (header.Value == null || header.Value.Length != 4)
            return null;
        return BitConverter.ToInt32(header.Value);
    }

    public static long? GetLongHeader(this Headers headers, string key)
    {
        var header = headers.FirstOrDefault(h => h.Key == key);
        if (header.Value == null || header.Value.Length != 8)
            return null;
        return BitConverter.ToInt64(header.Value);
    }

    public static string? GetStringHeader(this Headers headers, string key)
    {
        var header = headers.FirstOrDefault(h => h.Key == key);
        if (header.Value == null)
            return null;
        return Encoding.UTF8.GetString(header.Value);
    }
}
