namespace NLog.Extensions.AzureTableStorage
{
    public interface IPartitionKeyStrategy
    {
        string Get(LogEventInfo logEvent);
    }
}