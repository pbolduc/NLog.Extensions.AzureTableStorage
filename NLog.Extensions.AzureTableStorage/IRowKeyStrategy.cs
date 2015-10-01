namespace NLog.Extensions.AzureTableStorage
{
    public interface IRowKeyStrategy
    {
        string Get(LogEventInfo logEvent);
    }
}