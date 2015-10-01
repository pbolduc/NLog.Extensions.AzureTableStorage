namespace NLog.Extensions.AzureTableStorage
{
    using System;

    public class LoggerNamePartitionKeyStrategy : IPartitionKeyStrategy
    {
        private readonly Func<LogEventInfo, string> formatFunc;

        public LoggerNamePartitionKeyStrategy(string partitionKeyPrefix)
        {
            if (string.IsNullOrWhiteSpace(partitionKeyPrefix))
            {
                formatFunc = (e) => e.LoggerName;
            }
            else
            {
                formatFunc = (e) => partitionKeyPrefix + "." + e.LoggerName;
            }
        }

        public string Get(LogEventInfo logEvent)
        {
            return formatFunc(logEvent);
        }
    }
}