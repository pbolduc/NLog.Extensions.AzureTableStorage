namespace NLog.Extensions.AzureTableStorage
{
    using System;

    public class ReverseTicksWithGuidRowKeyStrategy : IRowKeyStrategy
    {
        public string Get(LogEventInfo logEvent)
        {
            return string.Format("{0}_{1:N}", (DateTime.MaxValue.Ticks - DateTime.UtcNow.Ticks).ToString("d19"),
                Guid.NewGuid());
        }
    }
}