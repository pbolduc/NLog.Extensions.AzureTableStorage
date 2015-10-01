using System;
using System.ComponentModel.DataAnnotations;
using NLog.Targets;

namespace NLog.Extensions.AzureTableStorage
{
    using System.Collections.Generic;
    using System.Globalization;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;

    public interface IKeyStrategy
    {
        string Get(LogEventInfo logEvent);
    }

    public interface IPartitionKeyStrategy : IKeyStrategy
    {
    }

    public interface IRowKeyStrategy : IKeyStrategy
    {

    }

    public class ReverseTicksWithGuidRowKeyStrategy : IRowKeyStrategy
    {
        public string Get(LogEventInfo logEvent)
        {
            return string.Format("{0}__{1}", (DateTime.MaxValue.Ticks - DateTime.UtcNow.Ticks).ToString("d19"),
                Guid.NewGuid());
        }
    }

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


    [Target("AzureTableStorage")]
    public class AzureTableStorageTarget : TargetWithLayout
    {
        private CloudTable _table;
        
        [Required]
        public string TableName { get; set; }

        private IPartitionKeyStrategy _partitionKeyStrategy;
        private IRowKeyStrategy _rowKeyStrategy;

        protected override void InitializeTarget()
        {
            base.InitializeTarget();
            ValidateParameters();

            CloudStorageAccount storageAccount = CloudStorageAccount.DevelopmentStorageAccount;

            _partitionKeyStrategy = new LoggerNamePartitionKeyStrategy("");
            _rowKeyStrategy = new ReverseTicksWithGuidRowKeyStrategy();

            var tableClient = storageAccount.CreateCloudTableClient();
            _table = tableClient.GetTableReference(TableName);
            _table.CreateIfNotExists();
        }

        private void ValidateParameters()
        {
            IsNameValidForTableStorage(TableName);
        }

        private void IsNameValidForTableStorage(string tableName)
        {
            var validator = new AzureStorageTableNameValidator(tableName);
            if (!validator.IsValid())
            {
                throw new NotSupportedException(tableName + " is not a valid name for Azure storage table name.")
                {
                    HelpLink = "http://msdn.microsoft.com/en-us/library/windowsazure/dd179338.aspx"
                };
            }
        }

        protected override void Write(LogEventInfo logEvent)
        {
            //var layoutMessage = Layout.Render(logEvent);

            var entry = logEvent.CreateTableEntity(_partitionKeyStrategy, _rowKeyStrategy);

            var operation = new TableBatchOperation();
            operation.Add(TableOperation.Insert(entry));
            _table.ExecuteBatch(operation);
        }
    }


    internal static class LogEventInfoExtensions
    {
        private const int MaxStringLength = 30000;
        private const int MaxPayloadItems = 200;

        public static DynamicTableEntity CreateTableEntity(this LogEventInfo logEventInfo,
            IPartitionKeyStrategy partitionKeyStrategy, IRowKeyStrategy rowKeyStrategy)
        {
            var dictionary = new Dictionary<string, EntityProperty>();

            dictionary.Add("LoggerName", new EntityProperty(logEventInfo.LoggerName));
            dictionary.Add("LogTimeStamp", new EntityProperty(logEventInfo.TimeStamp));
            dictionary.Add("Level", new EntityProperty(logEventInfo.Level.ToString()));
            dictionary.Add("Message", new EntityProperty(logEventInfo.Message));
            dictionary.Add("MessageWithLayout", new EntityProperty(logEventInfo.FormattedMessage));
            dictionary.Add("SequenceID", new EntityProperty(logEventInfo.SequenceID));

            AddProperties(logEventInfo, dictionary);
            AddExceptionInfo(dictionary, "Exception_", logEventInfo.Exception, 0);

            if (logEventInfo.StackTrace != null)
            {
                dictionary.Add("StackTrace", new EntityProperty(logEventInfo.StackTrace.ToString()));
            }


            var partitionKey = partitionKeyStrategy.Get(logEventInfo);
            var rowKey = rowKeyStrategy.Get(logEventInfo);

            return new DynamicTableEntity(partitionKey, rowKey, null, dictionary);
        }

        private static void AddExceptionInfo(Dictionary<string, EntityProperty> dictionary, string prefix,
            Exception exception, int level)
        {
            if (exception == null)
            {
                return;
            }

            dictionary.Add(prefix + "_Type", new EntityProperty(exception.GetType().ToString()));
            dictionary.Add(prefix + "_Message", new EntityProperty(exception.Message));

            AddExceptionInfo(dictionary, prefix, exception.InnerException, level + 1);
        }

        private static void AddProperties(LogEventInfo logEventInfo, Dictionary<string, EntityProperty> dictionary)
        {
            foreach (var item in logEventInfo.Properties)
            {
                var value = item.Value;
                if (value != null)
                {
                    EntityProperty property = null;
                    var type = value.GetType();

                    if (type == typeof (string))
                    {
                        property = new EntityProperty((string) value);
                    }
                    else if (type == typeof (int))
                    {
                        property = new EntityProperty((int) value);
                    }
                    else if (type == typeof (long))
                    {
                        property = new EntityProperty((long) value);
                    }
                    else if (type == typeof (double))
                    {
                        property = new EntityProperty((double) value);
                    }
                    else if (type == typeof (Guid))
                    {
                        property = new EntityProperty((Guid) value);
                    }
                    else if (type == typeof (bool))
                    {
                        property = new EntityProperty((bool) value);
                    }
                    else if (type.IsEnum)
                    {
                        var typeCode = ((Enum) value).GetTypeCode();
                        if (typeCode <= TypeCode.Int32)
                        {
                            property = new EntityProperty(Convert.ToInt32(value, CultureInfo.InvariantCulture));
                        }
                        else
                        {
                            property = new EntityProperty(Convert.ToInt64(value, CultureInfo.InvariantCulture));
                        }
                    }
                    else if (type == typeof (byte[]))
                    {
                        property = new EntityProperty((byte[]) value);
                    }

                    //// TODO: add & review DateTimeOffset if it's supported

                    if (property != null)
                    {
                        dictionary.Add(string.Format(CultureInfo.InvariantCulture, "Property_{0}", item.Key), property);
                    }
                }
            }
        }

    }
}
