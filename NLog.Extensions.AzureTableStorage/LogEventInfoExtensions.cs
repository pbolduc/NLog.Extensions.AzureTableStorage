namespace NLog.Extensions.AzureTableStorage
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Reflection;
    using System.Security.AccessControl;
    using System.Text;
    using Microsoft.WindowsAzure.Storage.Table;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    internal static class LogEventInfoExtensions
    {
        private const int MaxStringLength = 30000;
        private const int MaxPayloadItems = 200;

        public static DynamicTableEntity CreateTableEntity(
            this LogEventInfo logEventInfo,
            string layoutMessage,
            IPartitionKeyStrategy partitionKeyStrategy, 
            IRowKeyStrategy rowKeyStrategy)
        {
            var dictionary = new Dictionary<string, EntityProperty>();

            dictionary.Add("LoggerName", new EntityProperty(logEventInfo.LoggerName));
            dictionary.Add("LogTimeStamp", new EntityProperty(logEventInfo.TimeStamp));
            dictionary.Add("Level", new EntityProperty(logEventInfo.Level.ToString()));
            dictionary.Add("Message", new EntityProperty(logEventInfo.FormattedMessage));
            dictionary.Add("MessageWithLayout", new EntityProperty(layoutMessage));
            dictionary.Add("SequenceID", new EntityProperty(logEventInfo.SequenceID));
            dictionary.Add("MachineName", new EntityProperty(Environment.MachineName));

            if (logEventInfo.Exception != null)
            {
                dictionary.Add("Exception", new EntityProperty(ToJson(logEventInfo.Exception)));
            }

            if (logEventInfo.Properties.Count != 0)
            {
                dictionary.Add("Properties", new EntityProperty(ToJson(logEventInfo.Properties)));
            }

            if (logEventInfo.StackTrace != null)
            {
                dictionary.Add("StackTrace", new EntityProperty(ToJson(logEventInfo.StackTrace)));
            }

            var partitionKey = partitionKeyStrategy.Get(logEventInfo);
            var rowKey = rowKeyStrategy.Get(logEventInfo);

            return new DynamicTableEntity(partitionKey, rowKey, null, dictionary);
        }

        private static string ToJson(object o)
        {
            if (o != null)
            {
                return JObject.FromObject(o).ToString(Formatting.None);
            }
            return string.Empty;
        }

        private static void AddExceptionInfo(Dictionary<string, EntityProperty> dictionary, string prefix, Exception exception, bool innerException)
        {
            if (exception == null)
            {
                return;
            }

            //var exceptionType = exception.GetType();
            //dictionary.Add(prefix + "Type", new EntityProperty(exceptionType.FullName));
            AddProperties(dictionary, exception, prefix);            
            dictionary.Add(prefix + "Data", new EntityProperty(ToJson(exception.Data)));

            if (innerException)
            {
                AddExceptionInfo(dictionary, prefix + "InnerException_", exception.InnerException, false);
            }
        }

        private static void AddProperties(Dictionary<string, EntityProperty> dictionary, object obj, string prefix)
        {
            var objectType = obj.GetType();
            var properties = objectType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            foreach (var propertyInfo in properties)
            {
                object value = propertyInfo.GetValue(obj);
                if (value == null)
                {
                    continue;
                }

                EntityProperty property = GetEntityProperty(value);
                if (property != null)
                {
                    dictionary.Add(prefix + propertyInfo.Name, property);
                }
                else
                {
                    // nested properties ??
                }
            }
        }

        private static void AddProperties(LogEventInfo logEventInfo, Dictionary<string, EntityProperty> dictionary)
        {
            foreach (var item in logEventInfo.Properties)
            {
                var value = item.Value;
                if (value != null)
                {
                    EntityProperty property = GetEntityProperty(value);

                    if (property != null)
                    {
                        dictionary.Add(string.Format(CultureInfo.InvariantCulture, "Property_{0}", item.Key), property);
                    }
                }
            }
        }

        private static EntityProperty GetEntityProperty(object value)
        {
            EntityProperty property = null;
            var type = value.GetType();

            if (type == typeof(string))
            {
                property = new EntityProperty((string)value);
            }
            else if (type == typeof(int))
            {
                property = new EntityProperty((int)value);
            }
            else if (type == typeof(long))
            {
                property = new EntityProperty((long)value);
            }
            else if (type == typeof(double))
            {
                property = new EntityProperty((double)value);
            }
            else if (type == typeof(Guid))
            {
                property = new EntityProperty((Guid)value);
            }
            else if (type == typeof(bool))
            {
                property = new EntityProperty((bool)value);
            }
            else if (type.IsEnum)
            {
                var typeCode = ((Enum)value).GetTypeCode();
                if (typeCode <= TypeCode.Int32)
                {
                    property = new EntityProperty(Convert.ToInt32(value, CultureInfo.InvariantCulture));
                }
                else
                {
                    property = new EntityProperty(Convert.ToInt64(value, CultureInfo.InvariantCulture));
                }
            }
            else if (type == typeof(byte[]))
            {
                property = new EntityProperty((byte[])value);
            }

            //// TODO: add & review DateTimeOffset if it's supported

            return property;
        }
    }
}