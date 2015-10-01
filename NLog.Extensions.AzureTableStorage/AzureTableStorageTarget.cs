using System;
using System.ComponentModel.DataAnnotations;
using NLog.Targets;

namespace NLog.Extensions.AzureTableStorage
{
    using System.Collections.Concurrent;
    using System.Configuration;
    using System.Threading;
    using System.Threading.Tasks;
    using Internal;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;


    [Target("AzureTableStorage")]
    public class AzureTableStorageTarget : TargetWithLayout
    {
        private CloudTable _table;

        [Required]
        public string TableName { get; set; }

        [Required]
        public string ConnectionString { get; set; }

        private IPartitionKeyStrategy _partitionKeyStrategy;
        private IRowKeyStrategy _rowKeyStrategy;

        private ConcurrentQueue<DynamicTableEntity> _entities = new ConcurrentQueue<DynamicTableEntity>();
        private string _lastPartitionKey = null;
        private int _isProcessing;

        protected override void InitializeTarget()
        {
            base.InitializeTarget();
            ValidateParameters();

            var storageAccount = GetCloudStorageAccount();
            _partitionKeyStrategy = new ReverseTicksPartitionKeyStrategy();
            _rowKeyStrategy = new ReverseTicksWithGuidRowKeyStrategy();

            var tableClient = storageAccount.CreateCloudTableClient();
            _table = tableClient.GetTableReference(TableName);
            _table.CreateIfNotExists();
        }

        private void ValidateParameters()
        {
            IsNameValidForTableStorage(TableName);
        }

        private CloudStorageAccount GetCloudStorageAccount()
        {
            CloudStorageAccount account;
            if (CloudStorageAccount.TryParse(ConnectionString, out account))
            {
                return account;
            }

            var setting = System.Configuration.ConfigurationManager.AppSettings[ConnectionString];
            if (!string.IsNullOrEmpty(setting))
            {
                if (CloudStorageAccount.TryParse(setting, out account))
                {
                    return account;
                }
            }

            var connectionStringSetting = System.Configuration.ConfigurationManager.ConnectionStrings[ConnectionString];
            if (connectionStringSetting != null && !string.IsNullOrEmpty(connectionStringSetting.ConnectionString))
            {
                if (CloudStorageAccount.TryParse(connectionStringSetting.ConnectionString, out account))
                {
                    return account;
                }
            }

            throw new ConfigurationErrorsException("Could not locate storage account connection string.");
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
            string layoutMessage = Layout.Render(logEvent);
            DynamicTableEntity entity = logEvent.CreateTableEntity(layoutMessage, _partitionKeyStrategy, _rowKeyStrategy);
            _entities.Enqueue(entity);

            if (Interlocked.CompareExchange(ref _isProcessing, 1, 0) == 0)
            {
                ThreadPool.QueueUserWorkItem(ProcessQueue);
            }
        }

        private void ProcessQueue(object state)
        {

            do
            {
                DynamicTableEntity entity;
                string partitionKey = string.Empty;
                TableBatchOperation operation = new TableBatchOperation();

                while (_entities.TryDequeue(out entity))
                {
                    if (partitionKey != entity.PartitionKey)
                    {
                        if (operation.Count != 0)
                        {
                            _table.ExecuteBatch(operation);
                            operation = new TableBatchOperation();
                        }

                        partitionKey = entity.PartitionKey;
                    }

                    operation.Add(TableOperation.Insert(entity));
                    if (operation.Count == 100)
                    {
                        _table.ExecuteBatch(operation);
                        operation = new TableBatchOperation();
                    }
                }

                if (operation.Count != 0)
                {
                    _table.ExecuteBatch(operation);
                    operation = new TableBatchOperation();
                }

                Interlocked.Exchange(ref _isProcessing, 0);
            } while (_entities.Count > 0 && Interlocked.CompareExchange(ref _isProcessing, 1, 0) == 0);
        }
    }
}
