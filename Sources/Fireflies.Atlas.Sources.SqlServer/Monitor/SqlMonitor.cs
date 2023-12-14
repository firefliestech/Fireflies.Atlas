using System.Collections.Concurrent;
using System.Data;
using System.Reflection;
using System.Text.Json.Nodes;
using System.Xml.Linq;
using Fireflies.Logging.Abstractions;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
using JsonSerializer = System.Text.Json.JsonSerializer;

namespace Fireflies.Atlas.Sources.SqlServer.Monitor;

public class SqlMonitor : IDisposable {
    private readonly string _connectionString;
    private readonly Core.Atlas _atlas;
    private readonly ConcurrentDictionary<SqlDescriptor, MonitorEntry> _monitors = new();
    private static bool _initialized;
    private readonly Guid _uuid = Guid.NewGuid();
    private readonly Timer _timer;
    private SqlConnection? _dependencyConnection;
    private SqlDependency? _dependency;
    private readonly IFirefliesLogger _logger;
    private readonly SemaphoreSlim _semaphore = new(1);

    private int _lastReadUpdate = 0;

    public SqlMonitor(string connectionString, Core.Atlas atlas) {
        _logger = atlas.LoggerFactory.GetLogger<SqlMonitor>();
        _connectionString = connectionString;
        _atlas = atlas;
        _timer = new Timer(UpdateHeartbeat);
    }

    public void StartMonitor() {
        _dependencyConnection = new SqlConnection(_connectionString + ";Application Name=fireflies");
        InternalStartMonitor(true);
    }

    private async void InternalStartMonitor(bool readMaxValue) {
        try {
            if(_dependencyConnection!.State != ConnectionState.Open)
                _dependencyConnection.Open();

            await using var command = new SqlCommand("SELECT [UpdateId] FROM [Fireflies].[UpdateMax]", _dependencyConnection);
            _dependency = new SqlDependency(command);
            _dependency.OnChange += OnDependencyChange;

            var newMaxValue = (int)command.ExecuteScalar();
            if(readMaxValue) {
                _lastReadUpdate = newMaxValue;
            } else if(newMaxValue != _lastReadUpdate) {
                ReadUpdates();
            }
        } catch(Exception ex) {
            _logger.Error(ex, $"Exception while running {nameof(InternalStartMonitor)}");
            await Task.Delay(1000).ConfigureAwait(false);
#pragma warning disable CS4014
            Task.Run(() => InternalStartMonitor(readMaxValue));
#pragma warning restore CS4014
        }
    }

    private void OnDependencyChange(object sender, SqlNotificationEventArgs? e) {
        try {
            ReadUpdates();
        } catch(Exception ex) {
            _logger.Error(ex, $"Exception while running {nameof(OnDependencyChange)}");
        } finally {
            _dependency!.OnChange -= OnDependencyChange;
        }

        InternalStartMonitor(false);
    }

    private void ReadUpdates() {
        try {
            _semaphore.Wait();

            using var connection = new SqlConnection(_connectionString);
            connection.Open();
            using var command = new SqlCommand($"SELECT [UpdateId], [Schema], [Table], [Data] FROM [Fireflies].[Update] WITH (NOLOCK) WHERE [UpdateId] > {_lastReadUpdate}", connection);
            using var sqlDataReader = command.ExecuteReader();
            while(sqlDataReader.Read()) {
                var updateId = (int)sqlDataReader[0];
                if(_lastReadUpdate < updateId)
                    _lastReadUpdate = updateId;
                var tableDescriptor = new SqlNameDescriptor((string)sqlDataReader[1], (string)sqlDataReader[2]);

                if(_monitors.TryGetValue(tableDescriptor, out var monitor)) {
                    var value = XDocument.Parse((string)sqlDataReader[3]);
                    var json = JsonConvert.SerializeXNode(value, Formatting.None, true);
                    var jsonDocument = JsonSerializer.Deserialize<JsonObject>(json);
                    if(jsonDocument == null) continue;

                    foreach(var tableNotification in monitor.TableNotifications) {
                        tableNotification.Process(jsonDocument);
                    }
                }
            }
        } finally {
            _semaphore.Release();
        }
    }

    public TableNotification<TDocument> MonitorTable<TDocument>(SqlNameDescriptor sqlDescriptor) where TDocument : new() {
        if(!_initialized) {
            _logger.Debug("Initializing monitor infrastructure");
            _initialized = true;

            _logger.Trace($"Starting {nameof(SqlDependency)}");
            SqlDependency.Start(_connectionString);

            _logger.Trace($"Running install script");
            using var installScriptScript = Assembly.GetExecutingAssembly().GetManifestResourceStream($"{typeof(SqlMonitor).FullName}.sql")!;
            using var streamReader = new StreamReader(installScriptScript);
            var installScript = streamReader.ReadToEnd()!;

            using var connection = new SqlConnection(_connectionString);
            connection.Open();
            using var command = new SqlCommand(installScript, connection);
            command.ExecuteNonQuery();
            _logger.Trace("Install script was executed");

            StartMonitor();
            AddListener();

            _timer.Change(TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(20));
        }

        var monitor = _monitors.GetOrAdd(sqlDescriptor, _ => {
            _logger.Debug($"Adding monitor for {sqlDescriptor}");
            AddMonitor(sqlDescriptor);

            return new MonitorEntry(sqlDescriptor);
        });

        var tableNotification = monitor.AddTableNotification<TDocument>();
        _logger.Trace($"Created {nameof(TableNotification<TDocument>)} for {sqlDescriptor}");

        return tableNotification;
    }

    private void AddMonitor(SqlNameDescriptor sqlDescriptor) {
        try {
            _logger.Trace($"Running {nameof(AddMonitor)}");
            using var connection = new SqlConnection(_connectionString);
            connection.Open();
            using var insertMonitorCommand = new SqlCommand("INSERT INTO [Fireflies].[Monitor] VALUES (@uuid, @schema, @table)", connection);
            insertMonitorCommand.Parameters.Add("@uuid", SqlDbType.UniqueIdentifier).Value = _uuid;
            insertMonitorCommand.Parameters.Add("@schema", SqlDbType.NVarChar).Value = sqlDescriptor.Schema;
            insertMonitorCommand.Parameters.Add("@table", SqlDbType.NVarChar).Value = sqlDescriptor.Table;
            insertMonitorCommand.ExecuteNonQuery();
        } catch(Exception ex) {
            _logger.Error(ex, $"Exception while running {nameof(AddMonitor)}");
        }
    }

    private void AddListener() {
        try {
            _logger.Trace($"Running {nameof(AddListener)}");
            using var connection = new SqlConnection(_connectionString);
            connection.Open();
            using var insertListenerCommand = new SqlCommand("INSERT INTO [Fireflies].[Listener] VALUES (@uuid, GETUTCDATE())", connection);
            insertListenerCommand.Parameters.Add("@uuid", SqlDbType.UniqueIdentifier).Value = _uuid;
            insertListenerCommand.ExecuteNonQuery();
        } catch(Exception ex) {
            _logger.Error(ex, $"Exception while running {nameof(AddListener)}");
        }
    }

    private void RemoveListener() {
        try {
            _logger.Trace($"Running {nameof(RemoveListener)}");
            using var connection = new SqlConnection(_connectionString);
            connection.Open();
            using var insertListenerCommand = new SqlCommand("DELETE FROM [Fireflies].[Listener] WHERE [ListenerId]=@uuid", connection);
            insertListenerCommand.Parameters.Add("@uuid", SqlDbType.UniqueIdentifier).Value = _uuid;
            insertListenerCommand.ExecuteNonQuery();
        } catch(Exception ex) {
            _logger.Error(ex, $"Exception while running {nameof(RemoveListener)}");
        }
    }

    private void UpdateHeartbeat(object? state) {
        try {
            _logger.Trace($"Running {nameof(UpdateHeartbeat)}");
            using var connection = new SqlConnection(_connectionString);
            connection.Open();
            using var updateHeartbeatCommand = new SqlCommand("UPDATE [Fireflies].[Listener] SET LastHeartbeatAt=GETUTCDATE() WHERE [ListenerId]=@uuid", connection);
            updateHeartbeatCommand.Parameters.Add("@uuid", SqlDbType.UniqueIdentifier).Value = _uuid;
            updateHeartbeatCommand.ExecuteNonQuery();
        } catch(Exception ex) {
            _logger.Error(ex, $"Exception while running {nameof(UpdateHeartbeat)}");
        }
    }

    public void Dispose() {
        _timer.Dispose();
        _monitors.Clear();
        _dependencyConnection?.Dispose();

        RemoveListener();
    }

    private class MonitorEntry {
        private readonly ConcurrentDictionary<Type, TableNotification> _tableNotifications = new();

        public SqlDescriptor SqlDescriptor { get; }
        public IEnumerable<TableNotification> TableNotifications => _tableNotifications.Values;

        public MonitorEntry(SqlDescriptor sqlDescriptor) {
            SqlDescriptor = sqlDescriptor;
        }

        protected bool Equals(MonitorEntry other) {
            return SqlDescriptor.Equals(other.SqlDescriptor);
        }

        public override bool Equals(object? obj) {
            if(ReferenceEquals(null, obj)) return false;
            if(ReferenceEquals(this, obj)) return true;
            if(obj.GetType() != this.GetType()) return false;

            return Equals((MonitorEntry)obj);
        }

        public override int GetHashCode() {
            return SqlDescriptor.GetHashCode();
        }

        public TableNotification<TDocument> AddTableNotification<TDocument>() where TDocument : new() {
            return (TableNotification<TDocument>)_tableNotifications.GetOrAdd(typeof(TDocument), new TableNotification<TDocument>());
        }
    }
}