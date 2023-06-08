using System.Data;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Xml.Linq;
using FastExpressionCompiler;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
using JsonSerializer = System.Text.Json.JsonSerializer;

namespace Fireflies.Atlas.Sources.SqlServer;

public class SqlMonitor : IDisposable {
    private readonly string _connectionString;
    private readonly Core.Atlas _atlas;
    private int _maxValue;
    private readonly Dictionary<TableDescriptor, Action<JsonObject>> _monitors = new();
    private static bool _initialized;
    private readonly Guid _uuid = Guid.NewGuid();
    private readonly Timer _timer;
    private SqlConnection? _dependencyConnection;
    private SqlDependency? _dependency;
    private readonly JsonSerializerOptions _serializerOptions;

    public SqlMonitor(string connectionString, Core.Atlas atlas) {
        _connectionString = connectionString;
        _atlas = atlas;
        _timer = new Timer(UpdateHeartbeat);
        _serializerOptions = new JsonSerializerOptions { PropertyNameCaseInsensitive = true, Converters = { new XmlStringToEnumConverter(), new AutoStringToNumberConverter(), new XmlStringToBooleanConverter() } };
    }

    public void StartMonitor() {
        _dependencyConnection = new SqlConnection(_connectionString);
        _dependencyConnection.Open();

        InternalStartMonitor(true);
    }

    private void InternalStartMonitor(bool readMaxValue) {
        using var command = new SqlCommand("SELECT [UpdateId] FROM [Fireflies].[UpdateMax]", _dependencyConnection);
        _dependency = new SqlDependency(command);
        _dependency.OnChange += OnDependencyChange;

        var newMaxValue = (int)command.ExecuteScalar();
        if(readMaxValue) {
            _maxValue = newMaxValue;
        } else if(_maxValue != newMaxValue) {
            OnDependencyChange(_dependency, null);
        }
    }

    private void OnDependencyChange(object sender, SqlNotificationEventArgs? e) {
        using var connection = new SqlConnection(_connectionString);
        connection.Open();
        using var command = new SqlCommand($"SELECT [UpdateId], [Schema], [Table], [Data] FROM [Fireflies].[Update] WHERE [UpdateId] > {_maxValue}", connection);
        using var sqlDataReader = command.ExecuteReader();
        while(sqlDataReader.Read()) {
            var updateId = (int)sqlDataReader[0];
            if(_maxValue < updateId)
                _maxValue = updateId;

            var tableDescriptor = new TableDescriptor((string)sqlDataReader[1], (string)sqlDataReader[2]);

            if(_monitors.TryGetValue(tableDescriptor, out var callback)) {
                var value = XDocument.Parse((string)sqlDataReader[3]);
                var json = JsonConvert.SerializeXNode(value, Formatting.None, true);
                var jsonDocument = JsonSerializer.Deserialize<JsonObject>(json);
                if(jsonDocument != null)
                    callback(jsonDocument);
            }
        }

        _dependency!.OnChange -= OnDependencyChange;
        InternalStartMonitor(false);
    }

    public void MonitorTable<TDocument>(TableDescriptor tableDescriptor, Expression<Func<TDocument, bool>>? filter) where TDocument : new() {
        if(!_initialized) {
            _initialized = true;

            using var installScriptScript = Assembly.GetExecutingAssembly().GetManifestResourceStream($"{typeof(SqlMonitor).FullName}.sql")!;
            using var streamReader = new StreamReader(installScriptScript);
            var installScript = streamReader.ReadToEnd()!;

            SqlDependency.Start(_connectionString);

            using var connection = new SqlConnection(_connectionString);
            connection.Open();
            using var command = new SqlCommand(installScript, connection);
            command.ExecuteNonQuery();

            StartMonitor();
            AddListener();

            _timer.Change(TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(20));
        }

        AddMonitor(tableDescriptor);

        _monitors.TryAdd(tableDescriptor, jsonDocument => {
            var compiledFilter = filter?.CompileFast();

            var insertedRow = jsonDocument["inserted"]?["row"];
            if(insertedRow != null) {
                var document = insertedRow.Deserialize<TDocument>(_serializerOptions)!;
                if(compiledFilter != null && !compiledFilter(document)) {
                    _atlas.DeleteDocument(document);
                } else {
                    _atlas.UpdateDocument(document);
                }
            } else {
                var deletedRow = jsonDocument["deleted"]!["row"];
                if(deletedRow != null) {
                    var document = deletedRow.Deserialize<TDocument>(_serializerOptions);
                    _atlas.DeleteDocument(document);
                }
            }
        });
    }

    private void AddMonitor(TableDescriptor tableDescriptor) {
        using var connection = new SqlConnection(_connectionString);
        connection.Open();
        using var insertMonitorCommand = new SqlCommand("INSERT INTO [Fireflies].[Monitor] VALUES (@uuid, @schema, @table)", connection);
        insertMonitorCommand.Parameters.Add("@uuid", SqlDbType.UniqueIdentifier).Value = _uuid;
        insertMonitorCommand.Parameters.Add("@schema", SqlDbType.NVarChar).Value = tableDescriptor.Schema;
        insertMonitorCommand.Parameters.Add("@table", SqlDbType.NVarChar).Value = tableDescriptor.Table;
        insertMonitorCommand.ExecuteNonQuery();
    }

    private void AddListener() {
        using var connection = new SqlConnection(_connectionString);
        connection.Open();
        using var insertListenerCommand = new SqlCommand("INSERT INTO [Fireflies].[Listener] VALUES (@uuid, GETUTCDATE())", connection);
        insertListenerCommand.Parameters.Add("@uuid", SqlDbType.UniqueIdentifier).Value = _uuid;
        insertListenerCommand.ExecuteNonQuery();
    }

    private void RemoveListener() {
        using var connection = new SqlConnection(_connectionString);
        connection.Open();
        using var insertListenerCommand = new SqlCommand("DELETE FROM [Fireflies].[Listener] WHERE [ListenerId]=@uuid", connection);
        insertListenerCommand.Parameters.Add("@uuid", SqlDbType.UniqueIdentifier).Value = _uuid;
        insertListenerCommand.ExecuteNonQuery();
    }

    private void UpdateHeartbeat(object? state) {
        using var connection = new SqlConnection(_connectionString);
        connection.Open();
        using var updateHeatbeatCommand = new SqlCommand("UPDATE [Fireflies].[Listener] SET LastHeartbeatAt=GETUTCDATE() WHERE [ListenerId]=@uuid", connection);
        updateHeatbeatCommand.Parameters.Add("@uuid", SqlDbType.UniqueIdentifier).Value = _uuid;
        updateHeatbeatCommand.ExecuteNonQuery();
    }

    public void Dispose() {
        _timer.Dispose();
        _monitors.Clear();
        _dependencyConnection?.Dispose();

        RemoveListener();
    }
}