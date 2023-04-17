using System.Data;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Xml.Linq;
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

    public void MonitorTable<TDocument>(TableDescriptor tableDescriptor) where TDocument : new() {
        if(!_initialized) {
            _initialized = true;

            SqlDependency.Start(_connectionString);

            using var connection = new SqlConnection(_connectionString);
            connection.Open();
            using var command = new SqlCommand(MonitorInstallScript, connection);
            command.ExecuteNonQuery();

            StartMonitor();
            AddListener();

            _timer.Change(TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(20));
        }

        AddMonitor(tableDescriptor);

        _monitors.TryAdd(tableDescriptor, jsonDocument => {
            var insertedRow = jsonDocument["inserted"]!["row"];
            if(insertedRow != null) {
                var document = insertedRow.Deserialize<TDocument>(_serializerOptions);
                _atlas.UpdateDocument(document);
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

    private const string MonitorInstallScript = @"
DECLARE @Sql nvarchar(MAX)

/* Schema */

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'Fireflies' ) BEGIN
	EXEC('CREATE SCHEMA [Fireflies]')
END

/* Monitor */

IF NOT EXISTS (SELECT * FROM sysobjects WHERE id=OBJECT_ID(N'[Fireflies].[Monitor]')) BEGIN
	CREATE TABLE [Fireflies].[Monitor] ([ListenerId] [uniqueidentifier] NOT NULL, [Schema] [nvarchar](50) NOT NULL, [Table] [nvarchar](50) NOT NULL, CONSTRAINT [PK_Fireflies_Monitor] PRIMARY KEY CLUSTERED ([ListenerId] ASC, [Schema] ASC, [Table] ASC) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]) ON [PRIMARY]
END

IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[Fireflies].[Fireflies_Monitor_Trigger]')) BEGIN
	SET @Sql='
CREATE TRIGGER [Fireflies].[Fireflies_Monitor_Trigger] ON [Fireflies].[Monitor] AFTER INSERT, DELETE AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @Schema nvarchar(50)
	DECLARE @Table nvarchar(50)
	DECLARE @TriggerName nvarchar(50)
	DECLARE @Sql nvarchar(MAX)

	DECLARE Inserted_Cursor CURSOR FOR
		SELECT [Schema], [Table] FROM INSERTED; 
	OPEN Inserted_Cursor

	FETCH NEXT FROM Inserted_Cursor INTO @Schema, @Table
	WHILE @@FETCH_STATUS = 0 
	BEGIN
		SET @Schema = replace(replace(@Schema, ''['', ''''), '']'', '''')
		SET @Table = replace(replace(@Table, ''['', ''''), '']'', '''')

		SET @TriggerName = ''[Fireflies_'' + @Table + ''_Monitor]''
		IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(''['' + @Schema + ''].'' + @TriggerName)) BEGIN
			SET @Sql = ''CREATE TRIGGER ['' + @Schema + ''].'' + @TriggerName + ''
				ON ['' + @Schema + ''].['' + @Table + '']
				AFTER INSERT, DELETE, UPDATE
			AS 
			BEGIN
				SET NOCOUNT ON;
	
				DECLARE @retvalOUT NVARCHAR(MAX)

				DECLARE @message NVARCHAR(MAX)
				SET @message = N''''<root/>''''

				SET @retvalOUT = (SELECT * FROM INSERTED FOR XML PATH(''''row''''), ROOT (''''inserted''''))
				IF (@retvalOUT IS NOT NULL) BEGIN
					SET @message = N''''<root>'''' + @retvalOUT
				END

				SET @retvalOUT = (SELECT * FROM DELETED FOR XML PATH(''''row''''), ROOT (''''deleted''''))
				IF (@retvalOUT IS NOT NULL) BEGIN
					IF (@message = N''''<root/>'''')
						BEGIN SET @message = N''''<root>'''' + @retvalOUT
					END ELSE BEGIN
						SET @message = @message + @retvalOUT
					END
				END 

				IF (@message != N''''<root/>'''') BEGIN
					SET @message = @message + N''''</root>''''
				END

				INSERT INTO [Fireflies].[Update] ([Schema], [Table], [Data]) VALUES (''''['' + @Schema + '']'''', ''''['' + @Table + '']'''', @message)
			END
			
			ALTER TABLE ['' + @Schema + ''].['' + @Table + ''] ENABLE TRIGGER '' + @TriggerName
			
			EXEC sp_executesql @Sql
		END
		FETCH NEXT FROM Inserted_Cursor INTO @Schema, @Table
	END

	CLOSE Inserted_Cursor;
	DEALLOCATE Inserted_Cursor;

	DECLARE Deleted_Cursor CURSOR FOR
		SELECT [Schema], [Table] FROM DELETED 
	OPEN Deleted_Cursor

	FETCH NEXT FROM Deleted_Cursor INTO @Schema, @Table
	WHILE @@FETCH_STATUS = 0 
	BEGIN
		SET @Schema = replace(replace(@Schema, ''['', ''''), '']'', '''')
		SET @Table = replace(replace(@Table, ''['', ''''), '']'', '''')
		SET @TriggerName = ''[Fireflies_'' + @Table + ''_Monitor]''

		IF NOT EXISTS (SELECT * FROM [Fireflies].[Monitor] WHERE replace(replace([Schema], ''['', ''''), '']'', '''') = @Schema AND replace(replace([Table], ''['', ''''), '']'', '''') = @Table) BEGIN
			SET @Sql = ''DROP TRIGGER ['' + @Schema + ''].'' + @TriggerName
			EXEC sp_executesql @Sql
		END

		FETCH NEXT FROM Deleted_Cursor INTO @Schema, @Table
	END

	CLOSE Deleted_Cursor;
	DEALLOCATE Deleted_Cursor;
END'
	EXEC sp_executesql @Sql
END

/* Listener */

IF NOT EXISTS (SELECT * FROM sysobjects WHERE id=OBJECT_ID(N'[Fireflies].[Listener]')) BEGIN
	CREATE TABLE [Fireflies].[Listener] ([ListenerId] [uniqueidentifier] NOT NULL,  [LastHeartbeatAt] [datetime] NOT NULL, CONSTRAINT [PK_Fireflies_Listener] PRIMARY KEY CLUSTERED ([ListenerId] ASC) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]) ON [PRIMARY]
END

IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[Fireflies].[Fireflies_Listener_Trigger]')) BEGIN
	SET @Sql='
CREATE TRIGGER [Fireflies].[Fireflies_Listener_Trigger] ON [Fireflies].[Listener] AFTER INSERT, DELETE, UPDATE AS 
BEGIN
	SET NOCOUNT ON;

	DELETE FROM [Fireflies].[Monitor] WHERE [ListenerId] IN (
		SELECT [ListenerId] FROM DELETED WHERE [ListenerId] NOT IN (SELECT [ListenerId] FROM INSERTED)
	)

	-- Also clean up lingering listeners
    DELETE FROM [Fireflies].[Listener] WHERE [ListenerId] IN (SELECT [ListenerId] FROM [Fireflies].[Listener] WHERE DATEDIFF(MINUTE, LastHeartbeatAt, GETUTCDATE()) > 2)

    -- Clean up lingering monitors
    DELETE FROM [Fireflies].[Monitor] WHERE [ListenerId] NOT IN (SELECT [ListenerId] FROM [Fireflies].[Listener])
END'
	EXEC sp_executesql @Sql
	ALTER TABLE [Fireflies].[Listener] ENABLE TRIGGER [Fireflies_Listener_Trigger]
END

/* UpdateMax */
IF NOT EXISTS (SELECT * FROM sysobjects WHERE id=OBJECT_ID(N'[Fireflies].[UpdateMax]')) BEGIN
	CREATE TABLE [Fireflies].[UpdateMax] ([UpdateId] [int] NOT NULL) ON [PRIMARY]
	INSERT INTO [Fireflies].[UpdateMax] VALUES (0)
END

/* Update */

IF NOT EXISTS (SELECT * FROM sysobjects WHERE id=OBJECT_ID(N'[Fireflies].[Update]')) BEGIN
	CREATE TABLE [Fireflies].[Update] ([UpdateId] [int] IDENTITY(1,1) NOT NULL, [AddedAt] [datetimeoffset](0) NOT NULL, [Schema] [nvarchar](50) NOT NULL, [Table] [nvarchar](50) NOT NULL, [Data] [nvarchar](max) NOT NULL, CONSTRAINT [PK_Fireflies_Update] PRIMARY KEY CLUSTERED ([UpdateId] ASC)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
	ALTER TABLE [Fireflies].[Update] ADD  CONSTRAINT [DF_Update_AddedAt]  DEFAULT (getutcdate()) FOR [AddedAt]
END

IF NOT EXISTS (SELECT * FROM sys.triggers WHERE object_id = OBJECT_ID(N'[Fireflies].[Fireflies_Update_Trigger]')) BEGIN
	SET @Sql='
CREATE TRIGGER [Fireflies].[Fireflies_Update_Trigger] ON [Fireflies].[Update] AFTER INSERT, UPDATE AS 
BEGIN
	SET NOCOUNT ON;

	UPDATE [Fireflies].[UpdateMax] SET [UpdateId]=(SELECT MAX(UpdateId) FROM INSERTED)
	DELETE FROM [Fireflies].[Update] WHERE [AddedAt] < DATEADD(MINUTE, -5, GETUTCDATE())
END'
	EXEC sp_executesql @Sql
	ALTER TABLE [Fireflies].[Update] ENABLE TRIGGER [Fireflies_Update_Trigger]
END
";
}