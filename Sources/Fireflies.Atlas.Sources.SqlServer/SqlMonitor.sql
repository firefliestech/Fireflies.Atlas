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
			IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(''['' + @Schema + ''].'' + @TriggerName)) BEGIN
				SET @Sql = ''DROP TRIGGER ['' + @Schema + ''].'' + @TriggerName
				EXEC sp_executesql @Sql
			END
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

