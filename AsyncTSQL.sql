CREATE	MESSAGE TYPE mtAsyncRequest
		VALIDATION = NONE; 
GO
CREATE	MESSAGE TYPE mtAsyncResponse
		VALIDATION = NONE; 
CREATE	CONTRACT 
		coAsync (    
				mtAsyncRequest SENT BY INITIATOR,    
				mtAsyncResponse SENT BY TARGET);  
GO
CREATE	PROCEDURE spRunAsync AS
BEGIN	TRY
		DECLARE	@Handle		uniqueidentifier,
				@AsyncStmt	nvarchar(max),
				@MsgType	sysname

		DECLARE	@ResultSet TABLE (
				ColNum		smallint,
				ColName		sysname,
				ColType		nvarchar(256),
				ErrorNum	int,
				ErrorMsg	nvarchar(max));
		DECLARE	@ResultCols	int,
				@JsonColDef	nvarchar(max),
				@Stmt		nvarchar(max),
				@JsonResult	nvarchar(max),
				@Result		nvarchar(max);

		WHILE	1 = 1
		BEGIN
				WAITFOR (
						RECEIVE TOP (1) 
								@Handle		= conversation_handle,
								@AsyncStmt	= message_body,
								@MsgType	= message_type_name
								FROM	qAsyncRequest), TIMEOUT 1000;
				IF		@@ROWCOUNT = 0 
				OR		@MsgType <> 'mtAsyncRequest'
						BREAK

				INSERT	INTO @ResultSet
						SELECT	column_ordinal, ISNULL(name, CONCAT('Col', column_ordinal)), system_type_name, error_number, error_type_desc
								FROM	sys.dm_exec_describe_first_result_set(@AsyncStmt, null, 0)
				SELECT	@ResultCols = @@ROWCOUNT

				DECLARE	@ErrorNum	int,
						@ErrorMsg	nvarchar(max)
				SELECT	TOP 1 @ErrorNum = ErrorNum, @ErrorMsg = ErrorMsg
						FROM	@ResultSet
						WHERE	ErrorNum IS NOT NULL
				IF		@@ROWCOUNT > 0
				BEGIN
						SELECT	@ErrorMsg = CONCAT(N'Error ', convert(nvarchar, @ErrorNum), ': ', @ErrorMsg)
						RAISERROR(@ErrorMsg, 16, 1)
				END

				IF		@ResultCols > 0
				BEGIN
						SELECT	@JsonColDef = 
								STRING_AGG(CONCAT(CONVERT(nvarchar(max), QUOTENAME(ColName)), ' ', ColType, '''', '$.', ColName, ''''), ',') WITHIN GROUP (ORDER BY ColNum)
								FROM	@ResultSet
						SELECT	@Stmt = CONCAT(N'CREATE TABLE #res (',
								STRING_AGG(CONCAT(CONVERT(nvarchar(max), QUOTENAME(ColName)), ' ', ColType), ',') WITHIN GROUP (ORDER BY ColNum), ');', 
								N'INSERT INTO #res ', @AsyncStmt, ';',
								N'SELECT @JsonResult = (SELECT * FROM #res FOR JSON AUTO)')
								FROM	@ResultSet
						EXEC	sp_executesql @Stmt, N'@JsonResult nvarchar(max) OUTPUT', @JsonResult = @JsonResult OUTPUT
				END
				ELSE
						EXEC	sp_executesql @AsyncStmt

				SELECT	@Result = (
						SELECT	@@ROWCOUNT	Rows,
								'OK'		Status,
								@JsonResult	Result,
								@JsonColDef	JsonColDef
								FOR	JSON PATH);
				SEND	ON CONVERSATION @Handle 
						MESSAGE TYPE mtAsyncResponse
						(@Result);
				END		CONVERSATION @Handle;
				DELETE	FROM @ResultSet
		END
		RETURN
END		TRY
BEGIN	CATCH
		IF		@Handle is null
				RETURN

		SELECT	@Result = (
				SELECT	-1		Rows,
						'FAIL'	Status,
						CONCAT('Error ', CONVERT(VARCHAR, ERROR_NUMBER()),
								' at line ', CONVERT(VARCHAR, ERROR_LINE()), ': ', ERROR_MESSAGE()) Result,
						NULL	JsonColDef 
						FOR	JSON PATH);
		SEND	ON CONVERSATION @Handle
				MESSAGE TYPE mtAsyncResponse 
				(@Result);
		END		CONVERSATION @Handle;
		RETURN
END		CATCH
GO
CREATE	QUEUE qAsyncRequest WITH
		ACTIVATION (  
			STATUS = ON , 
			PROCEDURE_NAME = spRunAsync, 
			MAX_QUEUE_READERS = 20 ,
			EXECUTE AS OWNER  );
GO
CREATE	QUEUE qAsyncResponse; 
GO
CREATE	SERVICE svAsyncRequest  
		ON QUEUE qAsyncRequest (coAsync);
GO
CREATE	SERVICE svAsyncResponse  
		ON QUEUE qAsyncResponse (coAsync)
GO
CREATE	PROCEDURE spAsyncRequest
				@Stmt		nvarchar(max),
				@Handle		uniqueidentifier = null OUTPUT
AS
		BEGIN	DIALOG CONVERSATION @Handle
				FROM	SERVICE svAsyncResponse
				TO		SERVICE 'svAsyncRequest'
				ON		CONTRACT coAsync;
		SEND	ON CONVERSATION @Handle
				MESSAGE TYPE	mtAsyncRequest
				(@Stmt)
GO
CREATE	PROCEDURE [dbo].[spAsyncResponse] 
				@Handle				uniqueidentifier,				--conversation handle returned by spAsyncRequest
				@ResultSetFormat	char(1) = 'Q',					--return result set (if any) as [Q]uery or [Json]	
				@ResultSet			nvarchar(max) = NULL OUTPUT,	--returned if @ResultSetFormat = 'J'
				@JsonColDef			nvarchar(max) = NULL OUTPUT,	--returned if @ResultSetFormat = 'J' to query @ResultSet dynamically 
				@Timeout			int = 30						--timeout in seconds
AS
BEGIN	TRY
		DECLARE	@Response			nvarchar(max),
				@MsgType			sysname,
				@Rows				int,
				@Status				varchar(8),
				@Stmt				nvarchar(max);

		SELECT	@Timeout = @Timeout * 1000;
		WAITFOR(
				RECEIVE	TOP (1)
						@Response	= message_body,
						@MsgType	= message_type_name
						FROM	qAsyncResponse
						WHERE	conversation_handle = @Handle),
				TIMEOUT @Timeout;
		IF		@@ROWCOUNT = 0
		BEGIN
				SELECT	@Response = CONCAT('No response for conversation ', CONVERT(varchar(36), @Handle))
				RETURN	-1
		END
		IF		@MsgType <> 'mtAsyncResponse'
				RAISERROR('Unexpected message type (%s)', 16, 1, @MsgType)

		SELECT	@Rows = Rows, @Status = Status, @ResultSet = Result, @JsonColDef = JsonColDef
				FROM	OPENJSON(@Response) WITH (
						Rows		int				'$.Rows',
						Status		varchar(8)		'$.Status',
						Result		nvarchar(max)	'$.Result',
						JsonColDef	nvarchar(max)	'$.JsonColDef')
		IF		@@ROWCOUNT = 0
				RAISERROR('Unexpected response (%s)', 16, 1, @Response)

		IF		@Status = 'OK' AND @Rows > 0 AND @ResultSetFormat = 'Q'
		BEGIN
				SELECT	@Stmt = CONCAT(N'SELECT	* FROM OPENJSON(@Result) WITH (', @JsonColDef, ')')
				EXEC	sp_executesql @Stmt, N'@Result nvarchar(max)', @ResultSet
		END
		ELSE IF	@Status = 'FAIL'
				RAISERROR(@ResultSet, 16, 1)
		END		CONVERSATION @Handle

		RETURN	@Rows
END		TRY
BEGIN	CATCH
		THROW;
		IF		@Handle is not null
				END	CONVERSATION @Handle
		RETURN	-1
END		CATCH
GO
