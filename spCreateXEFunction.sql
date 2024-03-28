/*
**	Create a table-valued function to return shredded Extended Event target data.  
*/
CREATE	PROCEDURE spCreateXEFunction
				@SessionName		sysname,					-- Name of existing Extended Event session
				@FunctionName		sysname,					-- Name of function to create or overwrite
				@RecreateFunction	tinyint = 0,				-- Whether existing @FunctionName should be overwritten
				@Target				char(1) = 'R',				-- see @Target validation below
				@EventNames			nvarchar(max) = NULL,		-- Optional list of Extended Events to query; NULL = ALL
				@ColumnOrder		char(1) = 'A',				-- C = order by most Common columns first; <> C = order columns Alphabetically
				@NamingConvention	char(1) = 'P'				-- see @Naming validation below
AS
BEGIN
	SET	NOCOUNT ON
	BEGIN	TRY
		IF		OBJECT_ID(@FunctionName) IS NOT NULL AND @RecreateFunction = 0
				RAISERROR('%s already exists', 16, 1, @FunctionName) 
		IF		ISNULL(PARSENAME(@FunctionName, 3), DB_NAME()) <> DB_NAME()
				RAISERROR('Function can only be created in current database', 16, 1)
		IF		OBJECT_ID(@FunctionName) IS NULL AND @RecreateFunction = 1
				SELECT	@RecreateFunction = 0
		IF		@NamingConvention NOT IN ('P', 'C', 'S')
				RAISERROR('Invalid @NamingConvention:  P=PascalCase, C=camelCase, S=snake_case', 16, 1)
		if		@Target NOT IN ('R', 'F')
				RAISERROR('Invalid @Target: R=ring_buffer, F=event_file', 16, 1)

		/*
		**	Create temporary table to convert Extended Event to SQL datatype. Types are ordered by compatibility.
		**	If there are multiple events with the same column name and different datatypes, the type with the lowest TypeID 
		**	will be used.  Types not included are converted to nvarchar(max).  Note that there  may be combinations of events 
		**	with the same column name and incompatible datatypes.  In this case, the function will fail.  This is a known limitation.
		*/
		DECLARE	@TypeConversions TABLE (
				XEDataType		sysname,
				SQLType			sysname,
				TypeID			tinyint)

		INSERT	INTO @TypeConversions
				SELECT	XEDataType, SQLType, TypeID 
						FROM	(VALUES
								('unicode_string', 'nvarchar(max)', 1),
								('binary_data', 'varbinary(max)', 2),
								('float64', 'real', 3),
								('uint64', 'decimal(20, 0)', 4),
								('int64', 'bigint', 5),
								('ptr', 'varchar(16)', 6),
								('uint32', 'decimal(10, 0)', 7),
								('int32', 'int', 8),
								('uint16', 'decimal(5, 0)', 9),
								('boolean', 'nvarchar(5)', 10),
								('int16', 'smallint', 11),
								('uint8', 'tinyint', 12),
								('int8', 'tinyint', 13),
								('filetime', 'datetime', 14),
								('guid', 'uniqueidentifier', 15),
								('xml', 'xml', 16)) t(XEDataType, SQLType, TypeID);

		/*
		**	Query the selected events of the selected session into a temporary table to query the event columns
		**	and determine the number of events included.
		*/
		DECLARE	@Events TABLE (
				SessionID		int,
				EventID			int,
				EventName		sysname,
				PackageGUID		uniqueidentifier)
		DECLARE	@NumEvents		int

		INSERT	INTO @Events
				SELECT	s.event_session_id, e.event_id, e.name event_name, p.guid
						FROM	sys.server_event_sessions s
						JOIN	sys.server_event_session_events e
							ON	e.event_session_id = s.event_session_id
						JOIN	sys.dm_xe_packages p
							ON	p.name = e.package 
							AND	p.module_guid = e.module
						WHERE	s.name = @SessionName
						AND		(@EventNames IS NULL
						OR		 e.name IN (
								SELECT	value FROM STRING_SPLIT(@EventNames, ',')))
		/*
		**	We need to know how many total events the function will return, so we can selectively query the columns 
		**	that aren't included in all the events (see the @ResultColumns update below).
		*/
		SELECT	@NumEvents = @@ROWCOUNT
		IF		@NumEvents = 0
				RAISERROR('No events found', 16, 1)

		DECLARE	@EventColumns	TABLE (
				EventName		sysname,
				ColName			sysname,
				DataType		sysname,	    -- initially the Extended Event datatype; updated to the SQL datatype
				NodeName		varchar(8),		-- "data" or "action"
				Element			varchar(8));    -- "value" or "text"

		/*
		**	Query all the global fields (<action> elements) and event fields (<data> elements). For mapped  
		**	columns in the data elements, the descriptive text is returned instead of the internal numeric value.
		*/
		INSERT	INTO @EventColumns
				SELECT	e.EventName, o.name, o.type_name, o.object_type, 'value' 
						FROM	@Events e
						JOIN	sys.server_event_session_actions a
							ON	a.event_session_id = e.SessionID
							and	a.event_id = e.EventID
						JOIN	sys.dm_xe_objects o
							ON	o.name = a.name
						WHERE	o.object_type = 'action'
				UNION
				SELECT	e.EventName, c.name, c.type_name, c.column_type, 
						IIF(ISNULL(o.object_type, '') <> 'map', 'value', 'text')
						FROM	@Events e
						JOIN	sys.dm_xe_object_columns c
							ON	c.object_name = e.EventName
					LEFT JOIN	sys.dm_xe_objects o
							ON	o.name = c.name 
						WHERE	c.column_type = 'data'
							/*
							**	Eliminate columns not selected for collection
							*/
							AND	(c.capabilities = 0 
							OR	 EXISTS (
									SELECT	1 FROM sys.server_event_session_fields o
											WHERE	o.event_session_id = e.SessionID
											AND		o.name = 'collect_' + c.name
											AND		o.value = 1))
		IF		@@ROWCOUNT = 0
				RAISERROR('No event columns found', 16, 1)
		/*
		**	Translate the Extended Event datatype to the SQL datatype.  All columns 
		**	with the same name will be given the same datatype.
		*/
		UPDATE	c1
				SET		DataType = (
						SELECT	TOP 1 ISNULL(SQLType, 'nvarchar(max)')
								FROM	@EventColumns c2
							LEFT JOIN	@TypeConversions t
									ON	t.XEDataType = c2.DataType
								WHERE	c2.ColName = c1.ColName
							ORDER BY	t.TypeID)
				FROM	@EventColumns c1;

		DECLARE	@ResultColumns	TABLE (
				DisplayName		sysname,
				DataType		sysname,
				XSMethod		nvarchar(max),
				ColOrder		int);

		/*
		**	Build a XQuery string for each distinct column name.  If the column name is included in 
		**	multiple events, it's possible it could be both a global field and event field, in which
		**	case we query the nodes conditinally depending on the event.  The EventNames are strung together
		**	and counted so we can avoid calling the xml method if the column isn't included in the event. 
		*/
		WITH	cteColumnDef AS (
				SELECT	ColName, NodeName,
						STRING_AGG(''''+EventName+'''', ',') Events,
						COUNT(*) NumEvents,
						Element, DataType,
						/*
						**	I threw in the optional naming convention because I don't like underscores
						*/
						CONVERT(nvarchar(MAX), QUOTENAME(IIF(@NamingConvention = 'S', ColName, 
							(SELECT STRING_AGG(IIF(Row = 1 AND @NamingConvention = 'C', value, 
												UPPER(LEFT(value, 1)) + RIGHT(value, LEN(VALUE) - 1)), '')
									FROM	(SELECT value, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) Row 
												FROM STRING_SPLIT(ColName, '_')) nc)))) DisplayName
						FROM	@EventColumns 
					GROUP BY	ColName, Element, DataType, NodeName),
				cteConsolidateCols AS (
				SELECT	ColName, NodeName, Element, DataType, DisplayName, Events, 
						IIF(NumEvents <> @NumEvents, 1, 0) Conditional,
						CONVERT(nvarchar(max), 
							CASE	DataType
									WHEN 'xml'				THEN CONCAT('e.query(''event/', NodeName, '[@name="', ColName, '"]/value/*'') ')
									WHEN 'varbinary(max)'	THEN CONCAT('e.value(''xs:hexBinary((event/', NodeName, '[@name="', ColName, '"]/value)[1])'', ''', DataType, ''') ')
															ELSE CONCAT('e.value(''(event/', NodeName, '[@name="', ColName, '"]/', Element, ')[1]'', ''', DataType, ''') ')
							END)	XSMethod,
						DENSE_RANK() OVER (ORDER BY IIF(@ColumnOrder = 'C', NumEvents, 0) DESC, ColName) ColOrder								
						FROM	cteColumnDef)
				INSERT	INTO @ResultColumns (DisplayName, DataType, XSMethod, ColOrder)
						SELECT	DisplayName, DataType,
								CONCAT(IIF(Conditional = 0, '', 'CASE '),
										STRING_AGG(IIF(Conditional = 0, '', 'WHEN EventName IN ('+Events+') THEN ')+XSMethod+'
										', ''),
										IIF(Conditional = 0, '', 'END')),
								ColOrder
								FROM	cteConsolidateCols
							GROUP BY	DisplayName, DataType, Conditional, ColOrder
								

		DECLARE	@Stmt		nvarchar(max)
		SELECT	@FunctionName = CONCAT(QUOTENAME(ISNULL(PARSENAME(@FunctionName, 2), 'dbo')), 
									'.', QUOTENAME(PARSENAME(@FunctionName, 1)));

		/*
		**	Create the ddl for the function and execute it.
		*/
		SELECT	@Stmt = CONCAT(IIF(@RecreateFunction = 1, 'ALTER', 'CREATE'), ' FUNCTION ', @FunctionName, '(', 
				IIF(@Target = 'R', '', '
				@Path															nvarchar(260),
				@MDPath															nvarchar(260),
				@InitialFileName												nvarchar(260),
				@InitialOffset													bigint'), ')
RETURNS @EventsRet TABLE (
				EventTime                                                       datetime,
				EventName                                                       sysname,', 
				STRING_AGG(CONCAT('
				', CONVERT(nvarchar(max), DisplayName), REPLICATE(' ', 64 - LEN(DisplayName)), DataType), ',') 
					WITHIN GROUP (ORDER BY ColOrder), ') AS
BEGIN
		DECLARE	@TargetData		xml
		', IIF(@Target = 'F', 
				'SELECT	@TargetData = (
						SELECT	CONVERT(xml, event_data)
								FROM	sys.fn_xe_file_target_read_file(@Path, @MDPath, @InitialFileName, @InitialOffset)
									FOR	XML PATH)',
				CONCAT('SELECT	@TargetData = CONVERT(xml, t.target_data)
				FROM	sys.dm_xe_session_targets t
				JOIN	sys.dm_xe_sessions s 
					ON	s.address = t.event_session_address
				WHERE	s.name = ''', @SessionName, '''
					AND	t.target_name = ''ring_buffer''')
				)/*ENDIIF*/, '

		INSERT	INTO @EventsRet
				SELECT	EventTime,
						EventName,', STRING_AGG(CONCAT('
						',XSMethod, ' ', DisplayName), ',') WITHIN GROUP (ORDER BY ColOrder), '
						FROM	(
								SELECT	Event.value(''(@timestamp)[1]'', ''datetime'') EventTime,
										Event.value(''(@name)[1]'', ''sysname'') EventName, 
										Event.query(''.'') e
										FROM	@TargetData.nodes(''', IIF(@Target = 'F', 'row', 'RingBufferTarget'), '/event'') Target(Event)',
						IIF(@EventNames IS NULL, '', '
										WHERE	Event.value(''(event/@name)[1]'', ''sysname'') IN ('+
								(SELECT STRING_AGG('''' + value + '''', ',') FROM STRING_SPLIT(@EventNames, ','))+')')/*ENDIIF*/, ') Events 

		RETURN
END')			FROM	@ResultColumns
		SELECT	@Stmt
		EXEC	sp_executesql @Stmt

	
	END	TRY
	BEGIN CATCH
		THROW;
	END CATCH;
END