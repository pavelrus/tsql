drop table if exists #key_word;
create table #key_word
(
	key_word sysname primary key
);

insert #key_word
values
	('key_word') --, ('dev_02_DM3');

declare @key_word_OR char(1) = '0';
declare @searchinnames char(1) = '0';
declare @dbs varchar(500) = '''database_name1'', ''database_name2'', ''-database_name3''';

--
drop table if exists #data_in_comment;
create table #data_in_comment
(
	[object_name] nvarchar(257) not null,
	[type] char(2) not null,
	[type_desc] nvarchar(70),
	DB nvarchar(128) not null
);

declare @key_word sysname, @query1 nvarchar(max), @query_foreachdb nvarchar(max);
declare @query_final1 nvarchar(max);
declare @subquery01 nvarchar(max) = '',  @subquery02 nvarchar(max) = '',  @subquery03 nvarchar(max) = '',  @subquery04 nvarchar(max) = '';

declare record cursor for
select
	key_word
from #key_word;

open record

while 1 = 1
begin
	fetch record into @key_word;
	if @@fetch_status <> 0 break;

	set @query1 = 'alter table #data_in_comment add [' + @key_word + '] smallint;';

	exec (@query1);
	
	set @query1 = 'insert #data_in_comment
	(
		object_name,
		[type],
		[type_desc],
		DB,
		[' + @key_word + ']
	)
	select distinct
		s.name + ''.'' + o.name [table_name],
		o.[type],
		o.[type_desc],
		db_name() DB,
		1
	from sys.objects (nolock) o
		join (select distinct id from sys.syscomments (nolock) where lower(text) like ''%' + @key_word + '%'') oo on oo.id = o.object_id
		join sys.schemas (nolock) s on s.schema_id = o.schema_id
	 union all
	select
		s.name + ''.'' + o.name,
		o.[type],
		o.[type_desc],
		db_name(),
		1
	from sys.objects (nolock) o
		join sys.schemas (nolock) s on s.schema_id = o.schema_id
	where o.name like ''%' + @key_word + '%'' and ''1'' = ''' + @searchinnames + '''
	 union all
	select
		s.name + ''.'' + o.name + ''.'' + c.name,
		o.[type],
		o.[type_desc] + '' / COLUMN'',
		db_name(),
		1
	from sys.objects (nolock) o
		join sys.columns c on c.object_id = o.object_id
		join sys.schemas (nolock) s on s.schema_id = o.schema_id
	where c.name like ''%' + @key_word + '%'' and ''1'' = ''' + @searchinnames + ''';';

	set @query_foreachdb = 'if ''?'' in (''master'', ''model'', ''msdb'', ''tempdb'') return

		if not ''?'' in (' + @dbs + ') return

		use [?]

		' + @query1;
	
	exec sp_MSforeachdb @query_foreachdb;
	
	set @subquery01 += ',[' + @key_word + '].[' + @key_word + ']';
	set @subquery02 += '	' + iif(@key_word_OR = 1, 'left', '') + ' join #data_in_comment [' + @key_word + '] on [' + @key_word + '].[object_name] = o.[object_name] and [' + @key_word + '].[' + @key_word + '] = 1';
	set @subquery03 += iif(len(@subquery03) = 0, '', ' + ') + 'isnull([' + @key_word + '].[' + @key_word + '], 0)';
	set @subquery04 += ',[' + @key_word + '].[' + @key_word + ']';
end

close record
deallocate record

set @query_final1 = '
select
	o.DB
	,o.[object_name]
	,o.[type]
	,o.[type_desc]
	' + @subquery01 + '
from (select distinct [object_name], [type], [type_desc], DB from #data_in_comment) o
' + @subquery02 + '
order by
	case o.[type] when ''P'' then 1 when ''V'' then 2 when ''IF'' then 3 when ''FN'' then 4 else 100 end
	,o.[type]
	,' + @subquery03 + ' desc
	' + @subquery04 + '
	,o.[object_name];';

exec (@query_final1);
