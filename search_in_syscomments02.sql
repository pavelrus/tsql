drop table if exists #key_word;
create table #key_word
(
	key_word sysname primary key
);

insert #key_word
values
	('dead'), ('lock');
  --('mrrfRequest')--('clientService') --('mrrf')

declare @key_word_OR char(1) = '0';
declare @searchinnames char(1) = '2'; -- 0 - no, 1 - yes, 2 - only
declare @include_system_tables char(1) = '0';
declare @dbs nvarchar(max) --= '''test'', ''sten'', ''WideWorldImporters'', cbr';

--
drop table if exists #data_in_comment;
create table #data_in_comment
(
	[object_name] nvarchar(257) not null,
	[type] char(2) not null,
	[type_desc] nvarchar(70),
	parent_object_name nvarchar(257),
	parent_type char(2),
	parent_type_desc nvarchar(70),
	DB nvarchar(128) not null,
  [set] tinyint not null
);

declare @key_word sysname, @name_db sysname, @collation_name sysname, @query1 nvarchar(max), @query_foreachdb nvarchar(max);
declare @query_final1 nvarchar(max);
declare @subquery01 nvarchar(max) = '',  @subquery02 nvarchar(max) = '',  @subquery03 nvarchar(max) = '',  @subquery04 nvarchar(max) = '';

declare record cursor for
select
	key_word
from #key_word;

declare record_db cursor for
select
  d.[name],
  d.collation_name
from sys.databases d
  left join
      (
        select
          trim(replace(replace(replace(value, '''', ''), '[', ''), ']', '')) [name]
        from string_split(@dbs, ',')
      ) dbs
    on dbs.[name] = d.[name]
where not d.[name] in ('master', 'tempdb', 'model', 'msdb', 'SSISDB')
  and ((not dbs.[name] is null and not @dbs is null) or @dbs is null);

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
    parent_object_name,
    parent_type,
    parent_type_desc,
		DB,
    [set],
		[' + @key_word + ']
	)
	select
		s.name + ''.'' + o.name,
		o.[type],
		o.[type_desc],
		sp.name + ''.'' + op.name,
		op.[type],
		op.[type_desc],
		db_name() DB,
    1,
		1
	from sys.objects (nolock) o
		join (select distinct id from sys.syscomments (nolock) where lower(text) like ''%' + @key_word + '%'') oo on oo.id = o.object_id
		join sys.schemas (nolock) s on s.schema_id = o.schema_id
    left join sys.objects (nolock) op on op.object_id = o.parent_object_id
    left join sys.schemas (nolock) sp on sp.schema_id = op.schema_id
  where ''' + @searchinnames + ''' <> ''2'' and (o.[type] <> ''S'' or ''' + @include_system_tables + ''' = ''1'')
	 union
	select
		iif(o.[type] in (''D'', ''PK'', ''UQ''), '''', s.name + ''.'') + o.name,
		o.[type],
		o.[type_desc],
		sp.name + ''.'' + op.name,
		op.[type],
		op.[type_desc],
		db_name(),
    2,
		1
	from sys.objects (nolock) o
		join sys.schemas (nolock) s on s.schema_id = o.schema_id
    left join sys.objects (nolock) op on op.object_id = o.parent_object_id
    left join sys.schemas (nolock) sp on sp.schema_id = op.schema_id
	where o.name like ''%' + @key_word + '%'' and ''' + @searchinnames + ''' in (''1'', ''2'') and (o.[type] <> ''S'' or ''' + @include_system_tables + ''' = ''1'')
	 union
	select
		c.name,
		''C'',
		''COLUMN'',
		sp.name + ''.'' + op.name,
		op.[type],
		op.[type_desc],
		db_name(),
    3,
		1
	from sys.columns (nolock) c
    join sys.objects (nolock) op on op.object_id = c.object_id
    join sys.schemas (nolock) sp on sp.schema_id = op.schema_id
	where c.name like ''%' + @key_word + '%'' and ''' + @searchinnames + ''' in (''1'', ''2'') and (op.[type] <> ''S'' or ''' + @include_system_tables + ''' = ''1'')
   union
  select
		s.name,
		''SB'',
		s.[type_desc],
		null,
		null,
		null,
		db_name(),
    4,
		1
	from (
    select name collate $$@collation_name$$ name, ''SB_MESSAGE_TYPE'' [type_desc] from sys.service_message_types
     union all
    select name collate $$@collation_name$$, ''SB_CONTRACT'' from sys.service_contracts
     union all
    select name collate $$@collation_name$$, ''SB_SERVICE'' from sys.services) s
	where s.name like ''%' + @key_word + '%'' and ''' + @searchinnames + ''' in (''1'', ''2'');';

  open record_db

  while 1 = 1
  begin
    fetch record_db into @name_db, @collation_name;
	  if @@fetch_status <> 0 break;

    set @query_foreachdb = 'use [' + @name_db + ']' + replace(@query1, '$$@collation_name$$', @collation_name);
  
    exec (@query_foreachdb);
  end

  close record_db

	set @subquery01 += ',[' + @key_word + '].[' + @key_word + ']';
	set @subquery02 += '	' + iif(@key_word_OR = 1, 'left', '') + ' join #data_in_comment [' + @key_word + '] on [' + @key_word + '].[object_name] = o.[object_name] and [' + @key_word + '].[' + @key_word + '] = 1 and isnull([' + @key_word + '].parent_object_name, '''') = isnull(o.parent_object_name, '''') and [' + @key_word + '].DB = o.DB and [' + @key_word + '].[set] = o.[set]';
	set @subquery03 += iif(len(@subquery03) = 0, '', ' + ') + 'isnull([' + @key_word + '].[' + @key_word + '], 0)';
	set @subquery04 += ',[' + @key_word + '].[' + @key_word + ']';
end

deallocate record_db

close record
deallocate record

set @query_final1 = '
select
	o.DB
	,o.[object_name]
	,o.[type]
	,o.[type_desc]
  ,o.parent_object_name
  ,o.parent_type
  ,o.parent_type_desc
	' + @subquery01 + '
  ,o.[set]
from (select distinct [object_name], [type], [type_desc], parent_object_name, parent_type, parent_type_desc, DB, [set] from #data_in_comment) o
' + @subquery02 + '
order by
  o.DB
	,case isnull(o.parent_type, o.[type]) when ''P'' then 1 when ''V'' then 2 when ''IF'' then 3 when ''FN'' then 4 when ''U'' then 5 else 100 end
	,isnull(o.parent_type, o.[type])
	,' + @subquery03 + ' desc
	' + @subquery04 + '
  ,isnull(o.parent_object_name, o.[object_name])
	,o.[object_name];';

exec (@query_final1);
