--create table dbo.data_dt_log(
--	[uid] int not null identity(1, 1)
--	,ts datetime not null default getdate()
--	,id_client int not null
--	,id int
--	,id_property int
--	,commandType smallint not null
--	,primary key([uid]));

--create table dbo.[data_dt](
--	id int identity(1, 1)
--	,id_client int not null
--	,date_from datetime not null
--	,date_to datetime not null
--	,id_property int not null
--	,primary key(id));

/*
truncate table dbo.[data_dt];

insert dbo.[data_dt](id_client, date_from, date_to, id_property)
select 101, '19000101', '22000101', 4001 union all
select 102, '19000101', '22000101', 4001 union all
select 103, '19000101', '22000101', 4001 union all
select 104, '19000101', '22000101', 4001 union all
select 105, '19000101', '22000101', 4001;
*/

if object_id('tempdb..#stageData') <> 0 drop table #stageData;

create table #stageData(
	id_client int not null
	,id_property int not null
	,primary key(id_client));

insert #stageData
select 101, 4001 union all
select 102, 4001 union all
--select 103, 4001 union all
select 104, 4001 union all
select 105, 4002 union all
select 106, 4005 --union all
--select 107, 4002 --union all
--select 108, 4008 union all
--select 112, 4002 union all
--select 113, 4001 union all
--select 114, 4005 union all
--select 115, 4001 union all
--select 116, 4006;

declare @toDay datetime = getdate();
declare @maxDate datetime = '22000101';

if object_id('tempdb..#dwhData') <> 0 drop table #dwhData;

create table #dwhData(
	id int not null
	,id_client int not null
	,date_from datetime not null
	,date_to datetime not null
	,id_property int not null
	,primary key(id_client));

insert #dwhData
select id, id_client, date_from, date_to, id_property from dbo.[data_dt] where @toDay between date_from and date_to;

if object_id('tempdb..#changedData') <> 0 drop table #changedData;

create table #changedData(
	id_client int not null
	,id int
	,id_property int
	,commandType smallint not null
	,primary key(id_client));
create index commandType_idx on #changedData(commandType) include(id, id_property);

insert #changedData(id_client, id, id_property, commandType)
select
	id_client
	,id
	,id_property
	,commandType
from (
select
			isnull(d.id_client, s.id_client) id_client
			,d.id
			,s.id_property
			,case
				when s.id_client is null then 1 -- upd d.t = @toDay
				else
					case
						when d.id_client is null then 100 -- ins (@toDay, @maxDate, s.p)
						when d.id_property <> s.id_property then 103 -- upd d.t = @yesterDay, ins (@toDay, @maxDate, s.p)
					end
			end commandType
		from #dwhData d
			full join #stageData s on s.id_client = d.id_client)t
where not t.commandType is null;

insert dbo.data_dt_log([id_client],[id],[id_property],[commandType])
select [id_client],[id],[id_property],[commandType] from #changedData;

set xact_abort on;

begin tran

merge dbo.[data_dt] as target
	using (select * from #changedData where commandType in (1, 103)) as source
		on source.id = target.id
when matched then
	update set date_to = @toDay;

merge dbo.[data_dt] as target
	using (select * from #changedData where commandType in (100, 103)) as source
		on 1 = 0
when not matched then
	insert (id_client, date_from, date_to, id_property)
	values (source.id_client, @toDay, @maxDate, source.id_property);

commit tran
