--create table dbo.[data_exists](
--	id int identity(1, 1)
--	,id_client int not null
--	,date_from date not null
--	,date_to date not null
--	,primary key(id));

--create table dbo.data_exists_log(
--	[uid] int not null identity(1, 1)
--	,ts datetime not null default getdate()
--	,id_client int not null
--	,id int
--	,id_y int
--	,commandType smallint not null
--	,primary key([uid]));

/*
truncate table dbo.[data_exists];

insert dbo.[data_exists](id_client, date_from, date_to)
select 101, '19000101', '22000101' union all
select 102, '19000101', '22000101' union all
select 103, '19000101', '22000101' union all
select 104, '19000101', '22000101' union all
select 105, '19000101', '22000101';
*/

if object_id('tempdb..#stageData') <> 0 drop table #stageData;

create table #stageData(
	id_client int not null
	,primary key(id_client));

insert #stageData
select 101 union all
select 102 union all
select 103 union all
select 104 union all
select 105 union all
select 106 union all
select 107 union all
select 108 -- union all
--select 112, 4002 union all
--select 113, 4001 union all
--select 114, 4005 union all
--select 115, 4001 union all
--select 116, 4006;

declare @toDay date = '20180602'; --getdate();
declare @yesterDay date = cast(@toDay as datetime) - 1;
declare @maxDate date = '22000101';

if object_id('tempdb..#dwhData') <> 0 drop table #dwhData;

create table #dwhData(
	id int not null
	,id_client int not null
	,date_from date not null
	,date_to date not null
	,primary key(id_client));

insert #dwhData
select id, id_client, date_from, date_to from dbo.data_exists where @toDay between date_from and date_to;

if object_id('tempdb..#dwhDataOnYesterday') <> 0 drop table #dwhDataOnYesterday;

create table #dwhDataOnYesterday(
	id int not null
	,id_client int not null
	,primary key(id_client));

insert #dwhDataOnYesterday
select id, id_client from dbo.data_exists where date_to = @yesterDay;

if object_id('tempdb..#changedData') <> 0 drop table #changedData;

create table #changedData(
	id_client int not null
	,id int
	,id_y int
	,commandType smallint not null
	,primary key(id_client));
create index commandType_idx on #changedData(commandType) include(id, id_y);

insert #changedData(id_client, id, id_y, commandType)
select
	id_client
	,id
	,id_y
	,commandType
from (select
			coalesce(d.id_client, dy.id_client, s.id_client) id_client
			,d.id
			,dy.id id_y
			,case
				when s.id_client is null then
					case
						when d.date_to > @toDay then
							case
								when d.date_from = @toDay then 1 -- upd d.t = @toDay
								else 3 -- upd d.t = @yesterDay
							end
					end
				else
					case
						when d.id_client is null then
							case
								when dy.id_client is null then 100 -- ins (@toDay, @maxDate, s.p)
								else 7 -- upd dy.t = @maxDate
							end
						when d.date_to = @toDay then 2 -- upd d.t = @maxDate
					end
			end commandType
		from (#dwhData d
				full join #dwhDataOnYesterday dy
					on dy.id_client = d.id_client)
			full join #stageData s on s.id_client = isnull(d.id_client, dy.id_client))t
where not t.commandType is null;

insert dbo.data_exists_log([id_client],[id],[id_y],[commandType])
select [id_client],[id],[id_y],[commandType] from #changedData;

set xact_abort on;

begin tran

merge dbo.[data_exists] as target
	using (select * from #changedData where commandType in (1, 2, 3)) as source
		on source.id = target.id
when matched then
	update set date_to = case source.commandType
							when 1 then @toDay
							when 2 then @maxDate
							when 3 then @yesterDay
						end;

merge dbo.[data_exists] as target
	using (select * from #changedData where commandType = 100) as source
		on 1 = 0
when not matched then
	insert (id_client, date_from, date_to)
	values (source.id_client, @toDay, @maxDate);

merge dbo.[data_exists] as target
	using (select * from #changedData where commandType = 7) as source
		on source.id_y = target.id
when matched then
	update set date_to = @maxDate;

commit tran
