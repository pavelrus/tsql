--create table dbo.[data](
--	id int identity(1, 1)
--	,id_client int not null
--	,from_date date not null
--	,to_date date not null
--	,id_property int not null
--	,primary key(id));

--create index to_date_idx on dbo.[data](to_date);

/*
truncate table dbo.[data];

insert dbo.[data](id_client, from_date, to_date, id_property)
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
select 102, 4002 union all
select 103, 4001 union all
select 104, 4001 union all
select 105, 4001 union all
select 106, 4008 union all
select 107, 4008 union all
select 108, 4008 union all
select 112, 4002 union all
select 113, 4001 union all
select 114, 4005 union all
select 115, 4001 union all
select 116, 4006;

declare @toDay date = '20180603'; --getdate();
declare @yesterDay date = cast(@toDay as datetime) - 1;
declare @maxDate date = '22000101';

if object_id('tempdb..#changedData') <> 0 drop table #changedData;

create table #changedData(
	id_client int not null
	,id int
	,id_y int
	,id_property int
	,commandType smallint not null
	,primary key(id_client));
create index commandType_idx on #changedData(commandType);

insert #changedData(id_client, id, id_y, id_property, commandType)
select
	id_client
	,id
	,id_y
	,id_property
	,commandType
from (select
			coalesce(d.id_client, dy.id_client, s.id_client) id_client
			,d.id
			,dy.id id_y
			,s.id_property
			,case
				when d.from_date < @toDay and d.to_date > @toDay then
					case
						when s.id_client is null then 3 -- upd d.t = @yesterDay
						when d.id_property <> s.id_property then 103 -- upd d.t = @yesterDay, ins (@toDay, @maxDate, s.p)
					end
				when d.from_date = @toDay and d.to_date > @toDay then
					case
						when s.id_client is null then 1 -- upd d.t = @toDay
						when dy.id_property = s.id_property then 1007 -- del d.uid upd dy.t = @maxDate
						when d.id_property <> s.id_property then 10 -- upd d.p = s.p
					end
				when d.from_date <= @toDay and d.to_date = @toDay then
					case
						when d.id_property = s.id_property then 2 -- upd d.t = @maxDate
						when d.id_property <> s.id_property then 12 -- upd d.p = s.p, d.t = @maxDate
					end
				when d.id_client is null then
					case
						when (dy.to_date = @yesterDay and dy.id_property <> s.id_property) or (dy.id_client is null and not s.id_client is null) then 100 -- ins (@toDay, @maxDate, s.p)
						when dy.to_date = @yesterDay and dy.id_property = s.id_property then 7 -- upd dy.t = @maxDate
					end
			end commandType
		from ((select id, id_client, from_date, to_date, id_property from dbo.[data] where @toDay between from_date and to_date)d
				full join (select id, id_client, from_date, to_date, id_property from dbo.[data] where to_date = @yesterDay)dy
					on dy.id_client = d.id_client)
			full join #stageData s on s.id_client = isnull(d.id_client, dy.id_client))t
where not t.commandType is null;

set xact_abort on;

begin tran

merge dbo.[data] as target
	using (select * from #changedData where commandType in (10, 12)) as source
		on source.id = target.id
when matched then
	update set id_property = source.id_property;

merge dbo.[data] as target
	using (select * from #changedData where commandType in (1, 2, 12, 3, 103)) as source
		on source.id = target.id
when matched then
	update set to_date = case
							when source.commandType = 1 then @toDay
							when source.commandType in (2, 12) then @maxDate
							when source.commandType in (3, 103) then @yesterDay
						end;

merge dbo.[data] as target
	using (select * from #changedData where commandType in (100, 103)) as source
		on 1 = 0
when not matched then
	insert (id_client, from_date, to_date, id_property)
	values (source.id_client, @toDay, @maxDate, source.id_property);

merge dbo.[data] as target
	using (select * from #changedData where commandType in (7, 1007)) as source
		on source.id_y = target.id
when matched then
	update set to_date = @maxDate;

merge dbo.[data] as target
	using (select * from #changedData where commandType in (1007)) as source
		on source.id = target.id
when matched then
	delete;

commit tran

