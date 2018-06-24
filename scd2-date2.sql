--create table dbo.demo(
--	[uid] int identity(1, 1)
--	,id int not null
--	,f date not null
--	,t date not null
--	,p int not null
--	,primary key([uid]));
--go
/*
truncate table dbo.demo;

insert dbo.demo(id, f, t, p)
select 101, '19000101', '22000101', 4001 union all
select 102, '19000101', '22000101', 4001 union all
select 103, '19000101', '22000101', 4001 union all
select 104, '19000101', '22000101', 4001 union all
select 105, '19000101', '22000101', 4001;
*/

--create index f_t_idx on [dbo].[demo](f, t);

if object_id('tempdb..#s') <> 0 drop table #s;

create table #s(
	id int not null
	,p int not null
	,primary key(id));

insert #s
select 101, 4008 union all
select 102, 4008 union all
select 103, 4008 union all
select 104, 4008 union all
select 105, 4008 union all
select 106, 4008 union all
select 107, 4008;

declare @td date = '20180625' --getdate();
declare @y date = cast(@td as datetime) - 1;
declare @max date = '22000101';

if object_id('tempdb..#t') <> 0 drop table #t;

create table #t(
	id int not null
	,[uid] int
	,uid_y int
	,p int
	,commandType smallint not null
	,primary key(id));
create index commandType_idx on #t(commandType);

insert #t(id, [uid], uid_y, p, commandType)
select
	id
	,[uid]
	,uid_y
	,p
	,commandType
from (select
			coalesce(d.id, dy.id, s.id) id
			,d.[uid]
			,dy.[uid] uid_y
			,s.p p
			,case
				when d.f < @td and d.t > @td then
					case
						when d.p <> s.p then 103 -- upd d.t = @y, ins (@td, @max, s.p)
						when s.id is null then 3 -- upd d.t = @y
					end
				when d.f = @td and d.t > @td then
					case
						when d.p <> s.p then 10 -- upd d.p = s.p
						when dy.p = s.p then 1007 -- del d.uid upd dy.t = @max
						when s.id is null then 1 -- upd d.t = @td
					end
				when d.f <= @td and d.t = @td then
					case
						when d.p <> s.p then 12 -- upd d.p = s.p, d.t = @max
						when d.p = s.p then 2 -- upd d.t = @max
					end
				when d.id is null then
					case
						when (dy.t = @y and dy.p <> s.p) or (dy.id is null and not s.id is null) then 100 -- ins (@td, @max, s.p)
						when dy.t = @y and dy.p = s.p then 7 -- upd dy.t = @max
					end
			end commandType
		from ((select [uid], id, f, t, p from dbo.demo where @td between f and t)d
				full join (select [uid], id, f, t, p from dbo.demo where @y between f and t)dy
					on dy.id = d.id)
			full join #s s on s.id = isnull(d.id, dy.id))t
where not t.commandType is null;

set xact_abort on;

begin tran

merge dbo.demo as target
	using (select * from #t where commandType in (10, 12)) as source
		on source.[uid] = target.[uid]
when matched then
	update set p = source.p;

merge dbo.demo as target
	using (select * from #t where commandType in (1, 2, 12, 3, 103)) as source
		on source.[uid] = target.[uid]
when matched then
	update set t = case
						when source.commandType = 1 then @td
						when source.commandType in (2, 12) then @max
						when source.commandType in (3, 103) then @y
					end;

merge dbo.demo as target
	using (select * from #t where commandType in (100, 103)) as source
		on 1 = 0
when not matched then
	insert (id, f, t, p)
	values (source.id, @td, @max, source.p);

merge dbo.demo as target
	using (select * from #t where commandType in (7, 1007)) as source
		on source.uid_y = target.[uid]
when matched then
	update set t = @max;

merge dbo.demo as target
	using (select * from #t where commandType in (1007)) as source
		on source.[uid] = target.[uid]
when matched then
	delete;

commit tran

--select * from dbo.demo order by id, f

--update d set
--	p = t.p
--from dbo.demo d
--	join #t t on t.[uid] = d.[uid]
--		and commandType in (10, 12);

--update d set
--	t = case
--			when commandType = 1 then @td
--			when commandType in (2, 12) then @max
--			when commandType in (3, 103) then @y
--		end
--from dbo.demo d
--	join #t s on s.[uid] = d.[uid]
--		and s.commandType in (1, 2, 12, 3, 103);

--insert dbo.demo(id, f, t, p)
--select id, @td, @max, p from #t where commandType in (100, 103);

--update d set
--	t = @max
--from dbo.demo d
--	join #t t on t.uid_y = d.[uid]
--		and commandType in (7, 1007);

--delete dbo.demo
--from #t t
--where t.commandType = 1007
--	and demo.[uid] = t.[uid];

