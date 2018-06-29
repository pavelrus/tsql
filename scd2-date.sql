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

if object_id('tempdb..#s') <> 0 drop table #s;

create table #s(
	id int not null
	,p int not null
	,primary key(id));

insert #s
select 101, 4001 union all
select 102, 4001 union all
select 103, 4003 union all
--select 104, 4001 union all
select 105, 4004 union all
select 106, 4001;

declare @td date = '20180624' --getdate();
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
				when d.id is null and dy.id is null and not s.id is null then 100 -- (9), ins (@td, @max, s.p)
				when d.id is null then
					case
						when dy.t = @y then
							case
								when dy.p <> s.p then 100 -- (7) ins (@td, @max, s.p)
								when dy.p = s.p then 7 -- (8) upd dy.t = @max
							end
					end
				--when d.f <= @td and d.t = @td then
				--	case
				--		when d.p <> s.p then 12 -- (10) upd d.p = s.p, d.t = @max
				--		when d.p = s.p then 2 -- (11) upd d.t = @max
				--	end
				when d.f = @td then
					case
						when d.t > @td then
							case
								when s.id is null then 1 -- (6) upd d.t = @td
								when dy.p = s.p then 1007 -- (15) del d.uid upd dy.t = @max
								when d.p <> s.p then 10 -- (3) upd d.p = s.p
							end
						when d.t = @td then
							case
								when d.p <> s.p then 12 -- (1) upd d.p = s.p, d.t = @max
								when d.p = s.p then 2 -- (2) upd d.t = @max
							end
					end
				when d.f < @td then
					case
						when d.t = @td then
							case
								when d.p <> s.p then 12 -- (10) upd d.p = s.p, d.t = @max
								when d.p = s.p then 2 -- (11) upd d.t = @max
							end
						when d.t > @td then
							case
								when s.id is null then 3 -- (14) upd d.t = @y
								when d.p <> s.p then 103 -- (12) upd d.t = @y, ins (@td, @max, s.p)
							end
					end
			end commandType
		from ((select [uid], id, f, t, p from dbo.demo where @td between f and t)d
				full join (select [uid], id, f, t, p from dbo.demo where @y between f and t)dy
					on dy.id = d.id)
			full join #s s on s.id = isnull(d.id, dy.id))t
where not t.commandType is null;

set xact_abort on;

begin tran

update d set
	p = t.p
from dbo.demo d
	join #t t on t.[uid] = d.[uid]
		and commandType in (10, 12);

update d set
	t = case
			when commandType = 1 then @td
			when commandType in (2, 12) then @max
			when commandType in (3, 103) then @y
		end
from dbo.demo d
	join #t s on s.[uid] = d.[uid]
		and s.commandType in (1, 2, 12, 3, 103);

insert dbo.demo(id, f, t, p)
select id, @td, @max, p from #t where commandType in (100, 103);

update d set
	t = @max
from dbo.demo d
	join #t t on t.uid_y = d.[uid]
		and commandType in (7, 1007);

delete dbo.demo
from #t t
where t.commandType = 1007
	and demo.[uid] = t.[uid];

commit tran

--select * from dbo.demo order by id, f

