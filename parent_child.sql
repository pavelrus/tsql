--create table parent_child(
--	id_client_from int not null
--	,id_client_to int not null
--	,date_from date not null
--	,primary key(id_client_from, id_client_to));

--create table parent_child_migration(
--	id_client_from int not null
--	,id_client_to int not null
--	,date_from date not null
--	,id_client int not null
--	,primary key(id_client_from, id_client_to));

/*
truncate table parent_child

insert parent_child
select 100, 101, '20180601' union all
select 101, 102, '20180602' union all
select 102, 103, '20180603' union all
select 103, 104, '20180604' union all
select 105, 106, '20180605' union all
select 106, 107, '20180606' union all
select 108, 109, '20180607' union all
select 110, 111, '20180608' union all
select 111, 112, '20180609' union all
select 112, 113, '20180610' union all
select 113, 114, '20180611';
*/

if object_id('tempdb..#dwhData') <> 0 drop table #dwhData;
create table #dwhData(id_client_from int not null, id_client_to int not null, date_from date not null, primary key(id_client_from, id_client_to));

if object_id('tempdb..#tmpData') <> 0 drop table #tmpData;
create table #tmpData(id_client_from int not null, id_client_to int not null, date_from date not null, primary key(id_client_from, id_client_to));

if object_id('tempdb..#tmpData_') <> 0 drop table #tmpData_;
create table #tmpData_(id_client_from int not null, id_client_to int not null, date_from date not null, primary key(id_client_from, id_client_to));

if object_id('tempdb..#bufData') <> 0 drop table #bufData;
create table #bufData(id_client_from int not null, id_client_to int not null, date_from date not null, primary key(id_client_from, id_client_to));

if object_id('tempdb..#mstData') <> 0 drop table #mstData;
create table #mstData(id_client_from int not null, id_client_to int not null,primary key(id_client_from, id_client_to));

insert #dwhData
select id_client_from, id_client_to, max(date_from)  from parent_child where id_client_from <> id_client_to group by id_client_from, id_client_to;

insert #tmpData
select * from #dwhData;

insert #bufData
select * from #dwhData;

insert #mstData
select id_client_from, id_client_to from #dwhData where not id_client_to in (select id_client_from from #dwhData);


while 1 = 1
begin
	truncate table #tmpData_;

	insert #tmpData_
	select
		d.id_client_from
		,t.id_client_to
		,case when t.date_from > d.date_from then t.date_from else d.date_from end
	from #tmpData t
		join #dwhData d on d.id_client_to = t.id_client_from;

	insert #mstData
	select
		t.id_client_from
		,t.id_client_to
	from #tmpData_ t
		join (select distinct id_client_to from #mstData)mm on mm.id_client_to = t.id_client_to
		left join #mstData m on m.id_client_from = t.id_client_from
			and m.id_client_to = t.id_client_to
	where m.id_client_from is null;

	if @@rowcount = 0 break;
	
	insert #bufData
	select * from #tmpData_;

	truncate table #tmpData;

	insert #tmpData
	select * from #tmpData_;

end

insert #bufData
select
	t.id_client_from
	,t.id_client_to
	,max(t.date_from)
from (select id_client_to id_client_from, id_client_to, date_from from #dwhData
			union all
		select id_client_from, id_client_from, '19000101' from #dwhData)t
group by t.id_client_from, t.id_client_to;

truncate table parent_child_migration

insert parent_child_migration
select
	b.id_client_from
	,b.id_client_to
	,b.date_from
	,isnull(m.id_client_to, b.id_client_to) client_id
from #bufData b
	left join #mstData m on m.id_client_from = b.id_client_from
order by
	b.id_client_to
	,b.id_client_from;
