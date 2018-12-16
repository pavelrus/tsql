if object_id('tempdb..#rem') <> 0 drop table #rem;
create table #rem(
  branch varchar(8) not null,
  account_id varchar(25) not null,
  rub_sum_in float not null,
  rub_sum float not null,
  date_from date not null,
  date_to date not null,
  primary key (branch, account_id, date_from));

insert #rem
select 'ASD', '100010001', 0, 100, '20180101', '20180111' union all
select 'ASD', '100010001', 100, 10, '20180112', '20180121' union all
select 'ASD', '100010001', 10, 18, '20180122', '20180124' union all
select 'ASD', '100010001', 18, 44, '20180125', '22000101';

if object_id('tempdb..#entries') <> 0 drop table #entries;
create table #entries(
  branch varchar(8) not null,
  account_id varchar(25) not null,
  value_date date not null,
  rub_sum float not null,
  is_del bit not null,
  is_upd bit not null);

insert #entries
select 'ASD', '100010001', '20180101', 100, 0, 0 union all
select 'ASD', '100010001', '20180112', -90, 0, 0 union all
select 'ASD', '100010001', '20180122', 8, 0, 0 union all
select 'ASD', '100010001', '20180125', 26, 0, 0 union all
select 'ASD', '100010001', '20180120', 100, 0, 1 union all
select 'ASD', '100010001', '20180127', 1, 0, 1 union all
select 'ASD', '100010001', '20180128', -1, 0, 1 union all
select 'ASD', '100010002', '20180128', 33, 0, 1 union all
select 'ASD', '100010002', '20180129', 37, 0, 1;

--
set xact_abort on;

if object_id('tempdb..#entries_updated') <> 0 drop table #entries_updated;
create table #entries_updated(
  branch varchar(8) not null,
  account_id varchar(25) not null,
  value_date date not null,
  primary key (account_id, branch));

if object_id('tempdb..#entries_updated_buf') <> 0 drop table #entries_updated_buf;
create table #entries_updated_buf(
  branch varchar(8) not null,
  account_id varchar(25) not null,
  value_date date not null,
  primary key (account_id, branch));

if object_id('tempdb..#rem_in') <> 0 drop table #rem_in;
create table #rem_in(
  branch varchar(8) not null,
  account_id varchar(25) not null,
  rub_sum float not null,
  date_from date not null,
  date_to date not null,
  rn smallint not null,
  is_del bit not null,
  date_to_previous date,
  primary key (account_id, branch, date_from));

if object_id('tempdb..#entries_rem') <> 0 drop table #entries_rem;
create table #entries_rem(
  branch varchar(8) not null,
  account_id varchar(25) not null,
  value_date date not null,
  rub_sum float not null,
  primary key (account_id, branch, value_date));

if object_id('tempdb..#rem_final') <> 0 drop table #rem_final;
create table #rem_final(
  branch varchar(8) not null,
  account_id varchar(25) not null,
  rub_sum_in float not null,
  rub_sum float not null,
  date_from date not null,
  date_to date not null,
  rn smallint not null,
  primary key (account_id, branch, date_from, rn));

 -- 
insert #entries_updated
select branch, account_id, min(value_date) from #entries where value_date > '20180101' and is_upd = 1 group by branch, account_id;

while 1 = 1
begin
	if not exists(select 1 from #entries_updated) break;

	truncate table #entries_updated_buf;

	insert #entries_updated_buf
	select top 10 branch, account_id, value_date from #entries_updated;

	truncate table #rem_in;

	insert #rem_in
	select
		branch,
		account_id,
		rub_sum,
		date_from,
		date_to,
		rn,
		is_del,
		dateadd(day, iif(rn = 1, -1, 0), date_from) date_to_previous
	from (select
			  r.branch,
			  r.account_id,
			  iif(r.date_from = t.value_date, r.rub_sum_in, r.rub_sum) rub_sum,
			  r.date_from,
			  r.date_to,
			  row_number() over (partition by r.branch, r.account_id order by r.date_from) rn,
			  iif(r.date_from = t.value_date, 1, 0) is_del
			from #rem r
			  join #entries_updated_buf t on t.account_id = r.account_id
				and t.branch = r.branch
			where r.date_to >= t.value_date)t;

	insert #rem_in
	select
		r.branch,
		r.account_id,
		r.rub_sum,
		r.date_from,
		r.date_to,
		0 rn,
		0 is_del,
		null date_to_previous
	from #rem r
		join #rem_in ri on ri.account_id = r.account_id
			and ri.branch = r.branch
			and ri.date_to_previous = r.date_to
			and ri.rn = 1
			and ri.is_del = 1;
	
	truncate table #entries_rem;

	insert #entries_rem
	select
	  t.branch,
	  t.account_id,
	  t.value_date,
	  t.rub_sum + isnull(r.rub_sum, 0) rub_sum
	from (select
			branch,
			account_id,
			value_date,
			sum(t.rub_sum) over (partition by t.branch, t.account_id order by t.value_date rows between unbounded preceding and current row) rub_sum
		  from (select
				  e.branch,
				  e.account_id,
				  e.value_date,
				  sum(e.rub_sum) rub_sum
				from #entries e
				  join #entries_updated_buf t on t.account_id = e.account_id
					and t.branch = e.branch
				where e.value_date >= t.value_date
				  and e.is_del = 0
				group by
				  e.branch,
				  e.account_id,
				  e.value_date)t)t
	  left join #rem_in r on r.account_id = t.account_id
		and r.branch = t.branch
		and r.rn = 1;

	truncate table #rem_final;

	insert #rem_final
	select
	  branch,
	  account_id,
	  lag(rub_sum, 1, 0) over (partition by branch, account_id order by value_date) rub_sum_in,
	  rub_sum rub_sum,
	  value_date date_from,
	  dateadd(day, -1, lead(value_date, 1, '22000102') over (partition by branch, account_id order by value_date)) date_to,
	  rn
	from (select branch, account_id, date_from value_date, rub_sum, rn from #rem_in where rn = 1
		  union all
		  select branch, account_id, value_date, rub_sum, -1 from #entries_rem)t;
	
	insert #rem_final
	select
		ri.branch,
		ri.account_id,
		0 rub_sum_in,
		0 rub_sum,
		ri.date_from,
		rf.date_to,
		ri.rn
	from #rem_in ri
		join #rem_final rf on rf.account_id = ri.account_id
			and rf.branch = ri.branch
			and rf.rn = 1
	where ri.rn = 0;

	--select * from #rem_final
	--select * from #rem_in
	
	begin tran

	delete #rem
	from #rem_in ri 
	where ri.account_id = #rem.account_id
	  and ri.branch = #rem.branch
	  and ri.date_from = #rem.date_from
	  and (ri.rn > 1 or ri.is_del = 1);

	update r set
	  r.date_to = rf.date_to
	from #rem r
	  join #rem_final rf on rf.account_id = r.account_id
		and rf.branch = r.branch
		and rf.date_from = r.date_from
		and rf.rn in (0, 1);

	insert #rem
	select
	  branch,
	  account_id,
	  rub_sum_in,
	  rub_sum,
	  date_from,
	  date_to
	from #rem_final
	where rn = -1;

	commit tran

	delete #entries_updated
	from #entries_updated_buf b
	where b.branch = #entries_updated.branch
		and b.account_id = #entries_updated.account_id;

end

select * from #rem
