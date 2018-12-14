drop table if exists #rem;
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

drop table if exists #entries;
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
select 'ASD', '100010001', '20180124', 100, 0, 1 union all
select 'ASD', '100010001', '20180127', 1, 0, 1 union all
select 'ASD', '100010001', '20180128', -1, 0, 1 union all
select 'ASD', '100010002', '20180128', 33, 0, 1 union all
select 'ASD', '100010002', '20180129', 37, 0, 1;

-- 
set xact_abort on;

drop table if exists #entries_updated;
create table #entries_updated(
  branch varchar(8) not null,
  account_id varchar(25) not null,
  value_date date not null,
  primary key (account_id, branch));

drop table if exists #entries_updated_buf;
create table #entries_updated_buf(
  branch varchar(8) not null,
  account_id varchar(25) not null,
  value_date date not null,
  primary key (account_id, branch));

drop table if exists #rem_in;
create table #rem_in(
  branch varchar(8) not null,
  account_id varchar(25) not null,
  rub_sum_out float not null,
  date_from date not null,
  rn smallint not null,
  primary key (account_id, branch, date_from));

drop table if exists #entries_cumulative_totals
create table #entries_cumulative_totals(
  branch varchar(8) not null,
  account_id varchar(25) not null,
  value_date date not null,
  rub_sum_out float not null,
  primary key (account_id, branch, value_date));

drop table if exists #rem_final;
create table #rem_final(
  branch varchar(8) not null,
  account_id varchar(25) not null,
  rub_sum_in float not null,
  rub_sum float not null,
  date_from date not null,
  date_to date not null,
  rn smallint not null,
  primary key (account_id, branch, date_from));

drop table if exists #rem_previous;
create table #rem_previous(
  branch varchar(8) not null,
  account_id varchar(25) not null,
  date_from date not null,
  date_to date not null,
  is_upd bit not null default 0
  primary key (account_id, branch));

 -- 
insert #entries_updated
select branch, account_id, min(value_date) from #entries where is_upd = 1 group by branch, account_id;

-- while 1 = 1 begin
truncate table #entries_updated_buf;

insert #entries_updated_buf
select top 3000 branch, account_id, value_date from #entries_updated;

truncate table #rem_in;

insert #rem_in
select
  r.branch,
  r.account_id,
  case when r.date_from = t.value_date then r.rub_sum_in else r.rub_sum end rub_sum_out,
  dateadd(day, iif(r.date_from = t.value_date, -1, 0), r.date_from) date_from,
  row_number() over (partition by r.branch, r.account_id order by r.date_from) rn
from #rem r
  join #entries_updated_buf t on t.account_id = r.account_id
    and t.branch = r.branch
where r.date_to >= t.value_date;

truncate table #entries_cumulative_totals;

insert #entries_cumulative_totals
select
  t.branch,
  t.account_id,
  t.value_date,
  t.rub_sum_on_date + isnull(r.rub_sum_out, 0) rub_sum_out
from (select
        branch,
        account_id,
        value_date,
        sum(t.rub_sum) over (partition by t.branch, t.account_id order by t.value_date rows between unbounded preceding and current row) rub_sum_on_date
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
  left join #rem_in r on r.rn = 1
    and r.account_id = t.account_id
    and r.branch = t.branch;

truncate table #rem_final;

insert #rem_final
select
  branch,
  account_id,
  lag(rub_sum_out, 1, 0) over (partition by branch, account_id order by value_date) rub_sum_in,
  rub_sum_out rub_sum,
  value_date date_from,
  dateadd(day, -1, lead(value_date, 1, '22000102') over (partition by branch, account_id order by value_date)) date_to,
  rn
from (select branch, account_id, date_from value_date, rub_sum_out, rn from #rem_in where rn = 1
      union all
      select branch, account_id, value_date, rub_sum_out, -1 from #entries_cumulative_totals)t;

truncate table #rem_previous;

insert #rem_previous(branch, account_id, date_from, date_to)
select
  branch,
  account_id,
  date_from,
  date_to
from (select
        r.branch,
        r.account_id,
        r.date_from,
        r.date_to,
        row_number() over (partition by r.branch, r.account_id order by r.date_from desc) rn
      from #rem r
        join #entries_updated_buf t on t.account_id = r.account_id
          and t.branch = r.branch
      where r.date_to < t.value_date)t
where rn = 1;

update rp set
  date_to = rf.date_to,
  is_upd = 1
from #rem_previous rp
  join (select
          branch,
          account_id,
          dateadd(day, -1, min(date_from)) date_to
        from #rem_final
        where rn = -1
        group by branch, account_id)rf
    on rf.account_id = rp.account_id
      and rf.branch = rp.branch
where rp.date_to <> rf.date_to;

begin tran

delete #rem
from #entries_updated_buf t 
where t.account_id = #rem.account_id
  and t.branch = #rem.branch
  and t.value_date <= #rem.date_to;

update r set
  r.date_to = rf.date_to
from #rem r
  join #rem_final rf on rf.rn = 1
    and rf.account_id = r.account_id
    and rf.branch = r.branch
    and rf.date_from = r.date_from;

update r set
  r.date_to = rp.date_to
from #rem r
  join #rem_previous rp on rp.is_upd = 1
    and rp.account_id = r.account_id
    and rp.branch = r.branch
    and rp.date_from = r.date_from;

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

select * from #rem

--select
--  e.branch,
--  e.account_id,
--  e.value_date,

--from #entries_cumulative_totals e
--  join #rem_in r on r.account_id = e.account_id
--    and r.branch = e.branch

--select
--  isnull(r.branch, t.branch) branch,
--  isnull(r.account_id, t.account_id) account_id,
--  isnull(r.date_from, t.value_date) date_from,
--  r.rub_sum_in,
--  t.rub_sum turnover
--from (select * from #rem_in where rn = 1) r
--  full join #entries_cumulative_totals t on t.account_id = r.account_id
--    and t.branch = r.branch
--    and t.value_date = r.date_from

--select * from #turn_updated

-- while 1 = 1 end


--SELECT actid, tranid, val,
--  SUM(val) OVER(PARTITION BY actid
--                ORDER BY tranid
--                ROWS BETWEEN UNBOUNDED PRECEDING
--                         AND CURRENT ROW) AS balance
--FROM dbo.Transactions;