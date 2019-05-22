drop table if exists #data_base_raw;
create table #data_base_raw(
  id_agreement int not null,
  date_from date not null,
  date_to date not null,
  rate float not null
);
create clustered index id_agreement_idx on #data_base_raw(id_agreement);

drop table if exists #data_merged_raw;
create table #data_merged_raw(
  id_agreement int not null,
  date_from date not null,
  date_to date not null,
  rate float not null,
  [priority] tinyint not null
);
create clustered index priority_id_agreement_idx on #data_merged_raw([priority], id_agreement);

drop table if exists #data_base;
create table #data_base(
  id_agreement int not null,
  date_from date not null,
  date_to date not null,
  rate float not null,
  primary key (id_agreement, date_from)
);

drop table if exists #data_merged;
create table #data_merged(
  id_agreement int not null,
  date_from date not null,
  date_to date not null,
  rate float not null,
  [priority] tinyint not null,
  primary key ([priority], id_agreement, date_from)
);

drop table if exists #void_periods;
create table #void_periods(
  id_agreement int not null,
  date_from date not null,
  date_to date not null
);

--
insert #data_base_raw
select 81140230, '20180101', '20180501', 5 union all
select 81140230, '20180601', '20180701', 6 union all
select 81140230, '20180601', '20180701', 6 union all
select 81140230, '20180901', '20181001', 7;

insert #data_merged_raw
select 81140230, '20180301', '20180801', -16, 1;

insert #data_merged_raw
select 81140230, '20180201', '20180401', 27, 2 union all
select 81140230, '20180701', '20181101', 28, 2;
--

insert #data_base
select
  id_agreement,
  date_from,
  iif(date_to > date_from_next, date_from_next, date_to) date_to,
  rate
from (select
        id_agreement,
        date_from,
        date_to,
        rate,
        lead(dateadd(day, -1, date_from), 1, date_to) over (partition by id_agreement order by date_from, date_to) date_from_next
      from (select
              id_agreement,
              iif(date_from < '19000101', '19000101', date_from) date_from,
              iif(date_to > '22000101', '22000101', date_to) date_to,
              rate
            from #data_base_raw)t
      where date_from <= date_to)t
where date_from <= date_from_next;

insert #data_merged
select
  id_agreement,
  date_from,
  iif(date_to > date_from_next, date_from_next, date_to) date_to,
  rate,
  [priority]
from (select
        id_agreement,
        date_from,
        date_to,
        rate,
        [priority],
        lead(dateadd(day, -1, date_from), 1, date_to) over (partition by [priority], id_agreement order by date_from, date_to) date_from_next
      from (select
              id_agreement,
              iif(date_from < '19000101', '19000101', date_from) date_from,
              iif(date_to > '22000101', '22000101', date_to) date_to,
              rate,
              [priority]
            from #data_merged_raw)t
      where date_from <= date_to)t
where date_from <= date_from_next;

declare @priority tinyint;

declare record cursor for
select distinct [priority] from #data_merged order by 1;

open record

while 1 = 1
begin
  fetch record into @priority;
  if @@fetch_status <> 0 break;

  truncate table #void_periods;

  insert #void_periods
  select
    id_agreement,
    date_from_void,
    date_to_viod
  from (select
          id_agreement,
          dateadd(day, 1, date_to) date_from_void,
          lead(dateadd(day, -1, date_from), 1, date_to) over (partition by id_agreement order by date_from) date_to_viod
        from #data_base)t
  where date_from_void <= date_to_viod
   union all
  select
    id_agreement,
    date_from,
    date_to
  from (select
          id_agreement,
          iif(d.[type] = -1, d.[date], t.date_to_max) date_from,
          iif(d.[type] = -1, t.date_from_min, d.[date]) date_to
        from (select
                id_agreement,
                dateadd(day, -1, min(date_from)) date_from_min,
                dateadd(day, 1, max(date_to)) date_to_max
              from #data_base
              group by id_agreement)t
          cross join (select convert(date, '19000101') [date], -1 [type] union all select '22000101', 1)d)t
  where date_from <= date_to
   union all
  select distinct
    id_agreement,
    '19000101',
    '22000101'
  from #data_merged
  where [priority] = @priority
    and not id_agreement in (select id_agreement from #data_base);

  insert #data_base
  select
    vp.id_agreement,
    iif(d.date_from < vp.date_from, vp.date_from, d.date_from) date_from,
    iif(d.date_to > vp.date_to, vp.date_to, d.date_to) date_to,
    d.rate
  from #void_periods vp
    join (select id_agreement, date_from, date_to, rate from #data_merged where [priority] = @priority) d
      on d.id_agreement = vp.id_agreement
        and vp.date_to >= d.date_from
        and vp.date_from <= d.date_to;

end

close record
deallocate record

select * from #data_base order by 1, 2
