set xact_abort on;

-- initial data
drop table if exists #dwh;
create table #dwh(
  id_client int not null,
  id_property int not null,
  date_from date not null,
  date_to date not null,
  primary key (id_client, date_from)
);

insert #dwh
select 1, 40, '2018-01-01', '2018-02-01' union all
select 1, 41, '2018-02-02', '2018-02-05' union all
select 1, 42, '2018-02-07', '2018-03-01' union all
select 1, 43, '2018-03-02', '2018-03-15' union all
select 1, 44, '2018-03-16', '2018-03-23' union all
select 1, 45, '2018-03-24', '2200-01-01' union all
select 2, 41, '2018-01-01', '2018-02-01' union all
select 2, 42, '2018-02-02', '2018-02-03' union all
select 2, 40, '2018-02-04', '2018-02-04' union all
select 3, 40, '2018-01-01', '2019-05-13' union all
select 5, 40, '2019-05-13', '2200-01-01' union all
select 6, 40, '2019-05-14', '2200-01-01' union all
select 7, 40, '2019-05-14', '2019-05-14' union all
select 8, 40, '2019-05-14', '2019-05-14' union all
select 9, 40, '2019-05-14', '2200-01-01' union all
select 10, 40, '2019-01-01', '2019-05-13' union all
select 10, 41, '2019-05-14', '2200-01-01';

drop table if exists #stage;
create table #stage(
  id_client int primary key,
  id_property int not null
);

insert #stage
select 1, 40 union all
select 2, 40 union all
select 3, 40 union all
select 4, 44 union all
select 7, 40 union all
select 8, 41 union all
select 9, 41 union all
select 10, 40;

-- parameters
declare @toDay date = getdate();
declare @yesterDay date = cast(@toDay as datetime) - 1;
declare @minDate date = '19000101';
declare @maxDate date = '22000101';

if exists (select 1 from #dwh where date_from > @toDay)
begin
  raiserror ('parameter @toDay is not set correctly ', 16, 1);
  
  return

end

-- new data
drop table if exists #newData;
create table #newData(
  id_client int primary key,
  id_property int not null,
  date_from date not null,
  date_to date not null
);

insert #newData
select
  id_client,
  id_property,
  iif(@minDate > @toDay, @toDay, @minDate),
  @maxDate
from #stage
where not id_client in (select id_client from #dwh);

-- stage data
drop table if exists #stageData;
create table #stageData(
  id_client int primary key,
  id_property int not null
);

insert #stageData
select
  id_client,
  id_property
from #stage
where not id_client in (select id_client from #newData);

-- dwh data (@toDay and @yesterDay)
drop table if exists #dwhData;
create table #dwhData(
  id_client int primary key,
  id_property int not null,
  date_from date not null,
  date_to date not null,
);

insert #dwhData
select
  id_client,
  id_property,
  date_from,
  date_to
from #dwh
where @toDay between date_from and date_to;

drop table if exists #dwhDataOnYesterday;
create table #dwhDataOnYesterday(
  id_client int primary key,
  id_property int not null,
  date_from date not null
);

insert #dwhDataOnYesterday
select
  id_client,
  id_property,
  date_from
from #dwh
where date_to = @yesterDay;

-- changed data
drop table if exists #changedData;
create table #changedData(
  id_client int primary key,
  id_property int,
  date_from date,
  date_from_yesterday date,
  command smallint not null
);

insert #changedData
select
	id_client,
	id_property,
	date_from,
	date_from_yesterday,
	command
from (select
			coalesce(d.id_client, dy.id_client, s.id_client) id_client,
			s.id_property,
			d.date_from,
			dy.date_from date_from_yesterday,
			case
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
								when dy.id_property = s.id_property then 7 -- upd dy.t = @maxDate
								when dy.id_property <> s.id_property or dy.id_client is null then 100 -- ins (@toDay, @maxDate, s.p)
							end
						when dy.id_property = s.id_property then 1007 -- del d upd dy.t = @maxDate
						when d.id_property = s.id_property and d.date_to = @toDay then 2 -- upd d.t = @maxDate
						when d.id_property <> s.id_property then
							case
								when d.date_from = @toDay and d.date_to = @toDay then 12 -- upd d.p = s.p, d.t = @maxDate
								when d.date_from = @toDay and d.date_to > @toDay then 10 -- upd d.p = s.p
								when d.date_from < @toDay and d.date_to > @toDay then 103 -- upd d.t = @yesterDay, ins (@toDay, @maxDate, s.p)
							end
					end
			end command
		from (#dwhData d
				full join #dwhDataOnYesterday dy
					on dy.id_client = d.id_client)
			full join #stageData s on
        s.id_client = isnull(d.id_client, dy.id_client))t
where not t.command is null;

if exists (select 1 from #newData) or exists (select 1 from #changedData)
begin
  begin tran

  delete d
  from #dwh d
    join #changedData t
      on t.id_client = d.id_client
        and t.date_from = d.date_from
        and t.command = 1007;

  update d set
    id_property = iif(t.command in (10, 12), t.id_property, d.id_property),
    date_to = case
                  when t.command = 1 then @toDay
                  when t.command in (2, 12) then @maxDate
                  when t.command in (3, 103) then @yesterDay
                  else d.date_to
                end
  from #dwh d
    join #changedData t on
      t.id_client = d.id_client
      and t.date_from = d.date_from
      and t.command in (1, 2, 10, 12, 3, 103);

  update d set
    date_to = @maxDate
  from #dwh d
    join #changedData t on
      t.id_client = d.id_client
      and t.date_from_yesterday = d.date_from
      and t.command in (7, 1007);

  insert #dwh(id_client, id_property, date_from, date_to)
  select
    id_client,
    id_property,
    date_from,
    date_to
  from #newData
   union all
  select
    id_client,
    id_property,
    @toDay,
    @maxDate
  from #changedData
  where command in (100, 103);

  commit tran

end
