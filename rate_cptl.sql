if object_id('tempdb..#d') <> 0 drop table #d;

create table #d(
	id_date int not null
	,[date] datetime not null
	,id_year smallint not null
	,days_in_year smallint not null
	,primary key(id_date));

;with d([date]) as (
	select cast('20180101' as datetime) [date]
		union all
	select [date] + 1 from d where [date] < '20300101')
insert #d
select
	convert(varchar(8), [date], 112)
	,[date]
	,convert(varchar(4), [date], 112)
	,datediff(day, dateadd(year, datediff(year, 0, [date]), 0), dateadd(year, datediff(year, 0, [date]) + 1, 0))
from d
option (maxrecursion 32000);

if object_id('tempdb..#s') <> 0 drop table #s;

create table #s(
	id int not null identity(1, 1)
	,date_from datetime not null
	,date_to datetime not null
	,rate float not null
	,primary key(id));

insert #s(date_from, date_to, rate)
select '20180112', '20180617', 8.5 union all
select '20180618', '20190101', 8.5 union all
select '20190102', '20220323', 8.5 union all
select '20220324', '20230124', 8.5;

declare @date_from datetime = '20180112';
declare @date_to datetime = '20230124';

declare @capitalization bit = 0;
declare @period tinyint = 3; -- 1, 3, 6
declare @period_from datetime = '20180131';

if object_id('tempdb..#t') <> 0 drop table #t;

create table #t(
	id int not null identity(1, 1)
	,id_date int not null
	,[date] datetime not null
	,rate float not null
	,days_in_year smallint not null
	,id_year smallint not null
	,id_period smallint not null
	,primary key(id));

insert #t
select
	d.id_date
	,d.[date]
	,s.rate / 100. rate
	,d.days_in_year
	,d.id_year
	,case
			when @capitalization = 0 then
				-1
			else
				case
					when d.[date] < dateadd(month, floor(datediff(month, @period_from, d.[date]) / @period) * @period, @period_from) then
						-1
					else
						0
				end + floor(datediff(month, @period_from, d.[date]) / @period)
		end id_period
from #s s
	join #d d on d.[date] between s.date_from and s.date_to
		and d.[date] between @date_from + 1 and @date_to;

if object_id('tempdb..#ty') <> 0 drop table #ty;

create table #ty(
	id_year smallint not null primary key
	,real_days_in_year smallint not null
	,days_in_year smallint not null
	,numberOfYears tinyint not null);

insert #ty
select id_year, real_days_in_year, days_in_year, s2.numberOfYears
from (select
			id_year
			,count(1) real_days_in_year
			,max(days_in_year) days_in_year
		from #t
		group by id_year)s1
	cross join (select count(distinct id_year) numberOfYears from #t)s2;

declare @id_period smallint = -1;

if object_id('tempdb..#tr') <> 0 drop table #tr;

create table #tr(
	id_year smallint primary key
	,rate float not null);

while 1 = 1
begin
	merge #tr as target
		using (select
					id_year
					,sum(rate / days_in_year) + 1. rate
				from #t
				where id_period = @id_period
				group by id_year) as source
			on source.id_year = target.id_year
	when matched then
		update set rate = target.rate * source.rate
	when not matched then
		insert (id_year, rate)
		values (source.id_year, source.rate);

	if @@ROWCOUNT = 0 break;

	set @id_period = @id_period + 1;

end

select
	sum((r.rate - 1.) * y.days_in_year / y.real_days_in_year) / max(numberOfYears) * 100. rate
from #tr r
	join #ty y on y.id_year = r.id_year;
