declare @d table(id_date int not null primary key, [value] float not null);

insert @d
select 20180101, .11 union all
select 20180102, .12 union all
select 20180103, .12 union all
select 20180104, .12 union all
select 20180105, .12 union all
select 20180106, .13 union all
select 20180109, .13 union all
select 20180110, .11 union all
select 20180111, .11 union all
select 20180112, .11 union all
select 20180114, .14 union all
select 20180115, .11 union all
select 20180116, .15 union all
select 20180117, .11 union all
select 20180118, .11 union all
select 20180125, .17 union all
select 20180126, .18 union all
select 20180127, .18 union all
select 20180128, .19 union all
select 20180129, .19;

declare @t table(id_date int not null primary key, [value] float not null, comnadType tinyint not null, rn smallint not null);

insert @t
select
	id_date
	,[value]
	,comnadType
	,row_number() over (order by id_date) rn
from (
select
	id_date
	,[value]
	,value_previous
	,value_next
	,id_date_previous
	,id_date_next
	,case
			when id_date_next - id_date > 1 then
				case when [value] <> value_previous then 2 else 1 end
			when id_date - id_date_previous > 1 then
				case when [value] <> value_next then 2 else 1 end
			when value_previous <> [value] and value_next <> [value] then 2
			when value_previous = 0 or value_next = 0 or value_previous <> [value] or value_next <> [value] then 1
		end comnadType
from (
select
	id_date
	,[value]
	,lead([value], 1, 0) over (order by id_date desc) value_previous
	,lead([value], 1, 0) over (order by id_date) value_next
	,lead(id_date, 1, 0) over (order by id_date desc) id_date_previous
	,lead(id_date, 1, 0) over (order by id_date) id_date_next
from @d
)t
)tt
where not comnadType is null;

select
	t1.id_date id_date_from
	,t2.id_date id_date_to
	,t1.[value]
from @t t1
	join @t t2 on t2.rn = t1.rn + 1
		and t2.[value] = t1.[value]
where t1.comnadType = 1
union all
select
	id_date id_date_from
	,id_date id_date_to
	,[value]
from @t
where comnadType = 2
order by id_date_from;