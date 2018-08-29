declare @source table(
	id int identity(1, 1) primary key
	,amount float not null
	,amount2 float not null);

insert @source
select 10, 1 union all
select 20, 2 union all
select 30, 3 union all
select 40, 4;

declare @target table(
	id int identity(1, 1) primary key
	,amount float not null);

insert @target
select 10 union all
select 40;

select sum(amount), sum(amount2) from (
select
	s.amount * t.amount * (1. / st.amount - 1. / tt.amount) amount
	,s.amount2 * t.amount * (1. / st.amount - 1. / tt.amount)  amount2
from (@source s
		cross join (select sum(amount) amount from @source)st)
	cross join (@target t
				cross join (select sum(amount) amount from @target)tt)
)t