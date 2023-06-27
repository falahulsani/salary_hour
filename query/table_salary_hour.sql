drop table if exists salary_hour;

create table if not exists salary_hour(
	year int null,
	month int null,
	branch_id varchar(8) null,
    salary_per_hour float null
);

WITH tbl_employees AS (
	select 
		employee_id,
        branch_id,
        salary,
        join_date,
        resign_date
	from 
	( select 
        *,
        ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY join_date DESC) AS rn
     from employees ) e
    where e.rn = 1
),
tbl_timesheets AS(
	select 
		timesheet_id,
		employee_id,
		date,
		checkin,
		checkout
	from 
	( select 
        *,
        ROW_NUMBER() OVER (PARTITION BY timesheets ORDER BY date DESC) AS rn
     from timesheets )t
    where t.rn = 1
),
monthly_hours AS (
    select
        e.branch_id,
        EXTRACT(YEAR FROM t.date) AS year,
        EXTRACT(MONTH FROM t.date) AS month,
        COUNT(*) AS total_hours,
        COUNT(DISTINCT t.employee_id) AS total_employees
    from
        tbl_timesheets t
    left join
        tbl_employees e ON t.employee_id = e.employee_id
    group by 
        1,2,3
),
monthly_salary AS (
    select
        EXTRACT(YEAR FROM e.join_date) AS year,
        EXTRACT(MONTH FROM e.join_date) AS month,
		e.branch_id,
        SUM(e.salary) AS total_salary
    from
        tbl_employees e
    where
        e.resign_date IS NULL OR (EXTRACT(YEAR FROM e.resign_date) > EXTRACT(YEAR FROM e.join_date))
    group by 
        1,2,3
)

insert into salary_hour (
	year
    , month
	, branch_id
	, salary_per_hour
	)
select
    mh.year,
    mh.month,
    mh.branch_id,
    ms.total_salary / mh.total_hours AS salary_per_hour
from
    monthly_hours mh
inner join
    monthly_salary ms ON mh.branch_id = ms.branch_id AND mh.year = ms.year AND mh.month = ms.month
order by
    1,2,3;
	