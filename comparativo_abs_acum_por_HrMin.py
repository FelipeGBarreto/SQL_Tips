# Parâmeters
payment_type = 'Saldo' #'Parcelado', 'À Vista', 'Total'
date_breakdown = '15 em 15 minutos' #30 em 30 minutos, #1 em 1 hora
kpi = 'QTD' #QTD
table_product = '<table_name>'


# Query
spark.sql(f"""
 
with
base_product as (
select *, (case when '{payment_type}' = 'Saldo' then 'Saldo' else payment_type end) payment_adjusted 
from (
select created_at
  ,dateadd(DAY, -7, (select max(created_at) from {table_product})) data_maxima_semana
  ,date(created_at) as data
  ,replace(substring(date_trunc('hour', created_at ), 1, 16), ':00',
  case 
      when minute(created_at) <= 15 then ':15'
      when minute(created_at) <= 30 then ':30'
      when minute(created_at) <= 45 then ':45'
      else ':59' end 
    ) as data_hora_15
  ,case
		when '15 em 15 minutos' = '{date_breakdown}' then (
			concat(
		   case 
		  	when hour(created_at) < 10 then concat('0', hour(created_at))
		  	else hour(created_at) end ,
			  	case 
			      when minute(created_at) <= 15 then ':15'
			      when minute(created_at) <= 30 then ':30'
			      when minute(created_at) <= 45 then ':45'
			      else ':59' end 
  	)
  	)
  	when '30 em 30 minutos' = '{date_breakdown}' then (
			concat(
		   case 
		  	when hour(created_at) < 10 then concat('0', hour(created_at))
		  	else hour(created_at) end ,
		  	case 
		      when minute(created_at) <= 30 then ':30'
		      else ':59' end 
	  	)
		)
		when '1 em 1 hora' = '{date_breakdown}' then (
		  case 
		  	when hour(created_at) <10 then concat(' 0',hour(created_at),'h')
		  	else concat(' ',hour(created_at),'h') end
		)
		else date(created_at)
		end as hora_minuto
  ,case 
  	when '{payment_type}' = 'Total' then 'Total'
  	else (
	  	case 
	    when payment_distribution in ('credit','mixed') and installments > 1 then 'Parcelado'
	    when payment_distribution in ('credit','mixed') and installments = 1 then 'À Vista'
	    else 'Saldo' end
  	) end as payment_type
  ,payment_distribution
  ,(case 
   		when '{payment_type}' = 'Saldo' then paid_balance_value
   		when '{payment_type}' = 'Parcelado' then paid_credit_card_value
   		when '{payment_type}' = 'À Vista' then paid_credit_card_value
   		when '{payment_type}' = 'Total' then paid_total_value
   	end
   	) as tpv  --paid_total_value_with_revenue
from {table_product}
where 1=1
	and is_approved = true) t
where (
	case 
		when '{payment_type}' IN ('Parcelado', 'À Vista', 'Total') then payment_type = '{payment_type}'
		when '{payment_type}' = 'Saldo' then payment_distribution in ('balance','mixed')
	end 
)
)

---.---.---.---.---.---.---.---.---.---
-- HOJE
---.---.---.---.---.---.---.---.---.---

,base_hoje as (
-- TOTAL
select 'Hoje' as Period,*
  ,sum(total) over(partition by data order by data_hora_15) as total_acum
from (
	select payment_adjusted
	  ,data_hora_15
	  ,hora_minuto
	  ,data
	  ,sum(
	  	case 
		  	when '{kpi}' = 'QTD' then 1 -- QTD
		  	when '{kpi}' = 'TPV' then tpv -- TPV
		  	end
	  ) as total
	from base_product
	where date(created_at) = current_date() --HOJE
	group by 1,2,3,4
	order by 1,2,3,4
) as t
)


---.---.---.---.---.---.---.---.---.---
-- SEMANA
---.---.---.---.---.---.---.---.---.---
,base_ontem as (
-- TOTAL
select 'Hoje - 7d' as Period, *
	,sum(total) over(partition by data order by data_hora_15) as total_acum
from (
	select payment_adjusted
	  ,data_hora_15
	  ,hora_minuto
	  ,data
	  ,sum(
	  	case 
		  	when '{kpi}' = 'QTD' then 1 -- QTD
		  	when '{kpi}' = 'TPV' then tpv -- TPV
		  	end
	  ) as total
	from base_product
	where date(created_at) = dateadd(current_date(), -7) --"SEMANA"
		and created_at <= data_maxima_semana -- limitação para a data máxima 
	group by 1,2,3,4
	order by 1,2,3,4
) as t
)

,result as (
select * from base_hoje
union
select * from base_ontem
)
select * from result
order by data_hora_15

""").display()
