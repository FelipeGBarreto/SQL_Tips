# Parameters
payment_type = 'Saldo' #'Parcelado', 'À Vista', 'Total'
kpi = 'QTD' #TPV
table_product = '<table_name>'

# Query
spark.sql(f"""
 
with
base_product as (
select *, (case when '{payment_type}' = 'Saldo' then 'Saldo' else payment_distribution end) payment_adjusted 
from (
select 
	date(created_at) as data
	,concat(
		date(created_at), ' (',
		(
			case 
				when b.weekday_name = 'sunday' then 'Domingo'
				when b.weekday_name = 'monday' then 'Segunda'
				when b.weekday_name = 'tuesday' then 'Terça'
				when b.weekday_name = 'wednesday' then 'Quarta'
				when b.weekday_name = 'thursday' then 'Quinta'
				when b.weekday_name = 'friday' then 'Sexta'
				when b.weekday_name = 'saturday' then 'Sábado'
			end
		)
		,')'
		) as data_dia_semana
	,(case 
  	when dateadd(
       		day, -- tirar esse delta de dias da data máxia, para comparar as horas com a created_at
		  	  -datediff( -- diferença de dia para a data mais atual e a created_at
		  		  (select max(created_at) from {table_product}),
		  		  created_at
		  	  ), 
		  	 (select max(created_at) from {table_product})
    	) >= created_at then true
  	else false end 
  	) as class_realizado
  ,case 
  	when '{payment_type}' = 'Total' then 'Total'
  	else (
	  	case 
	    when payment_distribution in ('credit', 'mixed') and installments > 1 then 'Parcelado'
	    when payment_distribution in ('credit', 'mixed') and installments = 1 then 'À Vista'
	    else 'Saldo' end
  	) end as payment_type
  ,payment_distribution
  ,(case 
   		when '{payment_type}' = 'Saldo' then balance_value
   		when '{payment_type}' = 'Parcelado' then credit_card_value
   		when '{payment_type}' = 'À Vista' then credit_card_value
   		when '{payment_type}' = 'Total' then total_value
   	end
   	) as tpv
from {table_product} as a
left join shared.calendar as b 
on date(a.created_at) = b.calendar_date
where 1=1
	and is_approved = true
	and product_type_name = 'PF'
  and date(created_at) >= dateadd(current_date(), -7)
) t
where (
	case 
		when '{payment_type}' IN ('Parcelado', 'À Vista', 'Total') then payment_type = '{payment_type}'
		when '{payment_type}' = 'Saldo' then payment_distribution in ('balance','mixed')
	end 
)
)


select '{kpi}' as KPI,
	data
	,class_realizado as Class
	,data_dia_semana
	,sum(
	  	case 
		  	when '{kpi}' = 'QTD' then 1 -- QTD
		  	when '{kpi}' = 'TPV' then tpv -- TPV
		  	end
	  ) as total
from base_product
group by 1,2,3,4
order by data



""").display()
