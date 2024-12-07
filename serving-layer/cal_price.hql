create table price_qileichen (
  name_merchant string,
  avg_price float,
  min_price float,
  max_price float
);

insert overwrite table price_qileichen
    select concat(name, prices_merchant) as name_merchant,
    avg((prices_amountMin + prices_amountMax) / 2) AS avg_price,
    min(prices_amountMin) AS min_price,
    max(prices_amountMin) AS max_price
    from price_history_qileichen
    where prices_amountMin IS NOT NULL AND prices_amountMax IS NOT NULL
    group by prices_merchant, id, name;


select * from price_qileichen limit 5;
