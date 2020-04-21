设备上线数：
```sbtshell
select ifnull(z.`name`,'其他'),sum(num) as num from (select * from (select m.zc_name,m.parent_id as pd,count(*) as num from (select a.asset_id as a_id,c.parent_id,c.`name` as zc_name,b.`status` from asset_category_mapping a 
left join asset b on a.asset_id=b.id left join asset_category c on c.id = a.asset_category_id where b.`status`='0') m GROUP BY m.zc_name) x left join asset_category y on x.pd = y.id) z group by z.`name`

```
