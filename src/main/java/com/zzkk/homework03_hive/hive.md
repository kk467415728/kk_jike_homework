### 1 展示电影ID为2116这部电影各年龄阶段的平均影评分
~~~
select u.age,avg(r.rate) avgrate
from t_user as u inner join t_rating as r
on u.userid = r.userid
where r.movieid = 2116
group by u.age;
~~~


第一题截图![img](/images/03-hive/1.png)
### 2 找出男性评分最高且评分次数超过50次的10部电影男性评分最高的十部：
~~~
select u.sex as sex,m.moviename as name, avg(r.rate) as average,count(*) total
from t_rating as r
left join t_movie as m on r.movieid= m.movieid
left join t_user as u on r.userid = u.userid
where u.sex = 'M'
group by u.sex,m.moviename
having total > 50
order by average desc
limit 10;
~~~
第二题截图![img](/images/03-hive/2.png)

