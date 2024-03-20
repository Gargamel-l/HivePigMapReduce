# HivePigMapReduce

− Разработать базу данных (не менее 3 сущностей). Разработать не менее 5 запросов к БД, из них не менее 3 с применением JOIN.

− Реализовать БД и запросы на Hive и Pig.

− Реализовать запросы с применением паттерна MapReduce.

− Продемонстрировать, что результаты выполнения запросов на Hive, Pig совпадают с результатами, полученными с помощью собственной реализации MapReduce.

− Организовать хранение данных так, чтобы входные файлы, расположенные в HDFS, были общими для Hive, Pig, MapReduce.

#Результаты работы запросов Hive:

1. Получить список всех заказов клиента по его customer_id.
   
![image](https://github.com/Gargamel-l/HivePigMapReduce/assets/57713624/98c66948-8a5b-445e-abdc-3ff85f62d13e)

3. Получить общую сумму, потраченную клиентом на заказы
   
![image](https://github.com/Gargamel-l/HivePigMapReduce/assets/57713624/340815b7-8f92-4834-b659-37eb62fcf80e)

5. Найти всех клиентов, которые ничего не заказывали

![image](https://github.com/Gargamel-l/HivePigMapReduce/assets/57713624/8bdc384d-fd92-4ddc-922d-65520194f97e)

6. Получить детали заказа (товары, количество и цена за единицу) по order_id

![image](https://github.com/Gargamel-l/HivePigMapReduce/assets/57713624/114f235d-6c57-4d51-ac98-05ca63d2f41d)

7. Получить список товаров, которые были заказаны более чем одним клиентом

![image](https://github.com/Gargamel-l/HivePigMapReduce/assets/57713624/4a9b9b3d-dc64-447b-b612-1aa9bbee928f)

#Результаты работы запросов Pig:

1. Получить список всех заказов клиента по его customer_id.

![image](https://github.com/Gargamel-l/HivePigMapReduce/assets/57713624/941f774a-8b65-4101-9e0e-d8e43fcfe678)

2. Получить общую сумму, потраченную клиентом на заказы

![image](https://github.com/Gargamel-l/HivePigMapReduce/assets/57713624/7aaecadf-5c27-4581-b900-5bb23033873a)

3. Найти всех клиентов, которые ничего не заказывали

![image](https://github.com/Gargamel-l/HivePigMapReduce/assets/57713624/b2b1d5b0-80dc-48d0-8325-25a7fc68ff7d)

4. Получить детали заказа (товары, количество и цена за единицу) по order_id

![image](https://github.com/Gargamel-l/HivePigMapReduce/assets/57713624/222f01b4-4175-47f3-8f86-6f5339cd3546)

5. Получить список товаров, которые были заказаны более чем одним клиентом

![image](https://github.com/Gargamel-l/HivePigMapReduce/assets/57713624/c00ac2f1-bf08-4fd8-87f2-a60ca3389404)

#Результаты работы собственной реализации MapReduce:

![image](https://github.com/Gargamel-l/HivePigMapReduce/assets/57713624/2cf4383f-c1a4-4c87-b733-6b57415faa8f)







