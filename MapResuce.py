

### First test
def map_orders(file_name):
    with open(file_name, 'r') as file:
        for line in file:
            parts = line.strip().split(',')
            yield (parts[1], line.strip())  # ключ - customer_id, значение - вся строка

# Функция reduce
def reduce_orders(mapped_values):
    reduced_values = {}
    for key, value in mapped_values:
        if key in reduced_values:
            reduced_values[key].append(value)
        else:
            reduced_values[key] = [value]
    return reduced_values

# Эмуляция MapReduce
def map_reduce_orders(file_name):
    mapped_values = list(map_orders(file_name))
    reduced_values = reduce_orders(mapped_values)
    return reduced_values

### Second test
# Функция map для обработки каждой строки файла
def map_total_spent(file_name):
    with open(file_name, 'r') as file:
        next(file)  # Пропускаем заголовок
        for line in file:
            parts = line.strip().split(',')
            customer_id = parts[1]
            total_amount = float(parts[3])
            yield (customer_id, total_amount)

# Функция reduce для агрегации данных по customer_id
def reduce_total_spent(mapped_values):
    total_spent_by_customer = {}
    for customer_id, amount in mapped_values:
        if customer_id in total_spent_by_customer:
            total_spent_by_customer[customer_id] += amount
        else:
            total_spent_by_customer[customer_id] = amount
    return total_spent_by_customer

# Эмуляция MapReduce
def map_reduce_total_spent(file_name):
    mapped_values = list(map_total_spent(file_name))
    reduced_values = reduce_total_spent(mapped_values)
    return reduced_values

### Third test

# Функция map для обработки строк из файлов клиентов и заказов
def map_customers_orders(customers_file, orders_file):
    customers = set()
    # Считываем всех клиентов
    with open(customers_file, 'r') as file:
        next(file)  # Пропускаем заголовок
        for line in file:
            customer_id = line.strip().split(',')[0]
            customers.add(customer_id)
            yield (customer_id, 'customer')

    # Считываем все заказы
    with open(orders_file, 'r') as file:
        next(file)  # Пропускаем заголовок
        for line in file:
            customer_id = line.strip().split(',')[1]
            yield (customer_id, 'order')

# Функция reduce для определения клиентов без заказов
def reduce_customers_without_orders(mapped_values):
    customers_with_orders = set()
    all_customers = set()
    for customer_id, record_type in mapped_values:
        if record_type == 'customer':
            all_customers.add(customer_id)
        elif record_type == 'order':
            customers_with_orders.add(customer_id)
    # Возвращаем клиентов, которые не совершали заказы
    return all_customers - customers_with_orders

# Эмуляция MapReduce
def map_reduce_customers_without_orders(customers_file, orders_file):
    mapped_values = list(map_customers_orders(customers_file, orders_file))
    reduced_values = reduce_customers_without_orders(mapped_values)
    return reduced_values

###Forth test

def map_order_details(orders_file, order_details_file, products_file, target_order_id):
    # Словари для хранения деталей продуктов и заказов
    product_details = {}
    order_products = []

    # Считываем информацию о продуктах
    with open(products_file, 'r') as file:
        next(file)  # Пропускаем заголовок
        for line in file:
            product_id, name, price = line.strip().split(',')
            product_details[product_id] = {'name': name, 'price': float(price)}

    # Считываем детали заказов для целевого order_id
    with open(order_details_file, 'r') as file:
        next(file)  # Пропускаем заголовок
        for line in file:
            order_id, product_id, quantity = line.strip().split(',')
            if order_id == target_order_id:
                # Добавляем информацию о продукте и его количестве в заказе
                order_products.append((product_id, int(quantity)))

    # Возвращаем список деталей заказа с информацией о продукте
    return [(product_id, product_details[product_id]['name'], quantity, product_details[product_id]['price'])
            for product_id, quantity in order_products]

# Функция reduce не требуется, так как агрегация не выполняется
def get_order_details(orders_file, order_details_file, products_file, order_id):
    # Вызов функции map для получения деталей заказа по order_id
    order_details = map_order_details(orders_file, order_details_file, products_file, str(order_id))
    return order_details


### Fifth test

# Функция map для обработки строк из файла заказов и файла деталей заказа
def map_products_customers(orders_file, order_details_file, products_file):
    order_customer_map = {}
    # Сопоставляем заказы и клиентов
    with open(orders_file, 'r') as file:
        next(file)  # Пропускаем заголовок
        for line in file:
            parts = line.strip().split(',')
            order_id, customer_id = parts[0], parts[1]
            order_customer_map[order_id] = customer_id

    # Сопоставляем заказы с товарами
    with open(order_details_file, 'r') as file:
        next(file)  # Пропускаем заголовок
        for line in file:
            parts = line.strip().split(',')
            order_id, product_id = parts[0], parts[1]
            customer_id = order_customer_map.get(order_id, None)
            if customer_id:
                yield (product_id, customer_id)

# Функция reduce для подсчета уникальных клиентов для каждого товара
def reduce_products_multiple_customers(mapped_values):
    product_customers = {}
    for product_id, customer_id in mapped_values:
        if product_id in product_customers:
            product_customers[product_id].add(customer_id)
        else:
            product_customers[product_id] = {customer_id}
    
    # Отфильтровываем товары, заказанные более чем одним клиентом
    return {product: customers for product, customers in product_customers.items() if len(customers) > 1}

# Эмуляция MapReduce
def map_reduce_products_multiple_customers(orders_file, order_details_file, products_file):
    mapped_values = list(map_products_customers(orders_file, order_details_file, products_file))
    reduced_values = reduce_products_multiple_customers(mapped_values)
    return reduced_values


# Запустим MapReduce для файла orders.csv и получим заказы для customer_id = '1'
orders_by_customer = map_reduce_orders('/home/osboxes/mysources/lab3/orders.csv')
for customer_id in orders_by_customer:
    if customer_id == '1':  # Предполагая, что мы ищем заказы клиента с ID 1
        print(f"Заказы клиента {customer_id}:")
        for order in orders_by_customer[customer_id]:
            print('First test: ', order)

# Предполагая, что файл orders.csv находится в том же каталоге
file_name = '/home/osboxes/mysources/lab3/orders.csv'  
total_spent = map_reduce_total_spent(file_name)
print('Second test: ', total_spent)

customers_file = '/home/osboxes/mysources/lab3/customers.csv'  
orders_file = '/home/osboxes/mysources/lab3/orders.csv'  

customers_without_orders = map_reduce_customers_without_orders(customers_file, orders_file)
print('Third test: ', customers_without_orders)

orders_file = '/home/osboxes/mysources/lab3/orders.csv'  
order_details_file = '/home/osboxes/mysources/lab3/order_details.csv'  
products_file = '/home/osboxes/mysources/lab3/products.csv'  
target_order_id = 1  # Целевой ID заказа

# Получение и вывод деталей заказа
order_details = get_order_details(orders_file, order_details_file, products_file, target_order_id)
for detail in order_details:
    print('Forth test: ', detail)

# Пути к файлам
orders_file = '/home/osboxes/mysources/lab3/orders.csv'  
order_details_file = '/home/osboxes/mysources/lab3/order_details.csv' 
products_file = '/home/osboxes/mysources/lab3/products.csv'  

products_ordered_by_multiple_customers = map_reduce_products_multiple_customers(orders_file, order_details_file, products_file)
print('Fifth test: ', products_ordered_by_multiple_customers)
