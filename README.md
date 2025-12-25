# DB2025
Лабораторные работы по БД

## Лр 1
<img width="1920" height="944" alt="image" src="https://github.com/user-attachments/assets/81c8cdd5-494c-4726-9c13-49fafdf7e7af" />

## Лр 2
```sql
CREATE TABLE izykenov.company (
    company_id SERIAL PRIMARY KEY,
    name VARCHAR,
    address VARCHAR,
    phone_number VARCHAR
);

CREATE TABLE izykenov.trip (
    trip_id SERIAL PRIMARY KEY,
    company_id INTEGER,
    to_the_city VARCHAR,
    from_the_city VARCHAR,
    departure_time TIMESTAMP,
    arrival_time TIMESTAMP,
    FOREIGN KEY (company_id) REFERENCES izykenov.company(company_id)
);


CREATE TABLE izykenov.iata (
    iata_code VARCHAR PRIMARY KEY,
    company_id INTEGER,
    FOREIGN KEY (company_id) REFERENCES izykenov.company(company_id)
);

CREATE TABLE izykenov.passenger (
    passenger_id SERIAL PRIMARY KEY,
    name VARCHAR,
    birthday TIMESTAMP,
    passport VARCHAR,
    email VARCHAR
);

CREATE TABLE izykenov.purchased_tickets (
    purchased_tickets SERIAL,
    trip_id INTEGER,
    passenger_id INTEGER,
    FOREIGN KEY (trip_id) REFERENCES izykenov.trip(trip_id),
    FOREIGN KEY (passenger_id) REFERENCES izykenov.passenger(passenger_id)
);

SELECT t.*, c.name
FROM izykenov.trip t
JOIN izykenov.company c 
ON t.company_id = c.company_id
WHERE t.to_the_city = 'Москва';
```

## Лр 3
```sql
CREATE OR REPLACE VIEW record_second AS
SELECT *
FROM izykenov.company
WHERE (EXTRACT(SECOND FROM now())::INTEGER) % 2 = 0;

CREATE OR REPLACE FUNCTION izykenov.my_function(company_id INT)
RETURNS table(a int, b varchar, c varchar, d varchar)
LANGUAGE plpgsql
AS $$
BEGIN
RETURN query
SELECT * FROM izykenov.record_second;
END
$$;

SELECT * FROM izykenov.my_function(1);
```

## Лр 4
```sql
Запрос: Популярные направления из Москвы
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT 
    t.to_the_city as destination,
    COUNT(*) as total_trips,
    COUNT(DISTINCT t.company_id) as companies_count,
    COUNT(DISTINCT pt.passenger_id) as passengers_count,
    ROUND(AVG(EXTRACT(EPOCH FROM (t.arrival_time - t.departure_time))/3600), 1) as avg_duration_hours
FROM izykenov.trip t
INNER JOIN izykenov.purchased_tickets pt ON t.trip_id = pt.trip_id
WHERE t.from_the_city = 'Moscow'
  AND EXTRACT(YEAR FROM t.departure_time) = 2024
  AND t.to_the_city NOT IN ('Moscow', 'St. Petersburg')
GROUP BY t.to_the_city
HAVING COUNT(*) >= 5
ORDER BY total_trips DESC, avg_duration_hours
LIMIT 8;

ИНДЕКСЫ:
Для фильтрации поездок из Москвы в 2024 году
CREATE INDEX idx_trip_moscow_2024 
ON izykenov.trip (from_the_city, departure_time)
WHERE from_the_city = 'Moscow' 
  AND departure_time >= '2024-01-01' 
  AND departure_time < '2025-01-01';

Для соединения trip с purchased_tickets
CREATE INDEX idx_purchased_tickets_trip_id 
ON izykenov.purchased_tickets (trip_id);

Для группировки по to_the_city (Covering index)
CREATE INDEX idx_trip_for_grouping 
ON izykenov.trip (to_the_city, company_id, departure_time, arrival_time, from_the_city);

EXPLAIN (ANALYZE, BUFFERS)
SELECT 
    t.to_the_city as destination,
    COUNT(*) as total_trips,
    COUNT(DISTINCT t.company_id) as companies_count,
    COUNT(DISTINCT pt.passenger_id) as passengers_count,
    ROUND(AVG(EXTRACT(EPOCH FROM (t.arrival_time - t.departure_time))/3600), 1) as avg_duration_hours
FROM izykenov.trip t
INNER JOIN izykenov.purchased_tickets pt ON t.trip_id = pt.trip_id
WHERE t.from_the_city = 'Moscow'
  AND t.departure_time >= '2024-01-01'
  AND t.departure_time < '2025-01-01'
  AND t.to_the_city NOT IN ('Moscow', 'St. Petersburg')
GROUP BY t.to_the_city
HAVING COUNT(*) >= 5
ORDER BY total_trips DESC, avg_duration_hours
LIMIT 8;


DROP INDEX IF EXISTS izykenov.idx_trip_moscow_2024;
DROP INDEX IF EXISTS izykenov.idx_purchased_tickets_trip_id;
DROP INDEX IF EXISTS izykenov.idx_trip_for_grouping;
 

LIKE 'foo%'	ДА, использует	Начинается с константы
LIKE '%bar'	 НЕТ, не использует	Начинается с wildcard (%)
```

## Лр 5
```sql
Создание триггерные функции для каскадного удаления
Функция для удаления связанных записей при удалении компании:
CREATE OR REPLACE FUNCTION izykenov.delete_company_cascade()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM izykenov.trip
    WHERE company_id = OLD.company_id;

    DELETE FROM izykenov.iata
    WHERE company_id = OLD.company_id;

    RETURN OLD;
END;
$$;

Функция для удаления связанных записей при удалении рейса:
CREATE OR REPLACE FUNCTION izykenov.delete_trip_cascade()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM izykenov.purchased_tickets
    WHERE trip_id = OLD.trip_id;

    RETURN OLD;
END;
$$;

Функция для удаления связанных записей при удалении пассажира:
CREATE OR REPLACE FUNCTION izykenov.delete_passenger_cascade()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM izykenov.purchased_tickets
    WHERE passenger_id = OLD.passenger_id;

    RETURN OLD;
END;
$$;

Создание триггеры для каскадного удаления
CREATE TRIGGER trg_delete_company_cascade
BEFORE DELETE ON izykenov.company
FOR EACH ROW
EXECUTE FUNCTION izykenov.delete_company_cascade();

CREATE TRIGGER trg_delete_trip_cascade
BEFORE DELETE ON izykenov.trip
FOR EACH ROW
EXECUTE FUNCTION izykenov.delete_trip_cascade();

CREATE TRIGGER trg_delete_passenger_cascade
BEFORE DELETE ON izykenov.passenger
FOR EACH ROW
EXECUTE FUNCTION izykenov.delete_passenger_cascade();

Создание таблицы-журнала для аудита
CREATE TABLE izykenov.audit_log (
    audit_id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    operation_type VARCHAR(10) NOT NULL CHECK (operation_type IN ('INSERT', 'UPDATE', 'DELETE')),
    old_data JSONB,
    new_data JSONB,
    changed_at TIMESTAMP DEFAULT NOW(),
    changed_by VARCHAR(255) DEFAULT CURRENT_USER
);

Создание общей функции аудита
CREATE OR REPLACE FUNCTION izykenov.log_changes()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_old_data JSONB;
    v_new_data JSONB;
BEGIN
    IF TG_OP = 'UPDATE' OR TG_OP = 'DELETE' THEN
        v_old_data := to_jsonb(OLD);
    END IF;

    IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
        v_new_data := to_jsonb(NEW);
    END IF;

    INSERT INTO izykenov.audit_log (
        table_name,
        operation_type,
        old_data,
        new_data,
        changed_at,
        changed_by
    )
    VALUES (
        TG_TABLE_NAME,
        TG_OP,
        v_old_data,
        v_new_data,
        NOW(),
        CURRENT_USER
    );

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    ELSE
        RETURN NEW;
    END IF;
END;
$$;

Создание триггера аудита для всех таблиц
CREATE TRIGGER trg_company_audit
AFTER INSERT OR UPDATE OR DELETE ON izykenov.company
FOR EACH ROW
EXECUTE FUNCTION izykenov.log_changes();

CREATE TRIGGER trg_trip_audit
AFTER INSERT OR UPDATE OR DELETE ON izykenov.trip
FOR EACH ROW
EXECUTE FUNCTION izykenov.log_changes();

CREATE TRIGGER trg_iata_audit
AFTER INSERT OR UPDATE OR DELETE ON izykenov.iata
FOR EACH ROW
EXECUTE FUNCTION izykenov.log_changes();

CREATE TRIGGER trg_passenger_audit
AFTER INSERT OR UPDATE OR DELETE ON izykenov.passenger
FOR EACH ROW
EXECUTE FUNCTION izykenov.log_changes();

CREATE TRIGGER trg_purchased_tickets_audit
AFTER INSERT OR UPDATE OR DELETE ON izykenov.purchased_tickets
FOR EACH ROW
EXECUTE FUNCTION izykenov.log_changes();

Пример для тестирования и сдачи:
Каскадное удаление: Удалил компанию -> автоматически удалились все её рейсы и билеты
Журнал изменений: Любое изменение в БД (добавление, изменение, удаление) → записывается кто, что и когда сделал


TRUNCATE TABLE izykenov.audit_log RESTART IDENTITY; - для очистки полного аудита перед сдачей

INSERT INTO izykenov.company (name, address, phone_number)
VALUES ('S7 Airlines', 'Новосибирск, Толмачёво', '+7-383-333-33-33');

INSERT INTO izykenov.trip (company_id, to_the_city, from_the_city, departure_time, arrival_time)
VALUES (currval('izykenov.company_company_id_seq'), 'Москва', 'Новосибирск', 
        '2024-12-18 08:00:00', '2024-12-18 12:00:00');

INSERT INTO izykenov.passenger (name, birthday, passport, email)
VALUES ('Иванов Иван', '1990-01-01', '1234 567890', 'ivan@example.com');

INSERT INTO izykenov.purchased_tickets (trip_id, passenger_id)
VALUES (currval('izykenov.trip_trip_id_seq'), currval('izykenov.passenger_passenger_id_seq'));

UPDATE izykenov.company 
SET phone_number = '+7-495-765-43-21'
WHERE name = 'S7 Airlines';

DELETE FROM izykenov.company WHERE name = 'S7 Airlines';

SELECT table_name, operation_type, COUNT(*) 
FROM izykenov.audit_log 
GROUP BY table_name, operation_type 
ORDER BY table_name;

Создал 3 таблицы, изменил и удалил в 1 таблице данные, но удалились сразу в 3 таблицах из-за триггера


Создать компанию, добавить трипы, посмотреть что они есть.

INSERT INTO izykenov.company (name, address, phone_number)
VALUES ('S7 Airlines', 'Новосибирск, аэропорт Толмачёво', '+7-383-333-33-33')
RETURNING company_id;

INSERT INTO izykenov.trip (
    company_id, 
    from_the_city, 
    to_the_city, 
    departure_time, 
    arrival_time
)
VALUES (
    1,  
    'Новосибирск',
    'Москва',
    '2024-12-19 08:00:00',
    '2024-12-19 11:30:00'
);

INSERT INTO izykenov.trip (
    company_id, 
    from_the_city, 
    to_the_city, 
    departure_time, 
    arrival_time
)
VALUES (
    1,
    'Москва',
    'Новосибирск',
    '2024-12-20 18:00:00',
    '2024-12-21 00:30:00'
);

INSERT INTO izykenov.trip (
    company_id, 
    from_the_city, 
    to_the_city, 
    departure_time, 
    arrival_time
)
VALUES (
    1,
    'Новосибирск',
    'Сочи',
    '2024-12-22 12:00:00',
    '2024-12-22 16:45:00'
);

SELECT 
    t.trip_id,
    c.name,
    t.from_the_city,
    t.to_the_city,
    t.departure_time,
    t.arrival_time,
    (t.arrival_time - t.departure_time)
FROM izykenov.trip t
JOIN izykenov.company c ON t.company_id = c.company_id;

Удалить компанию и посмотреть, что трипов нет.
DELETE FROM izykenov.company WHERE name = 'Company#20000';
```
