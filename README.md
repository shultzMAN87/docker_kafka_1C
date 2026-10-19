# Kafka Cluster — 3 брокера KRaft + Kafka UI

Готовый стенд для тестов и разработки: кластер Apache Kafka из трёх нод в
режиме KRaft (без ZooKeeper) и веб-админка [Kafka UI](https://github.com/kafbat/kafka-ui).
Поднимается одной командой.

```
                  ┌────────────┐
  клиенты  ──────>│  kafka-0   │ :29092 ─┐
 (снаружи         ├────────────┤         │  репликация,
  Docker)  ──────>│  kafka-1   │ :29093  ├─ кворум контроллеров
           ──────>│  kafka-2   │ :29094 ─┘  (KRaft)
                  └─────┬──────┘
                        │ kafka-N:9091 (внутренняя сеть)
                  ┌─────┴──────┐
                  │  Kafka UI  │ :8080
                  └────────────┘
```

## Состав

| Сервис | Образ | Назначение |
|---|---|---|
| `kafka-0`, `kafka-1`, `kafka-2` | `confluentinc/cp-kafka:8.1.0` (Kafka 4.1) | брокер + контроллер в каждой ноде |
| `kafka-ui` | `kafbat/kafka-ui:v1.5.0` | веб-интерфейс: топики, сообщения, consumer groups, брокеры |

Настройки кластера по умолчанию:

- replication factor = 3, `min.insync.replicas` = 2 — кластер переживает
  остановку одного брокера без потери записи;
- 3 партиции у новых топиков;
- автосоздание топиков включено;
- по 512 МБ памяти JVM на брокер (весь стенд — около 2–2,5 ГБ RAM).

## Требования

- Docker с Compose v2 (`docker compose`, не `docker-compose`)
- свободные порты 29092–29094 и 8080 (меняются в `.env`)

## Быстрый старт

```bash
git clone <url-этого-репозитория> kafka-cluster
cd kafka-cluster
cp .env.example .env      # необязательно — без .env работают значения по умолчанию
docker compose up -d
```

Через 30–60 секунд кластер готов. Проверка:

```bash
docker compose ps                    # kafka-0..2 и kafka-ui — healthy/running
```

Kafka UI: **http://localhost:8080**

## Подключение

| Откуда | Bootstrap servers |
|---|---|
| с хоста (приложение, IDE, CLI) | `localhost:29092,localhost:29093,localhost:29094` |
| из другого контейнера в той же compose-сети | `kafka-0:9091,kafka-1:9091,kafka-2:9091` |

Достаточно указать один адрес — клиент получит остальные из метаданных, но
список из трёх надёжнее при остановленном брокере.

### Доступ с другой машины

Брокеры сообщают клиентам адрес из `KAFKA_EXTERNAL_HOST`. По умолчанию это
`localhost`, поэтому подключиться можно только с той же машины. Если стенд
поднят на сервере или виртуалке, укажите в `.env` её IP или имя:

```env
KAFKA_EXTERNAL_HOST=192.168.1.50
```

и пересоздайте контейнеры: `docker compose up -d`.

## Настройка через `.env`

| Переменная | По умолчанию | Что задаёт |
|---|---|---|
| `KAFKA_EXTERNAL_HOST` | `localhost` | адрес брокеров для внешних клиентов |
| `KAFKA_0_PORT` / `KAFKA_1_PORT` / `KAFKA_2_PORT` | `29092` / `29093` / `29094` | внешние порты брокеров |
| `KAFKA_HEAP_OPTS` | `-Xms512m -Xmx512m` | память JVM на брокер |
| `KAFKA_AUTO_CREATE_TOPICS` | `true` | автосоздание топиков |
| `KAFKA_UI_PORT` | `8080` | порт Kafka UI |
| `KAFKA_CLUSTER_NAME` | `local` | имя кластера в Kafka UI |

## Создание топиков

Отдельного шага инициализации нет, топик можно получить тремя способами:

1. **Автоматически.** Достаточно начать писать в топик или читать из него —
   брокер создаст его с 3 партициями и RF=3. Отключается через
   `KAFKA_AUTO_CREATE_TOPICS=false`.
2. **В Kafka UI.** Topics → Add a topic: имя, число партиций, replication factor.
3. **Через CLI** — команда ниже в «Полезных командах».

Для тестов, где важны партиции или настройки топика, лучше создавать его явно
(способ 2 или 3), а автосоздание оставить как подстраховку.

## Полезные команды

Все CLI-утилиты Kafka есть внутри брокеров, путь добавлять не нужно.

```bash
# список топиков
docker exec kafka-0 kafka-topics --bootstrap-server kafka-0:9091 --list

# создать топик
docker exec kafka-0 kafka-topics --bootstrap-server kafka-0:9091 \
  --create --topic demo --partitions 3 --replication-factor 3

# описание топика: лидеры партиций и ISR
docker exec kafka-0 kafka-topics --bootstrap-server kafka-0:9091 --describe --topic demo

# отправить сообщения (ввод построчно, выход — Ctrl+C)
docker exec -it kafka-0 kafka-console-producer --bootstrap-server kafka-0:9091 --topic demo

# прочитать с начала
docker exec -it kafka-0 kafka-console-consumer --bootstrap-server kafka-0:9091 \
  --topic demo --from-beginning

# состояние кворума KRaft
docker exec kafka-0 kafka-metadata-quorum --bootstrap-server kafka-0:9091 describe --status
```

### Проверка отказоустойчивости

```bash
docker stop kafka-1
docker exec kafka-0 kafka-topics --bootstrap-server kafka-0:9091 --describe --topic demo
# лидеры переехали, ISR сократился до 2 — запись и чтение продолжают работать
docker start kafka-1
```

## Остановка и сброс

```bash
docker compose stop        # остановить, контейнеры и данные сохраняются
docker compose down        # удалить контейнеры, данные в томах сохраняются
docker compose down -v     # полный сброс, включая все сообщения и топики
```

## Ограничения

Стенд предназначен **только для тестов и разработки**:

- без шифрования и аутентификации (PLAINTEXT), Kafka UI открыт без пароля;
- все три ноды на одной машине — реальной отказоустойчивости по железу нет;
- `CLUSTER_ID` зашит в `compose.yml`. Если меняете его — делайте
  `docker compose down -v`, иначе брокеры не стартуют на старых томах.

## Структура

```
.
├── compose.yml      # кластер и Kafka UI
├── .env.example     # шаблон настроек
└── .gitignore
```
