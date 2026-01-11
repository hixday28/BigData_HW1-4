import asyncio
import json
from telethon import TelegramClient, events
from kafka import KafkaProducer

API_ID = 123
API_HASH = '123'

KAFKA_SERVER = 'localhost:9092'
TOPIC = 'telegram_stream'

# Список каналов для прослушки
TARGET_CHANNELS = [
    'BarcaFamilyNews', 'Match_TV', 'barcafamilyyy', 
    'Vasantino', 'myachPRO', 'offearth', '@dozafutbola', 'wtw2026'
]

async def main():
    # Подключение к Kafka
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"Kafka Producer подключен к {KAFKA_SERVER}")
    except Exception as e:
        print(f"Ошибка подключения к Kafka. Запущен ли Docker? Ошибка: {e}")
        return

    # Подключаемся к Telegram
    client = TelegramClient('homework_session', API_ID, API_HASH)
    await client.start()
    print("Telegram Client запущен")

    # Получаем ID каналов
    print("Поик каналов ...")
    valid_chats = []
    
    for link in TARGET_CHANNELS:
        try:
            entity = await client.get_entity(link)
            valid_chats.append(entity.id)
            print(f"   -> Найден: {link}")
        except Exception as e:
            print(f"   -> Не найден или нет доступа: {link} ({e})")

    print(f"Прослушивание {len(valid_chats)} каналов...")

    # Обработчик сообщений
    @client.on(events.NewMessage(chats=valid_chats))
    async def handler(event):
        try:
            if not event.message.message:
                return

            text = event.message.message
            
            # Формируем JSON для отправки
            message_data = {
                "text": text,
                "timestamp": event.date.timestamp(),
                "channel_id": event.chat_id,
                "msg_id": event.message.id
            }

            # Отправляем в Kafka
            producer.send(TOPIC, message_data)
            print(f"[Sent] {text[:40].replace(chr(10), ' ')}...")

        except Exception as e:
            print(f"Ошибка обработки: {e}")

    await client.run_until_disconnected()

if __name__ == '__main__':
    asyncio.run(main())