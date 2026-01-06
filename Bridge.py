import asyncio
import websockets
import json
import time
from kafka import KafkaConsumer

connected_clients = set()

async def kafka_reader():
    print("Connecting to Kafka...")
    try:
        # Latest offset and unique group ensures LIVE departure data only
        consumer = KafkaConsumer(
            'location_data',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            group_id=f'bridge-group-{int(time.time())}',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print("Kafka Connected! Waiting for bus data...")
        
        for message in consumer:
            data = message.value
            if connected_clients:
                msg = json.dumps(data)
                await asyncio.gather(*[client.send(msg) for client in connected_clients])
            await asyncio.sleep(0.01)
    except Exception as e:
        print(f"Bridge Error: {e}")

async def handler(websocket):
    connected_clients.add(websocket)
    try:
        await websocket.wait_closed()
    finally:
        connected_clients.remove(websocket)

async def main():
    asyncio.create_task(kafka_reader())
    async with websockets.serve(handler, "localhost", 8765):
        print("Bridge running on ws://localhost:8765")
        await asyncio.Future() 

if __name__ == "__main__":
    asyncio.run(main())
