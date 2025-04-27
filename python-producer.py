import datetime
import time
import sys
import os
import threading
import queue
import json
import asyncio
import aiohttp
import yaml
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv
from shared.weather.api import WeatherAPI

# Add project root to Python path
sys.path.append("/app")

##  External pipeline configurations  ##
kafka_nodes = os.environ.get('KAFKA_SERVER', 'kafka:9092')
weather_topic = "weather-data"
control_topic = "city-control"

def log_message(msg):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {msg}"
    print(log_entry, flush=True)

class WeatherProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_nodes,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(0, 10, 1)
        )
        self.weather_api = WeatherAPI()
        self.session = None
        self.all_cities = []
        self.dynamic_cities = []
        self.load_cities_from_config()
        self.cities_lock = threading.Lock()
        
        # Start control message consumer in a separate thread
        self.control_consumer_thread = threading.Thread(target=self._run_control_consumer)
        self.control_consumer_thread.daemon = True
        self.control_consumer_thread.start()

    def load_cities_from_config(self):
        """Load cities from configuration.yml"""
        try:
            with open('/app/configuration.yml', 'r') as f:
                config = yaml.safe_load(f)
                
            # Get static cities
            static_cities = config.get('cities', [])
            
            # Get dynamic cities if enabled
            self.dynamic_cities = []
            if config.get('dynamicCities', {}).get('enabled', False):
                self.dynamic_cities = config.get('dynamicCities', {}).get('current', [])
                
            # Combine all cities
            self.all_cities = list(set(static_cities + self.dynamic_cities))
            log_message(f"Loaded {len(self.all_cities)} total cities ({len(self.dynamic_cities)} dynamic)")
        except Exception as e:
            log_message(f"Error loading cities from configuration: {str(e)}")
            self.all_cities = []
            self.dynamic_cities = []

    def _run_control_consumer(self):
        """Run the control message consumer in a separate thread"""
        try:
            consumer = KafkaConsumer(
                control_topic,
                bootstrap_servers=kafka_nodes,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                api_version=(0, 10, 1)
            )
            
            log_message("Started control message consumer")
            
            for message in consumer:
                try:
                    control_data = message.value
                    if control_data.get('action') == 'UPDATE_CITIES':
                        new_cities = control_data.get('data', {}).get('cities', [])
                        with self.cities_lock:
                            self.dynamic_cities = new_cities
                            self.all_cities = list(set(self.all_cities + new_cities))
                        log_message(f"Updated dynamic cities list from control message: {new_cities}")
                except Exception as e:
                    log_message(f"Error processing control message: {str(e)}")
                    
        except Exception as e:
            log_message(f"Control consumer error: {str(e)}")

    async def initialize(self):
        """Initialize the producer and create an aiohttp session"""
        self.session = aiohttp.ClientSession()
        if not self.all_cities:
            log_message("No cities loaded from configuration, using default list")
            with self.cities_lock:
                self.all_cities = [
                    "London", "New York", "Tokyo", "Paris", "Sydney",
                    "Berlin", "Moscow", "Dubai", "Singapore", "Rio de Janeiro"
                ]

    async def close(self):
        """Close the aiohttp session"""
        if self.session:
            await self.session.close()

    async def fetch_city_data(self, city: str) -> dict:
        """Fetch weather data for a single city asynchronously"""
        return await self.weather_api.fetch_city_data_async(city, self.session)

    async def process_cities_batch(self, cities: list) -> dict:
        """Process multiple cities concurrently"""
        tasks = [self.fetch_city_data(city) for city in cities]
        results = await asyncio.gather(*tasks)
        return {city: data for city, data in zip(cities, results) if data is not None}

    def send_city_data(self, city: str, data: dict):
        """Send city data to Kafka with dynamic city prefix if applicable"""
        try:
            # Check if this is a dynamic city
            is_dynamic = city in self.dynamic_cities
            
            # Only add the DYNAMIC CITY prefix for actually dynamic cities
            prefix = "DYNAMIC CITY: " if is_dynamic else ""
            log_message(f"{prefix}Sent data for {city}: {json.dumps(data)}")
            
            self.producer.send(weather_topic, value=data)
        except Exception as e:
            log_message(f"Error sending data for {city}: {str(e)}")

    async def run(self, interval: int = None):
        """Main run loop"""
        try:
            await self.initialize()
            log_message(f"Starting weather data producer for {len(self.all_cities)} cities")
            
            interval_ms = int(os.environ.get('PRODUCER_INTERVAL', 100))
            interval_sec = interval_ms / 1000.0
            
            while True:
                try:
                    with self.cities_lock:
                        current_cities = self.all_cities.copy()
                    
                    batch_results = await self.process_cities_batch(current_cities)
                    
                    # Send each city's data
                    for city, data in batch_results.items():
                        if data:
                            self.send_city_data(city, data)
                    
                    self.producer.flush()
                    await asyncio.sleep(interval_sec)
                    
                except Exception as e:
                    log_message(f"Error in cycle: {str(e)}")
                    await asyncio.sleep(0.1)
                
        except Exception as e:
            log_message(f"Fatal error in run loop: {str(e)}")
        finally:
            log_message("Shutting down producer")
            await self.close()
            self.producer.close()

async def main():
    producer = WeatherProducer()
    await producer.run()

if __name__ == "__main__":
    asyncio.run(main())