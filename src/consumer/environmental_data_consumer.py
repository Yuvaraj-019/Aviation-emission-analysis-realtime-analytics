import json
import psycopg2
from kafka import KafkaConsumer
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CombinedDataConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'aviation-environment-data',
            'traffic-congestion-data',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='combined-consumer-group'
        )
        self.db_connection = psycopg2.connect(
            dbname='aviation_db',
            user='postgres',
            password='postgres',
            host='localhost',
            port='5433'
        )

    def save_aviation_data(self, data):
        try:
            cursor = self.db_connection.cursor()
            timestamp = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00')) if 'timestamp' in data else datetime.utcnow()
            
            cursor.execute("""
                INSERT INTO flight_emission_data 
                (icao24, callsign, origin_country, longitude, latitude, 
                 baro_altitude, velocity, vertical_rate, geo_altitude,
                 aircraft_type, fuel_consumption_kg_hour, co2_emissions_kg_hour,
                 emission_intensity_kg_km, efficiency_score, predicted_co2_next_hour,
                 environmental_impact_score, noise_pollution_level, air_quality_index,
                 environmental_rating, population_density_factor, timestamp, data_source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data['icao24'], data['callsign'], data['origin_country'],
                data['longitude'], data['latitude'], data.get('baro_altitude', 0),
                data['velocity'], data.get('vertical_rate', 0), data['geo_altitude'],
                data['aircraft_type'], data['fuel_consumption_kg_hour'],
                data['co2_emissions_kg_hour'], data.get('emission_intensity_kg_km', 0),
                data.get('efficiency_score', 75), data.get('predicted_co2_next_hour', 0),
                data['environmental_impact_score'], data['noise_pollution_level'],
                data['air_quality_index'], data.get('environmental_rating', 'B'),
                data.get('population_density_factor', 1.0), timestamp,
                data.get('data_source', 'combined_realtime')
            ))
            
            self.db_connection.commit()
            cursor.close()
            
            logger.info(f"🌱 Saved Aviation: {data['callsign']} | {data['aircraft_type']} | Impact: {data['environmental_impact_score']:.1f}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Aviation save error: {e}")
            self.db_connection.rollback()
            return False

    def save_traffic_data(self, data):
        try:
            cursor = self.db_connection.cursor()
            timestamp = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00')) if 'timestamp' in data else datetime.utcnow()
            
            cursor.execute("""
                INSERT INTO traffic_congestion_data 
                (road_id, road_name, city, country, longitude, latitude,
                 traffic_level, average_speed_kmh, vehicle_count, 
                 congestion_index, co2_emissions_kg_hour, noise_level_db, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                data['road_id'], data['road_name'], data['city'], data['country'],
                data['longitude'], data['latitude'], data['traffic_level'],
                data['average_speed_kmh'], data['vehicle_count'], data['congestion_index'],
                data['co2_emissions_kg_hour'], data['noise_level_db'], timestamp
            ))
            
            self.db_connection.commit()
            cursor.close()
            
            logger.info(f"🚦 Saved Traffic: {data['city']} - {data['road_name']} - Congestion: {data['congestion_index']}%")
            return True
            
        except Exception as e:
            logger.error(f"❌ Traffic save error: {e}")
            self.db_connection.rollback()
            return False

    def process_message(self, message):
        try:
            data = message.value
            
            if message.topic == 'aviation-environment-data':
                self.save_aviation_data(data)
            elif message.topic == 'traffic-congestion-data':
                self.save_traffic_data(data)
                
        except Exception as e:
            logger.error(f"❌ Message processing error: {e}")

    def run(self):
        logger.info("🔄 Starting Combined Data Consumer...")
        logger.info("📥 Listening: aviation-environment-data + traffic-congestion-data")
        for message in self.consumer:
            self.process_message(message)

if __name__ == "__main__":
    consumer = CombinedDataConsumer()
    consumer.run()