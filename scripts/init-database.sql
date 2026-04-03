-- Aviation emissions table
CREATE TABLE IF NOT EXISTS flight_emission_data (
    id SERIAL PRIMARY KEY,
    icao24 VARCHAR(255),
    callsign VARCHAR(255),
    origin_country VARCHAR(255),
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    baro_altitude DOUBLE PRECISION,
    velocity DOUBLE PRECISION,
    vertical_rate DOUBLE PRECISION,
    geo_altitude DOUBLE PRECISION,
    aircraft_type VARCHAR(255),
    fuel_consumption_kg_hour DOUBLE PRECISION,
    co2_emissions_kg_hour DOUBLE PRECISION,
    emission_intensity_kg_km DOUBLE PRECISION,
    efficiency_score DOUBLE PRECISION,
    predicted_co2_next_hour DOUBLE PRECISION,
    environmental_impact_score DOUBLE PRECISION,
    noise_pollution_level DOUBLE PRECISION,
    air_quality_index DOUBLE PRECISION,
    environmental_rating VARCHAR(5),
    population_density_factor DOUBLE PRECISION,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_source VARCHAR(50)
);

-- Traffic congestion monitoring table
CREATE TABLE IF NOT EXISTS traffic_congestion_data (
    id SERIAL PRIMARY KEY,
    road_id VARCHAR(255),
    road_name VARCHAR(255),
    city VARCHAR(255),
    country VARCHAR(255),
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    traffic_level INTEGER,
    average_speed_kmh DOUBLE PRECISION,
    vehicle_count INTEGER,
    congestion_index DOUBLE PRECISION,
    co2_emissions_kg_hour DOUBLE PRECISION,
    noise_level_db DOUBLE PRECISION,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_flight_geo ON flight_emission_data (latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_flight_time ON flight_emission_data (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_flight_impact ON flight_emission_data (environmental_impact_score);

CREATE INDEX IF NOT EXISTS idx_traffic_geo ON traffic_congestion_data (latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_traffic_time ON traffic_congestion_data (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_traffic_congestion ON traffic_congestion_data (congestion_index);

-- Materialized view for combined environmental data
CREATE MATERIALIZED VIEW IF NOT EXISTS combined_environmental_view AS
SELECT 
    'aviation' as source_type,
    callsign as identifier,
    aircraft_type as type,
    longitude,
    latitude,
    environmental_impact_score,
    co2_emissions_kg_hour,
    noise_pollution_level as noise_level,
    timestamp,
    origin_country as location
FROM flight_emission_data 
WHERE timestamp >= NOW() - INTERVAL '1 hour'

UNION ALL

SELECT 
    'traffic' as source_type,
    road_id as identifier,
    road_name as type,
    longitude,
    latitude,
    congestion_index as environmental_impact_score,
    co2_emissions_kg_hour,
    noise_level_db as noise_level,
    timestamp,
    city as location
FROM traffic_congestion_data 
WHERE timestamp >= NOW() - INTERVAL '1 hour';

CREATE INDEX IF NOT EXISTS idx_combined_geo ON combined_environmental_view (latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_combined_source ON combined_environmental_view (source_type);