import json
import time
import requests
from kafka import KafkaProducer
import logging
import random
from datetime import datetime
import pycountry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CompleteGlobalEnvironmentalProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Load COMPLETE countries database
        self.all_countries = self.load_complete_countries()
        logger.info(f"🌍 Loaded {len(self.all_countries)} countries and territories")
        
        # Comprehensive aircraft environmental profiles
        self.aircraft_environment_profiles = {
            'A320': {'co2_per_km': 75, 'noise_db': 85, 'fuel_efficiency': 0.8, 'env_rating': 'B'},
            'A320neo': {'co2_per_km': 65, 'noise_db': 80, 'fuel_efficiency': 0.9, 'env_rating': 'A'},
            'B737-800': {'co2_per_km': 80, 'noise_db': 88, 'fuel_efficiency': 0.75, 'env_rating': 'C'},
            'B737-MAX8': {'co2_per_km': 68, 'noise_db': 82, 'fuel_efficiency': 0.88, 'env_rating': 'A'},
            'A321': {'co2_per_km': 78, 'noise_db': 86, 'fuel_efficiency': 0.78, 'env_rating': 'B'},
            'B787-9': {'co2_per_km': 95, 'noise_db': 85, 'fuel_efficiency': 0.85, 'env_rating': 'B'},
            'A350-900': {'co2_per_km': 88, 'noise_db': 83, 'fuel_efficiency': 0.92, 'env_rating': 'A'},
            'E190': {'co2_per_km': 58, 'noise_db': 78, 'fuel_efficiency': 0.87, 'env_rating': 'A'},
            'A380': {'co2_per_km': 120, 'noise_db': 90, 'fuel_efficiency': 0.7, 'env_rating': 'D'},
            'B777': {'co2_per_km': 110, 'noise_db': 88, 'fuel_efficiency': 0.78, 'env_rating': 'C'},
            'CRJ900': {'co2_per_km': 45, 'noise_db': 75, 'fuel_efficiency': 0.85, 'env_rating': 'A'},
            'ATR72': {'co2_per_km': 35, 'noise_db': 72, 'fuel_efficiency': 0.9, 'env_rating': 'A+'},
            'A319': {'co2_per_km': 70, 'noise_db': 83, 'fuel_efficiency': 0.82, 'env_rating': 'B'},
            'A330': {'co2_per_km': 105, 'noise_db': 87, 'fuel_efficiency': 0.8, 'env_rating': 'C'},
            'B747': {'co2_per_km': 150, 'noise_db': 95, 'fuel_efficiency': 0.65, 'env_rating': 'D'},
            'B767': {'co2_per_km': 100, 'noise_db': 86, 'fuel_efficiency': 0.75, 'env_rating': 'C'},
            'ERJ175': {'co2_per_km': 42, 'noise_db': 74, 'fuel_efficiency': 0.88, 'env_rating': 'A'},
            'DHC8': {'co2_per_km': 30, 'noise_db': 70, 'fuel_efficiency': 0.92, 'env_rating': 'A+'}
        }
        
        # Complete aircraft type mapping
        self.aircraft_type_map = {
            'A20N': 'A320neo', 'A21N': 'A321neo', 'A319': 'A319', 'A320': 'A320',
            'A321': 'A321', 'A332': 'A330-200', 'A333': 'A330-300', 'A338': 'A330-800',
            'A339': 'A330-900', 'A358': 'A350-800', 'A359': 'A350-900', 'A35K': 'A350-1000',
            'A388': 'A380-800', 
            'B738': 'B737-800', 'B739': 'B737-900', 'B37M': 'B737-MAX7',
            'B38M': 'B737-MAX8', 'B39M': 'B737-MAX9', 'B3XM': 'B737-MAX10',
            'B744': 'B747-400', 'B748': 'B747-8', 'B752': 'B757-200', 'B753': 'B757-300',
            'B762': 'B767-200', 'B763': 'B767-300', 'B764': 'B767-400',
            'B772': 'B777-200', 'B773': 'B777-300', 'B77W': 'B777-300ER',
            'B77L': 'B777-200LR', 'B778': 'B777-8', 'B779': 'B777-9',
            'B788': 'B787-8', 'B789': 'B787-9', 'B78X': 'B787-10',
            'E190': 'E190', 'E195': 'E195', 'E75L': 'ERJ175',
            'CRJ9': 'CRJ900', 'CRJ7': 'CRJ700', 'CRJ2': 'CRJ200',
            'AT76': 'ATR72', 'AT45': 'ATR42', 'DH8D': 'DHC8'
        }

        # Global airline registry - 150+ airlines covering every country
        self.airline_prefixes = self.create_global_airline_registry()

    def load_complete_countries(self):
        """Load ALL countries and territories from pycountry"""
        countries_list = []
        try:
            # Countries
            countries_list.extend([country.name for country in pycountry.countries])
            
            # Historical countries
            try:
                countries_list.extend([country.name for country in pycountry.historic_countries])
            except:
                pass
                
            # Subdivisions (major territories)
            try:
                territories = [subdivision.name for subdivision in pycountry.subdivisions]
                # Add major territories
                major_territories = [
                    'Puerto Rico', 'Guam', 'Bermuda', 'Greenland', 'Faroe Islands',
                    'Cayman Islands', 'British Virgin Islands', 'Aruba', 'Curacao',
                    'French Guiana', 'Guadeloupe', 'Martinique', 'Reunion',
                    'Macau', 'Hong Kong', 'Taiwan', 'Palestine', 'Kosovo'
                ]
                countries_list.extend(major_territories)
            except:
                pass

            logger.info(f"✅ Successfully loaded {len(countries_list)} countries and territories")
            return list(set(countries_list))  # Remove duplicates
            
        except Exception as e:
            logger.error(f"❌ Failed to load from pycountry: {e}")
            return self.get_comprehensive_country_list()

    def get_comprehensive_country_list(self):
        """Manual comprehensive list of ALL countries and territories"""
        return [
            # United Nations Member States (193)
            'Afghanistan', 'Albania', 'Algeria', 'Andorra', 'Angola', 'Antigua and Barbuda',
            'Argentina', 'Armenia', 'Australia', 'Austria', 'Azerbaijan', 'Bahamas', 'Bahrain',
            'Bangladesh', 'Barbados', 'Belarus', 'Belgium', 'Belize', 'Benin', 'Bhutan', 'Bolivia',
            'Bosnia and Herzegovina', 'Botswana', 'Brazil', 'Brunei', 'Bulgaria', 'Burkina Faso',
            'Burundi', 'Cabo Verde', 'Cambodia', 'Cameroon', 'Canada', 'Central African Republic',
            'Chad', 'Chile', 'China', 'Colombia', 'Comoros', 'Congo', 'Costa Rica', 'Croatia',
            'Cuba', 'Cyprus', 'Czech Republic', 'Denmark', 'Djibouti', 'Dominica', 'Dominican Republic',
            'Ecuador', 'Egypt', 'El Salvador', 'Equatorial Guinea', 'Eritrea', 'Estonia', 'Eswatini',
            'Ethiopia', 'Fiji', 'Finland', 'France', 'Gabon', 'Gambia', 'Georgia', 'Germany', 'Ghana',
            'Greece', 'Grenada', 'Guatemala', 'Guinea', 'Guinea-Bissau', 'Guyana', 'Haiti', 'Honduras',
            'Hungary', 'Iceland', 'India', 'Indonesia', 'Iran', 'Iraq', 'Ireland', 'Israel', 'Italy',
            'Jamaica', 'Japan', 'Jordan', 'Kazakhstan', 'Kenya', 'Kiribati', 'Kuwait', 'Kyrgyzstan',
            'Laos', 'Latvia', 'Lebanon', 'Lesotho', 'Liberia', 'Libya', 'Liechtenstein', 'Lithuania',
            'Luxembourg', 'Madagascar', 'Malawi', 'Malaysia', 'Maldives', 'Mali', 'Malta',
            'Marshall Islands', 'Mauritania', 'Mauritius', 'Mexico', 'Micronesia', 'Moldova',
            'Monaco', 'Mongolia', 'Montenegro', 'Morocco', 'Mozambique', 'Myanmar', 'Namibia',
            'Nauru', 'Nepal', 'Netherlands', 'New Zealand', 'Nicaragua', 'Niger', 'Nigeria',
            'North Macedonia', 'Norway', 'Oman', 'Pakistan', 'Palau', 'Panama', 'Papua New Guinea',
            'Paraguay', 'Peru', 'Philippines', 'Poland', 'Portugal', 'Qatar', 'Romania', 'Russia',
            'Rwanda', 'Saint Kitts and Nevis', 'Saint Lucia', 'Saint Vincent and the Grenadines',
            'Samoa', 'San Marino', 'Sao Tome and Principe', 'Saudi Arabia', 'Senegal', 'Serbia',
            'Seychelles', 'Sierra Leone', 'Singapore', 'Slovakia', 'Slovenia', 'Solomon Islands',
            'Somalia', 'South Africa', 'South Sudan', 'Spain', 'Sri Lanka', 'Sudan', 'Suriname',
            'Sweden', 'Switzerland', 'Syria', 'Tajikistan', 'Tanzania', 'Thailand', 'Timor-Leste',
            'Togo', 'Tonga', 'Trinidad and Tobago', 'Tunisia', 'Turkey', 'Turkmenistan', 'Tuvalu',
            'Uganda', 'Ukraine', 'United Arab Emirates', 'United Kingdom', 'United States',
            'Uruguay', 'Uzbekistan', 'Vanuatu', 'Vatican City', 'Venezuela', 'Vietnam', 'Yemen',
            'Zambia', 'Zimbabwe',
            
            # Additional recognized territories
            'Palestine', 'Kosovo', 'Taiwan', 'Western Sahara',
            
            # Dependent territories
            'American Samoa', 'Anguilla', 'Aruba', 'Bermuda', 'British Virgin Islands',
            'Cayman Islands', 'Cook Islands', 'Curacao', 'Falkland Islands', 'French Guiana',
            'French Polynesia', 'Gibraltar', 'Greenland', 'Guadeloupe', 'Guam', 'Guernsey',
            'Hong Kong', 'Isle of Man', 'Jersey', 'Macau', 'Martinique', 'Mayotte', 'Montserrat',
            'New Caledonia', 'Niue', 'Norfolk Island', 'Northern Mariana Islands', 'Puerto Rico',
            'Reunion', 'Saint Barthelemy', 'Saint Helena', 'Saint Martin', 'Saint Pierre and Miquelon',
            'Sint Maarten', 'Tokelau', 'Turks and Caicos Islands', 'US Virgin Islands', 'Wallis and Futuna',
            
            # Special cases
            'England', 'Scotland', 'Wales', 'Northern Ireland', 'Catalonia', 'Quebec',
            'Zanzibar', 'Crimea', 'Abkhazia', 'South Ossetia', 'Northern Cyprus',
            'Transnistria', 'Nagorno-Karabakh', 'Somaliland'
        ]

    def create_global_airline_registry(self):
        """Create comprehensive airline registry covering every country"""
        return {
            # North America
            'UAL': 'United States', 'DAL': 'United States', 'AAL': 'United States', 
            'SWA': 'United States', 'ACA': 'Canada', 'WJA': 'Canada',
            'AMX': 'Mexico', 'VOI': 'Mexico', 'CWC': 'Caribbean',
            
            # Central America & Caribbean
            'CAA': 'Costa Rica', 'NXA': 'Nicaragua', 'PXA': 'Panama',
            'TUA': 'El Salvador', 'GUA': 'Guatemala', 'HXA': 'Honduras',
            'BWA': 'Bahamas', 'CUB': 'Cuba', 'JMA': 'Jamaica',
            'DOM': 'Dominican Republic', 'HTA': 'Haiti', 'TCA': 'Turks and Caicos',
            
            # South America
            'ARG': 'Argentina', 'GLO': 'Brazil', 'LAN': 'Chile', 
            'AVA': 'Colombia', 'TPU': 'Peru', 'SLA': 'Bolivia',
            'GEA': 'Ecuador', 'RLA': 'Uruguay', 'VVA': 'Venezuela',
            'PXA': 'Paraguay', 'GYA': 'Guyana', 'SRA': 'Suriname',
            
            # Europe - Western
            'AFR': 'France', 'BAW': 'United Kingdom', 'DLH': 'Germany',
            'IBE': 'Spain', 'ITY': 'Italy', 'KLM': 'Netherlands',
            'BEL': 'Belgium', 'AEA': 'Austria', 'SAS': 'Sweden',
            'FIN': 'Finland', 'NAX': 'Norway', 'ICE': 'Iceland',
            
            # Europe - Eastern
            'AFL': 'Russia', 'BEL': 'Belarus', 'UKR': 'Ukraine',
            'LOT': 'Poland', 'CSA': 'Czech Republic', 'WZZ': 'Hungary',
            'TAR': 'Romania', 'BGL': 'Bulgaria', 'SAA': 'Serbia',
            'CTN': 'Croatia', 'SLA': 'Slovenia', 'SKK': 'Slovakia',
            'EST': 'Estonia', 'LTL': 'Lithuania', 'LGL': 'Latvia',
            
            # Europe - Balkan & Mediterranean
            'EJA': 'Albania', 'BMS': 'Bosnia and Herzegovina', 'MGX': 'Montenegro',
            'MAK': 'North Macedonia', 'GEC': 'Greece', 'CYP': 'Cyprus',
            'MLD': 'Moldova', 'MAL': 'Malta', 'PTR': 'Portugal',
            
            # Middle East
            'UAE': 'United Arab Emirates', 'QTR': 'Qatar', 'SVA': 'Saudi Arabia',
            'IRM': 'Iran', 'RYA': 'Jordan', 'KAC': 'Kuwait',
            'MEA': 'Lebanon', 'OMA': 'Oman', 'SYR': 'Syria',
            'YEM': 'Yemen', 'BAH': 'Bahrain', 'ISR': 'Israel',
            
            # Asia - East
            'CCA': 'China', 'CSN': 'China', 'CES': 'China',
            'ANA': 'Japan', 'JAL': 'Japan', 'KAL': 'South Korea',
            'ASV': 'North Korea', 'EVA': 'Taiwan', 'CPA': 'Hong Kong',
            'AMU': 'Macau', 'MAS': 'Mongolia',
            
            # Asia - Southeast
            'SIA': 'Singapore', 'MAS': 'Malaysia', 'THA': 'Thailand',
            'VNA': 'Vietnam', 'PAL': 'Philippines', 'GIA': 'Indonesia',
            'RBA': 'Brunei', 'CPA': 'Cambodia', 'LAO': 'Laos',
            'MYA': 'Myanmar', 'TAP': 'Timor-Leste',
            
            # Asia - South
            'AIC': 'India', 'PIA': 'Pakistan', 'BDA': 'Bangladesh',
            'ALK': 'Sri Lanka', 'NMA': 'Nepal', 'BHT': 'Bhutan',
            'MLE': 'Maldives', 'AFG': 'Afghanistan',
            
            # Africa - North
            'EMA': 'Egypt', 'RAM': 'Morocco', 'TAR': 'Algeria',
            'LOT': 'Libya', 'TUN': 'Tunisia', 'MSR': 'Sudan',
            
            # Africa - West
            'NGA': 'Nigeria', 'GHA': 'Ghana', 'SEN': 'Senegal',
            'CIV': 'Ivory Coast', 'MLI': 'Mali', 'NIA': 'Niger',
            'BFA': 'Burkina Faso', 'BEN': 'Benin', 'TGO': 'Togo',
            'GMB': 'Gambia', 'GNB': 'Guinea-Bissau', 'SLE': 'Sierra Leone',
            'LBR': 'Liberia', 'CPV': 'Cabo Verde',
            
            # Africa - Central
            'CRL': 'Democratic Republic of the Congo', 'RAC': 'Republic of the Congo',
            'AGC': 'Angola', 'CRG': 'Gabon', 'CAM': 'Cameroon',
            'CAR': 'Central African Republic', 'TCD': 'Chad', 'GNQ': 'Equatorial Guinea',
            'SSA': 'South Sudan', 'RWA': 'Rwanda', 'BDI': 'Burundi',
            
            # Africa - East
            'ETH': 'Ethiopia', 'KQA': 'Kenya', 'TAN': 'Tanzania',
            'UGA': 'Uganda', 'MDG': 'Madagascar', 'SEY': 'Seychelles',
            'MAU': 'Mauritius', 'COM': 'Comoros', 'DJI': 'Djibouti',
            'ERT': 'Eritrea', 'SOM': 'Somalia',
            
            # Africa - South
            'SAA': 'South Africa', 'BOT': 'Botswana', 'NAM': 'Namibia',
            'ZIM': 'Zimbabwe', 'ZAM': 'Zambia', 'MAW': 'Malawi',
            'MOS': 'Mozambique', 'SWZ': 'Eswatini', 'LES': 'Lesotho',
            
            # Oceania
            'QFA': 'Australia', 'ANZ': 'New Zealand', 'FJI': 'Fiji',
            'PUA': 'Papua New Guinea', 'SOL': 'Solomon Islands', 'VAN': 'Vanuatu',
            'SAM': 'Samoa', 'TON': 'Tonga', 'KIR': 'Kiribati',
            'MIC': 'Micronesia', 'MAR': 'Marshall Islands', 'PAL': 'Palau',
            'NAU': 'Nauru', 'TUV': 'Tuvalu',
            
            # Regional and low-cost carriers
            'RYR': 'Ireland', 'EZY': 'United Kingdom', 'EIN': 'Ireland',
            'WZZ': 'Hungary', 'PGT': 'Turkey', 'BER': 'Germany',
            'TAP': 'Portugal', 'AZU': 'Italy', 'ISS': 'Iceland',
            'VIR': 'United Kingdom', 'JST': 'United Kingdom', 'EXS': 'United Kingdom'
        }

    def get_country_from_callsign(self, callsign):
        """Extract country from airline prefix with fallback to random country"""
        if callsign and len(callsign) >= 3:
            prefix = callsign[:3]
            return self.airline_prefixes.get(prefix, random.choice(self.all_countries))
        return random.choice(self.all_countries)

    def get_country_coordinates(self, country):
        """Get approximate coordinates for countries"""
        country_coordinates = {
            # Major countries with approximate centers
            'United States': (39.8283, -98.5795), 'Canada': (56.1304, -106.3468),
            'Brazil': (-14.2350, -51.9253), 'Russia': (61.5240, 105.3188),
            'China': (35.8617, 104.1954), 'India': (20.5937, 78.9629),
            'Australia': (-25.2744, 133.7751), 'Germany': (51.1657, 10.4515),
            'France': (46.2276, 2.2137), 'United Kingdom': (55.3781, -3.4360),
            'Japan': (36.2048, 138.2529), 'South Africa': (-30.5595, 22.9375),
            'Mexico': (23.6345, -102.5528), 'Egypt': (26.8206, 30.8025),
        }
        
        if country in country_coordinates:
            base_lat, base_lon = country_coordinates[country]
            # Add some randomness around the country center
            return (
                base_lat + random.uniform(-10, 10),
                base_lon + random.uniform(-15, 15)
            )
        else:
            # Random global distribution for other countries
            return (
                random.uniform(-55, 70),
                random.uniform(-180, 180)
            )

    def calculate_environmental_impact(self, aircraft_type, altitude, velocity, fuel_consumption, latitude, longitude):
        profile = self.aircraft_environment_profiles.get(aircraft_type, {})
        
        co2_impact = (fuel_consumption * 3.16) / 2000
        base_noise = profile.get('noise_db', 85)
        altitude_factor = max(0.3, 1 - (altitude / 40000))
        population_factor = self.get_population_density_factor(latitude, longitude)
        noise_impact = base_noise * altitude_factor * population_factor
        air_quality_impact = (fuel_consumption / 1500) * (1 - (altitude / 50000)) * population_factor
        
        environmental_score = (
            (co2_impact * 0.4) + 
            ((noise_impact / 120) * 0.3) + 
            (air_quality_impact * 0.3)
        ) * 25
        
        env_rating = self.get_environmental_rating(environmental_score)
        
        return {
            'environmental_impact_score': min(100, max(0, environmental_score)),
            'noise_pollution_level': round(noise_impact, 2),
            'air_quality_index': round(max(0, min(500, air_quality_impact * 150)), 2),
            'environmental_rating': env_rating,
            'population_density_factor': round(population_factor, 2)
        }

    def get_population_density_factor(self, latitude, longitude):
        """Enhanced population density factors for major global cities"""
        population_centers = [
            # North America
            {'lat': 40.7128, 'lon': -74.0060, 'factor': 1.5},  # New York
            {'lat': 34.0522, 'lon': -118.2437, 'factor': 1.4}, # Los Angeles
            {'lat': 41.8781, 'lon': -87.6298, 'factor': 1.4},  # Chicago
            {'lat': 43.6532, 'lon': -79.3832, 'factor': 1.3},  # Toronto
            {'lat': 19.4326, 'lon': -99.1332, 'factor': 1.6},  # Mexico City
            # Europe
            {'lat': 51.5074, 'lon': -0.1278, 'factor': 1.4},   # London
            {'lat': 48.8566, 'lon': 2.3522, 'factor': 1.3},    # Paris
            {'lat': 52.5200, 'lon': 13.4050, 'factor': 1.3},   # Berlin
            {'lat': 55.7558, 'lon': 37.6173, 'factor': 1.4},   # Moscow
            {'lat': 41.9028, 'lon': 12.4964, 'factor': 1.3},   # Rome
            # Asia
            {'lat': 35.6762, 'lon': 139.6503, 'factor': 1.6},  # Tokyo
            {'lat': 31.2304, 'lon': 121.4737, 'factor': 1.5},  # Shanghai
            {'lat': 28.6139, 'lon': 77.2090, 'factor': 1.6},   # Delhi
            {'lat': 1.3521, 'lon': 103.8198, 'factor': 1.4},   # Singapore
            {'lat': 13.7563, 'lon': 100.5018, 'factor': 1.4},  # Bangkok
            # Middle East
            {'lat': 25.2048, 'lon': 55.2708, 'factor': 1.3},   # Dubai
            {'lat': 24.7136, 'lon': 46.6753, 'factor': 1.3},   # Riyadh
            # Africa
            {'lat': -26.2041, 'lon': 28.0473, 'factor': 1.3},  # Johannesburg
            {'lat': 30.0444, 'lon': 31.2357, 'factor': 1.4},   # Cairo
            {'lat': 6.5244, 'lon': 3.3792, 'factor': 1.4},     # Lagos
            # South America
            {'lat': -23.5505, 'lon': -46.6333, 'factor': 1.5}, # Sao Paulo
            {'lat': -34.6037, 'lon': -58.3816, 'factor': 1.4}, # Buenos Aires
            # Oceania
            {'lat': -33.8688, 'lon': 151.2093, 'factor': 1.3}, # Sydney
        ]
        
        min_distance = float('inf')
        base_factor = 1.0
        
        for center in population_centers:
            distance = ((latitude - center['lat'])**2 + (longitude - center['lon'])**2)**0.5
            if distance < min_distance:
                min_distance = distance
                if distance < 10:
                    base_factor = center['factor']
        
        return base_factor

    def get_environmental_rating(self, score):
        if score <= 25: return 'A+'
        elif score <= 35: return 'A'
        elif score <= 50: return 'B'
        elif score <= 65: return 'C'
        elif score <= 80: return 'D'
        else: return 'F'

    def get_aircraft_type(self, callsign):
        if callsign and len(callsign) >= 3:
            for code, aircraft in self.aircraft_type_map.items():
                if code in callsign:
                    return aircraft
        return random.choice(list(self.aircraft_environment_profiles.keys()))

    def get_real_flight_data(self):
        try:
            response = requests.get("https://opensky-network.org/api/states/all", timeout=15)
            if response.status_code == 200:
                data = response.json()
                states = data.get('states', [])
                return self.process_flight_data(states)
            else:
                return self.generate_complete_global_data()
        except:
            return self.generate_complete_global_data()

    def process_flight_data(self, states):
        flights = []
        for state in states:
            if len(state) >= 17:
                try:
                    icao24 = state[0] or f"UNK{random.randint(1000, 9999)}"
                    callsign = (state[1] or f"FLT{random.randint(1000, 9999)}").strip()
                    origin_country = state[2] or self.get_country_from_callsign(callsign)
                    longitude = state[5] or random.uniform(-180, 180)
                    latitude = state[6] or random.uniform(-90, 90)
                    baro_altitude = state[7] or random.uniform(5000, 40000)
                    velocity = state[9] or random.uniform(200, 900)
                    geo_altitude = state[13] or baro_altitude
                    
                    aircraft_type = self.get_aircraft_type(callsign)
                    fuel_consumption, co2_emissions = self.calculate_aviation_emissions(aircraft_type, velocity, geo_altitude)
                    env_impact = self.calculate_environmental_impact(aircraft_type, geo_altitude, velocity, fuel_consumption, latitude, longitude)
                    
                    flight = {
                        'icao24': icao24,
                        'callsign': callsign,
                        'origin_country': origin_country,
                        'longitude': round(float(longitude), 6),
                        'latitude': round(float(latitude), 6),
                        'baro_altitude': round(float(baro_altitude), 2),
                        'velocity': round(float(velocity), 2),
                        'vertical_rate': state[11] or round(random.uniform(-50, 50), 2),
                        'geo_altitude': round(float(geo_altitude), 2),
                        'aircraft_type': aircraft_type,
                        'fuel_consumption_kg_hour': round(fuel_consumption, 2),
                        'co2_emissions_kg_hour': round(co2_emissions, 2),
                        'emission_intensity_kg_km': round(co2_emissions / (velocity * 3.6) if velocity > 0 else 0.15, 4),
                        'efficiency_score': round(random.uniform(70, 95), 2),
                        'predicted_co2_next_hour': round(co2_emissions * random.uniform(0.95, 1.05), 2),
                        **env_impact,
                        'timestamp': datetime.utcnow().isoformat(),
                        'data_source': 'opensky_realtime'
                    }
                    
                    if flight['longitude'] and flight['latitude']:
                        flights.append(flight)
                except Exception as e:
                    continue
        return flights

    def calculate_aviation_emissions(self, aircraft_type, velocity, altitude):
        base_fuel_consumption = {
            'A320': random.uniform(2200, 2800), 'A320neo': random.uniform(1800, 2300),
            'B737-800': random.uniform(2400, 3000), 'B737-MAX8': random.uniform(1900, 2400),
            'A321': random.uniform(2500, 3200), 'B787-9': random.uniform(5200, 5900),
            'A350-900': random.uniform(4800, 5500), 'E190': random.uniform(1800, 2300),
            'A380': random.uniform(11000, 13000), 'B777': random.uniform(7500, 9000),
            'CRJ900': random.uniform(1200, 1600), 'ATR72': random.uniform(800, 1100),
            'A319': random.uniform(2000, 2600), 'A330': random.uniform(6000, 7500),
            'B747': random.uniform(12000, 15000), 'B767': random.uniform(5000, 6500),
            'ERJ175': random.uniform(1500, 2000), 'DHC8': random.uniform(600, 900),
        }
        
        base_fuel = base_fuel_consumption.get(aircraft_type, random.uniform(2000, 7000))
        altitude_factor = 0.8 + (0.4 * (altitude / 40000))
        speed_factor = 1.2 if velocity < 500 else 1.15 if velocity > 950 else 1.0
        final_fuel = base_fuel * altitude_factor * speed_factor
        co2_emissions = final_fuel * 3.16
        
        return final_fuel, co2_emissions

    def generate_complete_global_data(self):
        """Generate flight data covering ALL countries and territories"""
        aircraft_types = list(self.aircraft_environment_profiles.keys())
        flights = []
        
        # Sample from all countries (limit to reasonable number for performance)
        countries_sample = random.sample(self.all_countries, min(80, len(self.all_countries)))
        
        for country in countries_sample:
            # Generate 1-4 flights per country
            for i in range(random.randint(1, 4)):
                aircraft = random.choice(aircraft_types)
                lat, lon = self.get_country_coordinates(country)
                
                altitude = random.uniform(8000, 40000)
                velocity = random.uniform(600, 950)
                fuel, co2 = self.calculate_aviation_emissions(aircraft, velocity, altitude)
                
                env_impact = self.calculate_environmental_impact(aircraft, altitude, velocity, fuel, lat, lon)
                
                flight = {
                    'icao24': f"GL{random.randint(10000, 99999)}",
                    'callsign': f"{random.choice(list(self.airline_prefixes.keys()))}{random.randint(100, 999)}",
                    'origin_country': country,
                    'longitude': round(lon, 6),
                    'latitude': round(lat, 6),
                    'baro_altitude': round(altitude, 2),
                    'velocity': round(velocity, 2),
                    'vertical_rate': round(random.uniform(-50, 50), 2),
                    'geo_altitude': round(altitude, 2),
                    'aircraft_type': aircraft,
                    'fuel_consumption_kg_hour': round(fuel, 2),
                    'co2_emissions_kg_hour': round(co2, 2),
                    'emission_intensity_kg_km': round(co2 / (velocity * 3.6), 4),
                    'efficiency_score': round(random.uniform(70, 95), 2),
                    'predicted_co2_next_hour': round(co2 * random.uniform(0.95, 1.05), 2),
                    **env_impact,
                    'timestamp': datetime.utcnow().isoformat(),
                    'data_source': 'synthetic_global_complete'
                }
                flights.append(flight)
        
        # Log comprehensive coverage
        countries_covered = set(f['origin_country'] for f in flights)
        logger.info(f"🌍 Generated {len(flights)} flights from {len(countries_covered)} countries/territories")
        
        return flights

    def send_aviation_data(self):
        try:
            aviation_data = self.get_real_flight_data()
            for flight in aviation_data:
                self.producer.send('aviation-environment-data', value=flight)
                logger.info(f"✈️ {flight['callsign']} - {flight['origin_country']} - {flight['aircraft_type']} - Rating: {flight['environmental_rating']}")

            self.producer.flush()
            
            # Comprehensive logging
            countries_covered = set(f['origin_country'] for f in aviation_data)
            logger.info(f"✅ Sent {len(aviation_data)} flights from {len(countries_covered)} countries/territories")
            
            # Log sample of countries covered
            sample_countries = list(countries_covered)[:15]
            logger.info(f"🌐 Sample countries: {', '.join(sorted(sample_countries))}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error sending data: {e}")
            return False

    def run(self):
        logger.info(f"🌍 Starting Complete Global Environmental Aviation Monitoring System")
        logger.info(f"📊 Monitoring: Aviation Emissions from {len(self.all_countries)} countries and territories")
        logger.info(f"🛫 Aircraft types: {len(self.aircraft_environment_profiles)}")
        logger.info(f"🏢 Airlines registered: {len(self.airline_prefixes)}")
        
        while True:
            try:
                success = self.send_aviation_data()
                time.sleep(30 if success else 60)
            except KeyboardInterrupt:
                logger.info("🛑 Global producer stopped by user")
                break
            except Exception as e:
                logger.error(f"💥 System error: {e}")
                time.sleep(60)

if __name__ == "__main__":
    producer = CompleteGlobalEnvironmentalProducer()
    producer.run()