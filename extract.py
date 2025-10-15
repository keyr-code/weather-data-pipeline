import pandas as pd
import requests
import psycopg2
import logging

class WeatherETL:
    def __init__(self):
        self.base_url = "http://localhost:5000"
        self.db_conn = self.connect_to_postgres()

    def connect_to_postgres(self):
        """
        Connect to postgres
        """
        try:
            conn = psycopg2.connect(
                host="localhost",
                database="weather_data",
                user="root",
                password="root"
            )
            return conn
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def extract_weather_data(self, cities):
        weather_data = []
        try:
            for city in cities:
                url = f"{self.base_url}/weather/current/{city}"
                response = requests.get(url)
                if response.status_code == 200:
                    response_data = response.json()
                    print(response_data)
                    if 'data' in response_data:
                        data = response_data['data']
                        weather_data.append({
                            'city': data['location'],
                            'timestamp': data['timestamp'],
                            'temperature': data['temperature'],
                            'humidity': data['humidity'],
                            'condition': data['condition'],
                            'wind_speed': data['wind_speed']
                        })
                else:
                    print(f"Failed to fetch data for {city}. Status code: {response.status_code}")
        except Exception as e:
            print(f"An error occurred: {e}")

        return weather_data

    def transform_weather(self, weather_data_raw):
        """
        Validation and quality
        """
        if not weather_data_raw:
            print("No weather data to transform")
            return pd.DataFrame()
            
        df = pd.DataFrame(weather_data_raw)
        print(f"DataFrame columns: {df.columns.tolist()}")
        print(f"DataFrame shape: {df.shape}")
        
        if df.empty:
            print("DataFrame is empty")
            return df
            
        df = df.dropna()
        
        # Check if required columns exist before filtering
        required_cols = ['temperature', 'humidity', 'wind_speed', 'timestamp']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"Missing columns: {missing_cols}")
            return pd.DataFrame()
            
        # Validation
        df = df[df['temperature'].between(-20,45)]
        df = df[df['humidity'].between(0, 100)]
        df = df[df['wind_speed'].between(0, 100)]

        #Convert to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        return df

    def load_data(self,df):
        """
        Create bronze, silver and gold tables in postgres
        """
        #add db connection
        cursor = self.db_conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cities (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) UNIQUE
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                id SERIAL PRIMARY KEY,
                city_id INTEGER REFERENCES cities(id),
                timestamp TIMESTAMP,
                temperature FLOAT,
                humidity FLOAT,
                wind_speed FLOAT,
                weather_condition VARCHAR(255)
            )
        """)
        for city in df['city'].unique():
            cursor.execute("""
                INSERT INTO cities (name) VALUES (%s)
                ON CONFLICT (name) DO NOTHING
            """, (city,))

        for index , row in df.iterrows():
            cursor.execute("""
                            INSERT INTO weather_data (city_id, timestamp, temperature, humidity, wind_speed, weather_condition)
                            VALUES ((SELECT id FROM cities WHERE name = %s), %s, %s, %s, %s, %s)
                        """, (row['city'], row['timestamp'], row['temperature'],
                              row['humidity'], row['wind_speed'], row['condition']))

    def run_etl(self):
        #Get cities
        try:
            url = f"{self.base_url}/weather/locations"
            response = requests.get(url)
            cities = []
            if response.status_code == 200:
                data = response.json()
                cities = data['data']

            else:
                cities = ['New York', 'London', 'Tokyo', 'Sydney', 'Mumbai']
        except Exception as e:
            print(f"An error occurred: {e}")

        # Extract
        raw_data = self.extract_weather_data(cities)
        print(f"Extracted {len(raw_data)} records")
        if raw_data:
            print(f"Sample record: {raw_data[0]}")

        # Transform
        clean_data = self.transform_weather(raw_data)

        # Load
        self.load_data(clean_data)

        logging.info(f"Processed {len(clean_data)} weather records")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    etl = WeatherETL()
    etl.run_etl()



