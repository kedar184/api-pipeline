from datetime import datetime, timedelta, UTC
from typing import Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Query, Header
from pydantic import BaseModel
import uvicorn
import random
from loguru import logger

app = FastAPI(title="Mock OpenWeatherMap API")

def validate_api_key(appid: Optional[str] = Query(None), authorization: Optional[str] = Header(None)):
    """Validate API key from either query param or header"""
    valid_key = "mock_api_key"
    
    logger.info(f"Validating API key - Query param: {appid}, Authorization header: {authorization}")
    
    # Check query parameter first (OpenWeatherMap style)
    if appid == valid_key:
        logger.info("API key valid (from query parameter)")
        return True
        
    # Check Authorization header (our framework style)
    if authorization:
        # Try different formats
        if authorization.startswith("ApiKey "):
            key = authorization.split(" ")[1]
            if key == valid_key:
                logger.info("API key valid (from Authorization header with prefix)")
                return True
        elif authorization == valid_key:
            logger.info("API key valid (from direct header)")
            return True
            
    logger.error(f"Invalid API key - appid: {appid}, auth: {authorization}")
    raise HTTPException(status_code=401, detail="Invalid API key")

# Mock data generators
def generate_weather_data(lat: float, lon: float, temp_celsius: Optional[float] = None, units: str = "standard") -> Dict[str, Any]:
    """Generate mock weather data"""
    # Generate or use provided temperature in Celsius
    if temp_celsius is not None:
        temp_c = temp_celsius
    else:
        temp_c = 15.0 if lat > 45 else 30.0  # Use latitude to determine temperature
    
    # Convert temperature based on units
    if units == "standard":
        temp = temp_c + 273.15  # Convert to Kelvin
    elif units == "metric":
        temp = temp_c  # Keep as Celsius
    else:  # imperial
        temp = (temp_c * 9/5) + 32  # Convert to Fahrenheit
    
    return {
        "coord": {"lat": lat, "lon": lon},
        "weather": [{
            "id": random.randint(200, 800),
            "main": "Clear" if temp_c > 20 else "Cloudy",
            "description": "clear sky" if temp_c > 20 else "cloudy"
        }],
        "main": {
            "temp": temp,
            "feels_like": temp - 2,
            "pressure": random.randint(1000, 1020),
            "humidity": random.randint(30, 90)
        },
        "wind": {
            "speed": random.uniform(0, 10),
            "deg": random.randint(0, 360)
        },
        "name": "MockCity",
        "sys": {"country": "MC"}
    }

def generate_forecast_data(lat: float, lon: float, units: str = "standard") -> Dict[str, Any]:
    """Generate mock forecast data"""
    base_time = datetime.now(UTC)
    forecasts = []
    
    for i in range(5):  # 5 forecast periods
        time = base_time + timedelta(hours=i*3)
        temp_c = random.uniform(0, 30)  # Random temperature in Celsius
        
        # Convert temperature based on units
        if units == "standard":
            temp = temp_c + 273.15  # Convert to Kelvin
        elif units == "metric":
            temp = temp_c  # Keep as Celsius
        else:  # imperial
            temp = (temp_c * 9/5) + 32  # Convert to Fahrenheit
        
        forecasts.append({
            "dt": int(time.timestamp()),
            "dt_txt": time.isoformat(),
            "main": {
                "temp": temp,
                "feels_like": temp - 2,
                "pressure": random.randint(1000, 1020),
                "humidity": random.randint(30, 90)
            },
            "weather": [{
                "id": random.randint(200, 800),
                "main": "Clear" if temp_c > 20 else "Cloudy",
                "description": "clear sky" if temp_c > 20 else "cloudy"
            }],
            "wind": {
                "speed": random.uniform(0, 10),
                "deg": random.randint(0, 360)
            }
        })
    
    return {
        "city": {
            "name": "MockCity",
            "country": "MC",
            "coord": {"lat": lat, "lon": lon}
        },
        "list": forecasts
    }

def generate_air_quality_data(lat: float, lon: float) -> Dict[str, Any]:
    """Generate mock air quality data"""
    return {
        "coord": {"lat": lat, "lon": lon},
        "list": [{
            "dt": int(datetime.now(UTC).timestamp()),
            "main": {
                "aqi": random.randint(1, 5)  # Air Quality Index 1-5
            },
            "components": {
                "co": random.uniform(200, 300),
                "no": random.uniform(10, 20),
                "no2": random.uniform(20, 30),
                "o3": random.uniform(30, 40),
                "so2": random.uniform(5, 15),
                "pm2_5": random.uniform(10, 20),
                "pm10": random.uniform(20, 30),
                "nh3": random.uniform(5, 10)
            }
        }]
    }

# API endpoints
@app.get("/data/2.5/weather")
async def get_current_weather(
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
    units: str = Query("standard", description="Units (standard, metric, imperial)"),
    appid: Optional[str] = Query(None, description="API Key"),
    authorization: Optional[str] = Header(None)
):
    """Mock current weather endpoint"""
    logger.info(f"Current weather request - lat: {lat}, lon: {lon}, units: {units}")
    logger.info(f"Auth - appid: {appid}, authorization: {authorization}")
    
    try:
        validate_api_key(appid, authorization)
        
        # For testing temperature threshold, use latitude to determine temperature
        # Locations above 45° latitude will have low temperatures
        temp_celsius = 15.0 if lat > 45 else 30.0
        result = generate_weather_data(lat, lon, temp_celsius, units)
        logger.info(f"Generated weather data: {result}")
        return result
    except Exception as e:
        logger.error(f"Error in current weather endpoint: {str(e)}")
        raise

@app.get("/data/2.5/forecast")
async def get_forecast(
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
    units: str = Query("standard", description="Units (standard, metric, imperial)"),
    appid: Optional[str] = Query(None, description="API Key"),
    authorization: Optional[str] = Header(None)
):
    """Mock forecast endpoint"""
    logger.info(f"Forecast request - lat: {lat}, lon: {lon}, units: {units}")
    logger.info(f"Auth - appid: {appid}, authorization: {authorization}")
    
    try:
        validate_api_key(appid, authorization)
        result = generate_forecast_data(lat, lon, units)
        logger.info(f"Generated forecast data: {result}")
        return result
    except Exception as e:
        logger.error(f"Error in forecast endpoint: {str(e)}")
        raise

@app.get("/data/2.5/air_pollution")
async def get_air_quality(
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
    appid: Optional[str] = Query(None, description="API Key"),
    authorization: Optional[str] = Header(None)
):
    """Mock air quality endpoint"""
    logger.info(f"Air quality request - lat: {lat}, lon: {lon}")
    logger.info(f"Auth - appid: {appid}, authorization: {authorization}")
    
    try:
        validate_api_key(appid, authorization)
        result = generate_air_quality_data(lat, lon)
        logger.info(f"Generated air quality data: {result}")
        return result
    except Exception as e:
        logger.error(f"Error in air quality endpoint: {str(e)}")
        raise

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000) 