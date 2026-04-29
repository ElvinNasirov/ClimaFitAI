# weather_logic.py

def get_activity_recommendation(weather_data, city):
    """
    weather_data: dict with keys:
        - temperature_2m_max (°C)
        - precipitation_sum (mm)
        - wind_speed_10m_max (m/s)
        - apparent_temperature_max (°C)
        - relative_humidity_2m_mean (%)
        - weather_code (int)

    city: string

    returns: dict
    """

    try:
        temp = weather_data["temperature_2m_max"]
        feels_like = weather_data["apparent_temperature_max"]
        rain = weather_data["precipitation_sum"]
        wind = weather_data["wind_speed_10m_max"]
        humidity = weather_data["relative_humidity_2m_mean"]
        code = weather_data["weather_code"]
    except KeyError:
        return {"status": "error", "message": "Missing required weather fields"}

    # -----------------------------
    # WEATHER LOGIC (SMART RULES)
    # -----------------------------

    # Rain or bad weather
    if rain > 3 or code >= 60:
        activity_type = "indoor"
        reason = "Rainy or unstable weather"

    # Strong wind
    elif wind > 12:
        activity_type = "indoor"
        reason = "Too windy for outdoor activities"

    # Very hot / uncomfortable
    elif feels_like > 33 or (temp > 30 and humidity > 70):
        activity_type = "hot"
        reason = "Too hot and humid"

    # Perfect conditions
    elif 20 <= feels_like <= 30 and rain < 1 and wind < 8:
        activity_type = "perfect"
        reason = "Ideal weather"

    # Cooler weather
    elif feels_like < 20:
        activity_type = "cool"
        reason = "Cool weather"

    else:
        activity_type = "mixed"
        reason = "Moderate conditions"

    # -----------------------------
    # CITY-SPECIFIC SUGGESTIONS
    # -----------------------------
    suggestions = get_city_suggestions(city, activity_type)

    return {
        "status": "success",
        "city": city,
        "activity_type": activity_type,
        "reason": reason,
        "suggestions": suggestions
    }


# -----------------------------
# CITY LOGIC
# -----------------------------
def get_city_suggestions(city, activity_type):
    city = city.lower()

    data = {
        "baku": {
            "indoor": ["Museums", "Shopping malls", "Cafes"],
            "hot": ["Caspian beach", "Pool", "Water parks"],
            "perfect": ["Boulevard walk", "Old city tour"],
            "cool": ["Cafe hopping", "Light прогулка"],
            "mixed": ["Short outdoor walks + cafes"]
        },

        "guba": {
            "indoor": ["Hotels", "Rest"],
            "hot": ["Mountain resorts"],
            "perfect": ["Hiking", "Nature trips"],
            "cool": ["Forest walks"],
            "mixed": ["Short hikes"]
        },

        "lankaran": {
            "indoor": ["Hotels", "Restaurants"],
            "hot": ["Beach", "Swimming"],
            "perfect": ["Nature exploration"],
            "cool": ["Light прогулка"],
            "mixed": ["Mixed outdoor + rest"]
        },

        "shaki": {
            "indoor": ["Sheki Khan Palace", "Museums"],
            "hot": ["Garden walks"],
            "perfect": ["City exploration"],
            "cool": ["Tea houses"],
            "mixed": ["Short visits"]
        },

        "gabala": {
            "indoor": ["Spa", "Resorts"],
            "hot": ["Pools"],
            "perfect": ["Cable car", "Hiking"],
            "cool": ["Forest walks"],
            "mixed": ["Nature + rest"]
        }
    }

    if city not in data:
        return ["General sightseeing", "Cafes"]

    return data[city].get(activity_type, ["General activities"])


if __name__ == "__main__":
    sample_weather = {
        "temperature_2m_max": 31,
        "precipitation_sum": 0,
        "wind_speed_10m_max": 5,
        "apparent_temperature_max": 34,
        "relative_humidity_2m_mean": 75,
        "weather_code": 1
    }

    result = get_activity_recommendation(sample_weather, "Lankaran")
    print(result)