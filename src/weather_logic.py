# weather_logic.py

def get_activity_recommendation(weather_data, city):
    """
    weather_data: dict with keys:
        - temperature_2m_max (°C)
        - precipitation_sum (mm)
        - wind_speed_10m_max (m/s)
        - apparent_temperature_max (°C)
        - relative_humidity_2m_mean (%)
        - cloud_cover_mean (%)
        - sunshine_duration (sec)

    city: string

    returns: dict
    """

    try:
        temp = weather_data["temperature_2m_max"]
        feels_like = weather_data["apparent_temperature_max"]
        rain = weather_data["precipitation_sum"]
        wind = weather_data["wind_speed_10m_max"]
        humidity = weather_data["relative_humidity_2m_mean"]
        cloud = weather_data["cloud_cover_mean"]
        sun = weather_data["sunshine_duration"]
    except KeyError:
        return {"status": "error", "message": "Missing required weather fields"}

    
    # WEATHER LOGIC 
    
    # Rain or bad weather
    if rain > 31 or cloud > 80:
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
    elif 20 <= feels_like <= 30 and rain < 1 and wind < 8 and sun > 20000:
        activity_type = "perfect"
        reason = "Sunny and comfortable"

     # Cloudy but okay
    elif cloud > 50:
        activity_type = "mixed"
        reason = "Cloudy but manageable"

    # Cooler weather
    elif feels_like < 20:
        activity_type = "cool"
        reason = "Cool weather"

    else:
        activity_type = "mixed"
        reason = "Moderate conditions"

    # CITY-SPECIFIC SUGGESTIONS
    
    suggestions = get_city_suggestions(city, activity_type)

    return {
        "status": "success",
        "city": city,
        "activity_type": activity_type,
        "reason": reason,
        "suggestions": suggestions
    }

# CITY LOGIC

def get_city_suggestions(city, activity_type):
    city = city.lower()

    data = {
        "baku": {
            "indoor": [
                "Visiting the Heydar Aliyev Center",
                "Exploring museums in Icherisheher (Old City)",
                "Relaxing in cafes or Malls"
            ],
            "hot": [
                "Spending time at Caspian Sea beaches (Bilgah, Mardakan)",
                "Visiting water parks or hotel pools",
                "Enjoying evening walks at Baku Boulevard"
            ],
            "perfect": [
                "A Walk along Baku Boulevard",
                "Exploring Icherisheher and Maiden Tower",
                "Visiting Flame Towers viewpoint"
            ],
            "cool": [
                "Enjoying cozy cafes in Old City",
                "Walking along the seaside boulevard",
                "Visiting art galleries and museums"
            ],
            "mixed": [
                "Exploring Icherisheher with cafe breaks",
                "A Walk along Baku Boulevard for short periods",
                "Visiting museums between outdoor stops"
            ]
        },


       "guba": {
            "indoor": [
                "Relaxing at mountain resorts or hotels",
                "Enjoying spa or wellness centers",
                "Trying local restaurants"
            ],
            "hot": [
                "Visiting cooler mountain areas like Khinalig",
                "Relaxing near rivers or forest areas",
                "Staying in shaded resort areas"
            ],
            "perfect": [
                "A Hike in forests around Guba",
                "A Visit to Afurdja Waterfall",
                "Exploring mountain villages like Khinalig"
            ],
            "cool": [
                "Taking scenic forest walks",
                "A Visit to local villages and markets",
                "Enjoying tea with mountain views"
            ],
            "mixed": [
                "Visiting Afurdja Waterfall briefly",
                "Taking short forest walks with breaks",
                "Relaxing in mountain hotels between outings"
            ]
        },

        "lankaran": {
            "indoor": [
                "Relaxing in hotels and spas",
                "A Visit to local restaurants and tea houses",
                "Exploring indoor cultural centers"
            ],
            "hot": [
                "Spending time at Lankaran beaches",
                "Swimming in the Caspian Sea",
                "Relaxing in shaded parks"
            ],
            "perfect": [
                "Exploring Hirkan National Park",
                "A Walk along Lankaran coast",
                "Visiting tea plantations"
            ],
            "cool": [
                "Taking light walks in nature parks",
                "Visiting tea houses",
                "Exploring local markets"
            ],
            "mixed": [
                "Visiting Hirkan National Park for short walks",
                "A Stop by tea houses between outdoor time",
                "Spending limited time at the beach"
            ]
        },
        "shaki": {
            "indoor": [
                "Visiting Sheki Khan Palace",
                "Exploring Sheki History Museum",
                "Relaxing in traditional tea houses"
            ],
            "hot": [
                "A Walk in shaded palace gardens",
                "Exploring old streets in the morning/evening",
                "Relaxing in cool indoor spaces midday"
            ],
            "perfect": [
                "Exploring Sheki Khan Palace",
                "A Walk through old town streets",
                "A Visit to local handicraft shops"
            ],
            "cool": [
                "Enjoying tea houses with mountain views",
                "Taking relaxed city walks",
                "Visiting historical sites comfortably"
            ],
            "mixed": [
                "Visiting Sheki Khan Palace",
                "Exploring museums nearby",
                "Taking short walks in old town between breaks"
            ]
        },

        "gabala": {
            "indoor": [
                "Relaxing at spa resorts",
                "Visiting entertainment centers",
                "Enjoying hotel facilities"
            ],
            "hot": [
                "Swimming in hotel pools",
                "Relaxing near Nohur Lake in shade",
                "Staying in cooler mountain areas"
            ],
            "perfect": [
                "Riding the Gabala cable car",
                "Visiting Nohur Lake",
                "Going hiking in the mountains"
            ],
            "cool": [
                "Forest walks",
                "Enjoying lake views",
                "Visiting nature parks"
            ],
            "mixed": [
                "Riding the cable car for short trips",
                "Visiting Nohur Lake briefly",
                "Alternating between outdoor spots and cafes"
            ]
        }
    }

    if city not in data:
        return ["General sightseeing", "Cafes"]

    return data[city].get(activity_type, ["General activities"])


def format_recommendation(result):
    """
    Converts logic output into human-friendly sentence
    """

    if result["status"] != "success":
        return "Sorry, something went wrong while generating your recommendation."

    city = result["city"]
    reason = result["reason"]
    suggestions = result["suggestions"]

    # Turn list into nice sentence
    if len(suggestions) == 1:
        suggestion_text = suggestions[0]
    else:
        suggestion_text = ", ".join(suggestions[:-1]) + " or " + suggestions[-1]

    # Final human message
    message = (
        f"In {city}, the weather looks {reason.lower()}.\n"
        f"We recommend activities like {suggestion_text}."
    )

    return message


