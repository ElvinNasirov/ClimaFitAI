/**
 * WeatherCard Component
 *
 * Renders a weather summary card using the EXACT field names returned by
 * the FastAPI /forecast endpoint, which in turn match src/config.py DAILY_VARIABLES:
 *   - temperature_2m_max
 *   - precipitation_sum
 *   - wind_speed_10m_max
 *   - apparent_temperature_max
 *   - condition  (human-readable label derived in backend)
 *   - activity_type  (matches weather_logic.py categories: perfect/indoor/hot/cool/mixed)
 *   - reason  (explanation text from weather_logic.py)
 */

function getWeatherIcon(condition) {
    // condition values are derived using the same thresholds as weather_logic.py
    const icons = {
        'sunny': '<i class="fa-solid fa-sun"          style="color:#fbbf24;"></i>',
        'rainy': '<i class="fa-solid fa-cloud-rain"   style="color:#60a5fa;"></i>',
        'cloudy': '<i class="fa-solid fa-cloud"        style="color:#94a3b8;"></i>',
        'cool': '<i class="fa-solid fa-cloud-sun"    style="color:#7dd3fc;"></i>',
        'windy': '<i class="fa-solid fa-wind"         style="color:#94a3b8;"></i>',
        'hot': '<i class="fa-solid fa-temperature-full" style="color:#f97316;"></i>',
        'moderate': '<i class="fa-solid fa-cloud-sun"    style="color:#a3e635;"></i>',
    };
    return icons[condition] || '<i class="fa-solid fa-temperature-half"></i>';
}

// Map weather_logic.py activity_type → badge colour
const ACTIVITY_TYPE_BADGE = {
    perfect: { label: '✅ Perfect Conditions', cls: 'badge-perfect' },
    indoor: { label: '🏠 Indoor Day', cls: 'badge-indoor' },
    hot: { label: '🌡️ Hot Day', cls: 'badge-hot' },
    cool: { label: '🧥 Cool Weather', cls: 'badge-cool' },
    mixed: { label: '⛅ Mixed Weather', cls: 'badge-mixed' },
};

function renderWeatherCard(data) {
    const badge = ACTIVITY_TYPE_BADGE[data.activity_type] || { label: data.activity_type, cls: '' };

    return `
        <div class="weather-card">
            <div class="weather-icon-large">
                ${getWeatherIcon(data.condition)}
            </div>

            <!-- Primary temperature from pipeline variable: temperature_2m_max -->
            <div class="temp-display">${Math.round(data.temperature_2m_max)}°C</div>

            <!-- Activity type badge matching config.py activity_type values -->
            <span class="activity-badge ${badge.cls}">${badge.label}</span>

            <div class="condition-text">${data.condition}</div>

            <!-- Feels-like from pipeline variable: apparent_temperature_max -->
            <div class="feels-like">Feels like ${data.apparent_temperature_max}°C</div>

            <!-- Reason text straight from config.py -->
            <p class="reason-text">${data.reason}</p>

            <div class="weather-details">
                <!-- wind_speed_10m_max (km/h from Open-Meteo API) -->
                <div class="detail-item">
                    <i class="fa-solid fa-wind"></i>
                    <span>${data.wind_speed_10m_max} km/h</span>
                    <small>Wind</small>
                </div>
                <!-- precipitation_sum (mm from pipeline) -->
                <div class="detail-item">
                    <i class="fa-solid fa-droplet"></i>
                    <span>${data.precipitation_sum} mm</span>
                    <small>Rain</small>
                </div>
                <!-- relative_humidity_2m_mean (% from pipeline) -->
                <div class="detail-item">
                    <i class="fa-solid fa-water"></i>
                    <span>${data.relative_humidity_2m_mean}%</span>
                    <small>Humidity</small>
                </div>
                <!-- cloud_cover_mean (% from pipeline) -->
                <div class="detail-item">
                    <i class="fa-solid fa-cloud"></i>
                    <span>${data.cloud_cover_mean}%</span>
                    <small>Cloud</small>
                </div>
            </div>
        </div>
    `;
}
