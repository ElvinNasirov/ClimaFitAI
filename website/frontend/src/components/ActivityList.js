/**
 * ActivityList Component
 *
 * Renders the list of city-specific activity suggestions.
 * The `activities` array is produced by `src/weather_logic.get_city_suggestions()`
 * which contains real named places like "Heydar Aliyev Center", "Afurdja Waterfall", etc.
 *
 * Icon mapping covers the landmark/activity names used in weather_logic.py.
 */

function getActivityIcon(activityName) {
    const name = activityName.toLowerCase();

    // Exact landmark/activity keywords from weather_logic.py
    if (name.includes('hike') || name.includes('hiking') || name.includes('forest walk'))
        return 'fa-person-hiking';
    if (name.includes('waterfall'))
        return 'fa-water';
    if (name.includes('cable car'))
        return 'fa-cable-car';
    if (name.includes('lake') || name.includes('beach') || name.includes('caspian') || name.includes('coast'))
        return 'fa-water';
    if (name.includes('museum') || name.includes('history') || name.includes('gallery'))
        return 'fa-building-columns';
    if (name.includes('palace') || name.includes('tower') || name.includes('flame') || name.includes('icherisheher') || name.includes('old city'))
        return 'fa-landmark';
    if (name.includes('park') || name.includes('hirkan') || name.includes('nature'))
        return 'fa-tree';
    if (name.includes('ski') || name.includes('snowboard'))
        return 'fa-person-skiing';
    if (name.includes('spa') || name.includes('wellness') || name.includes('resort') || name.includes('hotel'))
        return 'fa-spa';
    if (name.includes('cafe') || name.includes('tea') || name.includes('restaurant'))
        return 'fa-mug-hot';
    if (name.includes('shop') || name.includes('market') || name.includes('handicraft'))
        return 'fa-bag-shopping';
    if (name.includes('walk') || name.includes('boulevard') || name.includes('street') || name.includes('stroll'))
        return 'fa-person-walking';
    if (name.includes('photo'))
        return 'fa-camera';
    if (name.includes('pool') || name.includes('water park') || name.includes('swimming'))
        return 'fa-person-swimming';
    if (name.includes('village') || name.includes('khinalig'))
        return 'fa-house-chimney';
    if (name.includes('viewpoint') || name.includes('view'))
        return 'fa-binoculars';
    if (name.includes('entertainment') || name.includes('indoor'))
        return 'fa-gamepad';
    if (name.includes('plantation') || name.includes('tea plantation'))
        return 'fa-leaf';
    return 'fa-compass';
}

function renderActivityList(data) {
    // `data.activities` comes directly from weather_logic.get_city_suggestions()
    const activityItems = data.activities.map(activity => `
        <li class="activity-item">
            <div class="activity-icon">
                <i class="fa-solid ${getActivityIcon(activity)}"></i>
            </div>
            <div class="activity-name">${activity}</div>
        </li>
    `).join('');

    return `
        <div class="activities-card">
            <h3><i class="fa-solid fa-compass"></i> Recommended Activities</h3>
            <ul class="activities-list">
                ${activityItems}
            </ul>
        </div>
    `;
}
