document.addEventListener('DOMContentLoaded', () => {
    // Set default date to tomorrow
    const dateInput = document.getElementById('date');
    const tomorrow = new Date();
    tomorrow.setDate(tomorrow.getDate() + 1);
    dateInput.value = tomorrow.toISOString().split('T')[0];

    const form = document.getElementById('forecast-form');
    const submitBtn = document.getElementById('submit-btn');
    const btnText = document.getElementById('btn-text');
    const spinner = document.getElementById('loading-spinner');
    const resultsSection = document.getElementById('results-section');

    form.addEventListener('submit', async (e) => {
        e.preventDefault();

        const location = document.getElementById('location').value;
        const date = document.getElementById('date').value;

        if (!location || !date) return;

        // Start Loading State
        btnText.classList.add('hidden');
        spinner.classList.remove('hidden');
        submitBtn.disabled = true;
        resultsSection.classList.add('hidden');

        try {
            const response = await fetch('http://127.0.0.1:8000/forecast', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ location, date }),
            });

            if (!response.ok) {
                const err = await response.json();
                throw new Error(err.detail || `Request failed: ${response.statusText}`);
            }

            const data = await response.json();

            // Build results panel using the full response schema
            resultsSection.innerHTML = `
                <div class="dashboard-grid glass-panel">
                    ${renderWeatherCard(data)}
                    ${renderActivityList(data)}
                </div>
            `;

            resultsSection.classList.remove('hidden');
            resultsSection.scrollIntoView({ behavior: 'smooth', block: 'start' });

        } catch (error) {
            console.error('Error:', error);
            alert(`Failed to get forecast: ${error.message}\n\nMake sure the backend is running on http://127.0.0.1:8000`);
        } finally {
            btnText.classList.remove('hidden');
            spinner.classList.add('hidden');
            submitBtn.disabled = false;
        }
    });
});
