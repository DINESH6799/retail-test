const express = require('express');
const cors = require('cors');
const axios = require('axios');
const app = express();

app.use(cors());
app.use(express.json());

// Health check endpoint
app.get('/', (req, res) => {
    res.json({ 
        status: 'ok', 
        message: 'Retail Brands Scraper API is running',
        endpoints: [
            'POST /api/validate-key',
            'POST /api/scrape'
        ]
    });
});

app.get('/health', (req, res) => {
    res.json({ status: 'healthy', timestamp: new Date().toISOString() });
});

// Store active scraping sessions with results
const scrapingSessions = new Map();

// Helper function to generate grid
function generateGrid(bounds, spacingKm, centerLat) {
    const latDegPerKm = 1 / 110.574;
    const lngDegPerKm = 1 / (111.320 * Math.cos(centerLat * Math.PI / 180));
    
    const latStep = spacingKm * latDegPerKm;
    const lngStep = spacingKm * lngDegPerKm;
    
    const grid = [];
    let lat = bounds.min_lat;
    while (lat <= bounds.max_lat) {
        let lng = bounds.min_lng;
        while (lng <= bounds.max_lng) {
            grid.push({ lat, lng });
            lng += lngStep;
        }
        lat += latStep;
    }
    return grid;
}

// Fetch places from Google Maps API with retry logic
async function fetchPlaces(lat, lng, keyword, radius, apiKey, retries = 3) {
    const places = [];
    let nextPageToken = null;
    let apiCalls = 0;

    do {
        let attempt = 0;
        let success = false;

        while (attempt < retries && !success) {
            try {
                const params = {
                    key: apiKey,
                    location: `${lat},${lng}`,
                    radius: radius,
                    keyword: keyword
                };

                if (nextPageToken) {
                    params.pagetoken = nextPageToken;
                }

                const response = await axios.get(
                    'https://maps.googleapis.com/maps/api/place/nearbysearch/json',
                    { 
                        params,
                        timeout: 10000 // 10 second timeout
                    }
                );

                apiCalls++;

                if (response.data.status === 'OK' || response.data.status === 'ZERO_RESULTS') {
                    places.push(...(response.data.results || []));
                    nextPageToken = response.data.next_page_token || null;
                    
                    if (nextPageToken) {
                        // Wait 2 seconds for next page token to become active
                        await new Promise(resolve => setTimeout(resolve, 2000));
                    }
                    success = true;
                } else if (response.data.status === 'OVER_QUERY_LIMIT') {
                    console.log(`Rate limit hit, attempt ${attempt + 1}/${retries}`);
                    
                    // Exponential backoff: 2s, 4s, 8s
                    const backoffDelay = Math.pow(2, attempt + 1) * 1000;
                    await new Promise(resolve => setTimeout(resolve, backoffDelay));
                    attempt++;
                    
                    if (attempt >= retries) {
                        throw new Error('API quota exceeded after retries');
                    }
                } else if (response.data.status === 'INVALID_REQUEST') {
                    console.log(`Invalid request for ${keyword}, skipping`);
                    success = true; // Don't retry invalid requests
                    nextPageToken = null;
                } else {
                    console.log(`Unexpected status: ${response.data.status}, skipping`);
                    success = true;
                    nextPageToken = null;
                }
            } catch (error) {
                attempt++;
                console.error(`Error fetching places (attempt ${attempt}/${retries}):`, error.message);
                
                if (attempt >= retries) {
                    // After all retries failed, return what we have so far
                    console.log(`Failed after ${retries} attempts, continuing with ${places.length} places found`);
                    return { places, apiCalls };
                }
                
                // Wait before retry with exponential backoff
                const backoffDelay = Math.pow(2, attempt) * 1000;
                await new Promise(resolve => setTimeout(resolve, backoffDelay));
            }
        }
    } while (nextPageToken);

    return { places, apiCalls };
}

// Deduplicate places by place_id
function deduplicatePlaces(places) {
    const seen = new Set();
    return places.filter(place => {
        const placeId = place.place_id;
        if (placeId && !seen.has(placeId)) {
            seen.add(placeId);
            return true;
        }
        return false;
    });
}

// Validate API key
app.post('/api/validate-key', async (req, res) => {
    const { apiKey } = req.body;

    try {
        const response = await axios.get(
            'https://maps.googleapis.com/maps/api/place/nearbysearch/json',
            {
                params: {
                    key: apiKey,
                    location: '28.6139,77.2090',
                    radius: 100,
                    keyword: 'test'
                }
            }
        );

        if (response.data.status === 'REQUEST_DENIED') {
            return res.status(400).json({
                valid: false,
                error: 'API Key is invalid or Places API is not enabled. Please enable Places API in Google Cloud Console.'
            });
        } else if (response.data.status === 'OVER_QUERY_LIMIT') {
            return res.status(400).json({
                valid: false,
                error: 'API Key has exceeded its quota. Please check your billing settings.'
            });
        }

        res.json({ valid: true });
    } catch (error) {
        res.status(500).json({
            valid: false,
            error: 'Failed to validate API key. Please try again.'
        });
    }
});

// Start scraping endpoint with SSE for progress updates
app.post('/api/scrape', async (req, res) => {
    const { brands, cityBounds, cityCenter, apiKey, sessionId } = req.body;

    // Set up Server-Sent Events with longer timeout
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no'); // Disable proxy buffering

    const sendProgress = (data) => {
        res.write(`data: ${JSON.stringify(data)}\n\n`);
    };

    // Heartbeat to keep connection alive
    const heartbeatInterval = setInterval(() => {
        res.write(': heartbeat\n\n');
    }, 15000); // Every 15 seconds

    // Clean up on connection close
    req.on('close', () => {
        clearInterval(heartbeatInterval);
        console.log('Client disconnected');
    });

    try {
        const grid = generateGrid(cityBounds, 20, cityCenter[0]); // 20km spacing (12 points) - FASTEST
        const totalOperations = brands.length * grid.length;
        let currentOperation = 0;
        let totalApiCalls = 0;
        const allResults = [];
        const seenPlaceIds = new Set(); // Track seen place IDs for deduplication

        sendProgress({
            type: 'start',
            total: totalOperations
        });

        for (let brandIndex = 0; brandIndex < brands.length; brandIndex++) {
            const brand = brands[brandIndex];
            
            sendProgress({
                type: 'progress',
                current: currentOperation,
                total: totalOperations,
                percentage: Math.round((currentOperation / totalOperations) * 100),
                message: `Searching for ${brand.brand}...`,
                currentBrand: brand.brand,
                brandIndex: brandIndex + 1,
                totalBrands: brands.length
            });

            let brandHasResults = false;

            for (let gridIndex = 0; gridIndex < grid.length; gridIndex++) {
                const point = grid[gridIndex];
                currentOperation++;

                sendProgress({
                    type: 'progress',
                    current: currentOperation,
                    total: totalOperations,
                    percentage: Math.round((currentOperation / totalOperations) * 100),
                    message: `Searching for ${brand.brand}...`,
                    currentBrand: brand.brand,
                    brandIndex: brandIndex + 1,
                    totalBrands: brands.length,
                    gridPoint: `${gridIndex + 1}/${grid.length}`
                });

                try {
                    const { places, apiCalls } = await fetchPlaces(
                        point.lat,
                        point.lng,
                        brand.brand,
                        5000,
                        apiKey
                    );

                    totalApiCalls += apiCalls;
                    const currentCost = totalApiCalls * (17 / 1000);

                    sendProgress({
                        type: 'cost-update',
                        cost: currentCost,
                        apiCalls: totalApiCalls
                    });

                    if (currentCost > 20000) {
                        throw new Error('Cost limit of ₹20,000 exceeded');
                    }

                    if (places.length > 0) {
                        brandHasResults = true;
                    }

                    places.forEach(place => {
                        const placeId = place.place_id;
                        
                        // Skip if we've already seen this place
                        if (!placeId || seenPlaceIds.has(placeId)) {
                            return;
                        }
                        seenPlaceIds.add(placeId);
                        
                        const result = {
                            search_brand: brand.brand,
                            search_sku: brand.sku,
                            search_category: brand.category,
                            gmaps_category: place.types ? place.types[0] : '',
                            name: place.name || '',
                            address: place.vicinity || '',
                            latitude: place.geometry?.location?.lat || '',
                            longitude: place.geometry?.location?.lng || '',
                            business_status: place.business_status || '',
                            gmaps_url: `https://www.google.com/maps/place/?q=place_id=${placeId}`,
                            place_id: placeId,
                            is_brand_match: (place.name || '').toLowerCase().includes(brand.brand.toLowerCase())
                        };
                        allResults.push(result);
                    });

                    // Delay between grid points to avoid rate limiting
                    // 500ms for large scraping jobs (348 brands)
                    await new Promise(resolve => setTimeout(resolve, 500));
                } catch (error) {
                    if (error.message.includes('quota') || error.message.includes('Cost limit')) {
                        sendProgress({
                            type: 'error',
                            message: error.message
                        });
                        res.end();
                        return;
                    }
                    // Log error but continue with next grid point
                    console.error(`Error for ${brand.brand} at grid ${gridIndex + 1}:`, error.message);
                    // Small delay before continuing
                    await new Promise(resolve => setTimeout(resolve, 1000));
                }
            }
        }

        console.log(`✅ Scraping complete! Found ${allResults.length} unique results`);
        
        // Store results in session
        scrapingSessions.set(sessionId, {
            results: allResults,
            totalCost: totalApiCalls * (17 / 1000),
            timestamp: Date.now()
        });
        
        console.log('📤 Sending complete event to client...');

        sendProgress({
            type: 'complete',
            sessionId: sessionId,
            totalFound: allResults.length,
            totalCost: totalApiCalls * (17 / 1000)
        });

        console.log('✅ Complete event sent');
        // Give the client time to receive the complete message before closing
        await new Promise(resolve => setTimeout(resolve, 500));
        console.log('🔚 Closing stream');
        clearInterval(heartbeatInterval);
        res.end();
    } catch (error) {
        console.error('💥 Scraping error:', error);
        sendProgress({
            type: 'error',
            message: error.message
        });
        clearInterval(heartbeatInterval);
        res.end();
    } finally {
        clearInterval(heartbeatInterval);
    }
});

// Endpoint to fetch results by session ID
app.get('/api/results/:sessionId', (req, res) => {
    const { sessionId } = req.params;
    
    const session = scrapingSessions.get(sessionId);
    if (!session) {
        return res.status(404).json({ error: 'Session not found or expired' });
    }
    
    res.json({
        results: session.results,
        totalCost: session.totalCost
    });
    
    // Clean up old session after 5 minutes
    setTimeout(() => {
        scrapingSessions.delete(sessionId);
    }, 5 * 60 * 1000);
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
