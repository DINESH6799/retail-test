const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');

const app = express();

app.use(cors());
app.use(express.json());

// Initialize Supabase
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_KEY;
const supabase = supabaseUrl && supabaseKey ? createClient(supabaseUrl, supabaseKey) : null;

// Health check
app.get('/', (req, res) => {
    res.json({ 
        status: 'ok', 
        message: 'Retail Brands Scraper API - Polling Mode',
        supabaseConnected: !!supabase,
        mode: 'background-processing',
        endpoints: [
            'POST /api/validate-key',
            'POST /api/scrape (starts background job)',
            'GET /api/status/:sessionId (check progress)',
            'GET /api/results/:sessionId (download results)'
        ]
    });
});

app.get('/health', (req, res) => {
    res.json({ status: 'healthy', timestamp: new Date().toISOString() });
});

// Generate grid
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

// Fetch places with retry
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
                    { params, timeout: 10000 }
                );

                apiCalls++;

                if (response.data.status === 'OK' || response.data.status === 'ZERO_RESULTS') {
                    places.push(...(response.data.results || []));
                    nextPageToken = response.data.next_page_token || null;
                    
                    if (nextPageToken) {
                        await new Promise(resolve => setTimeout(resolve, 2000));
                    }
                    success = true;
                } else if (response.data.status === 'OVER_QUERY_LIMIT') {
                    const backoffDelay = Math.pow(2, attempt + 1) * 1000;
                    await new Promise(resolve => setTimeout(resolve, backoffDelay));
                    attempt++;
                    
                    if (attempt >= retries) {
                        throw new Error('API quota exceeded');
                    }
                } else {
                    success = true;
                    nextPageToken = null;
                }
            } catch (error) {
                attempt++;
                if (attempt >= retries) {
                    console.log(`Failed after ${retries} attempts`);
                    return { places, apiCalls };
                }
                const backoffDelay = Math.pow(2, attempt) * 1000;
                await new Promise(resolve => setTimeout(resolve, backoffDelay));
            }
        }
    } while (nextPageToken);

    return { places, apiCalls };
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
                error: 'API Key invalid or Places API not enabled'
            });
        } else if (response.data.status === 'OVER_QUERY_LIMIT') {
            return res.status(400).json({
                valid: false,
                error: 'API Key quota exceeded'
            });
        }

        res.json({ valid: true });
    } catch (error) {
        res.status(500).json({
            valid: false,
            error: 'Failed to validate API key'
        });
    }
});

// BACKGROUND SCRAPING FUNCTION
async function runScrapingInBackground(sessionId, brands, cityBounds, cityCenter, apiKey) {
    console.log(`🚀 Starting background scraping for session: ${sessionId}`);
    
    try {
        const grid = generateGrid(cityBounds, 10, cityCenter[0]); // 10km grid
        const totalOperations = brands.length * grid.length;
        let currentOperation = 0;
        let totalApiCalls = 0;
        // REMOVED: Global seenPlaceIds - was causing massive data loss!

        console.log(`Grid: ${grid.length} points, Total ops: ${totalOperations}`);

        // Update session to in_progress
        await supabase
            .from('scraping_sessions')
            .update({
                status: 'in_progress',
                total_operations: totalOperations,
                updated_at: new Date().toISOString()
            })
            .eq('session_id', sessionId);

        // Process each brand
        for (let brandIndex = 0; brandIndex < brands.length; brandIndex++) {
            const brand = brands[brandIndex];
            console.log(`Processing ${brandIndex + 1}/${brands.length}: ${brand.brand}`);

            const brandResults = [];
            const brandSeenPlaceIds = new Set(); // Per-brand deduplication!

            // Process each grid point
            for (let gridIndex = 0; gridIndex < grid.length; gridIndex++) {
                const point = grid[gridIndex];
                currentOperation++;

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

                    if (currentCost > 20000) {
                        throw new Error('Cost limit exceeded');
                    }

                    // Collect unique results PER BRAND (not globally!)
                    for (const place of places) {
                        const placeId = place.place_id;
                        
                        // Only skip if we've seen this place for THIS brand
                        if (!placeId || brandSeenPlaceIds.has(placeId)) {
                            continue;
                        }
                        brandSeenPlaceIds.add(placeId);
                        
                        brandResults.push({
                            session_id: sessionId,
                            search_brand: brand.brand,
                            search_sku: brand.sku,
                            search_category: brand.category,
                            gmaps_category: place.types ? place.types[0] : '',
                            name: place.name || '',
                            address: place.vicinity || '',
                            latitude: place.geometry?.location?.lat || null,
                            longitude: place.geometry?.location?.lng || null,
                            business_status: place.business_status || '',
                            gmaps_url: `https://www.google.com/maps/place/?q=place_id=${placeId}`,
                            place_id: placeId,
                            is_brand_match: (place.name || '').toLowerCase().includes(brand.brand.toLowerCase())
                        });
                    }

                    // Small delay between requests
                    await new Promise(resolve => setTimeout(resolve, 300));
                } catch (error) {
                    if (error.message.includes('Cost limit')) {
                        console.error('Cost limit exceeded, stopping');
                        await supabase
                            .from('scraping_sessions')
                            .update({
                                status: 'failed',
                                error_message: 'Cost limit exceeded',
                                updated_at: new Date().toISOString()
                            })
                            .eq('session_id', sessionId);
                        return;
                    }
                    console.error(`Error at grid ${gridIndex}:`, error.message);
                }

                // Update progress every 10 operations
                if (currentOperation % 10 === 0) {
                    await supabase
                        .from('scraping_sessions')
                        .update({
                            completed_operations: currentOperation,
                            total_cost: totalApiCalls * (17 / 1000),
                            current_brand: brand.brand,
                            current_brand_index: brandIndex + 1,
                            updated_at: new Date().toISOString()
                        })
                        .eq('session_id', sessionId);
                }
            }

            // Insert brand results
            if (brandResults.length > 0) {
                console.log(`Saving ${brandResults.length} results for ${brand.brand}`);
                const { error: insertError } = await supabase
                    .from('scraping_results')
                    .insert(brandResults);

                if (insertError) {
                    console.error('Error inserting results:', insertError);
                }
            }

            // Update session after each brand
            await supabase
                .from('scraping_sessions')
                .update({
                    completed_operations: currentOperation,
                    total_cost: totalApiCalls * (17 / 1000),
                    current_brand: brand.brand,
                    current_brand_index: brandIndex + 1,
                    updated_at: new Date().toISOString()
                })
                .eq('session_id', sessionId);
        }

        // Get final count
        const { count } = await supabase
            .from('scraping_results')
            .select('*', { count: 'exact', head: true })
            .eq('session_id', sessionId);

        // Mark complete
        await supabase
            .from('scraping_sessions')
            .update({
                status: 'complete',
                total_results: count || 0,
                completed_operations: totalOperations,
                total_cost: totalApiCalls * (17 / 1000),
                updated_at: new Date().toISOString()
            })
            .eq('session_id', sessionId);

        console.log(`✅ Scraping complete! Session: ${sessionId}, Results: ${count}`);

    } catch (error) {
        console.error('❌ Scraping error:', error);
        await supabase
            .from('scraping_sessions')
            .update({
                status: 'failed',
                error_message: error.message,
                updated_at: new Date().toISOString()
            })
            .eq('session_id', sessionId);
    }
}

// START SCRAPING (returns immediately, runs in background)
app.post('/api/scrape', async (req, res) => {
    const { brands, cityBounds, cityCenter, apiKey, sessionId } = req.body;

    if (!supabase) {
        return res.status(500).json({ 
            error: 'Supabase not configured' 
        });
    }

    try {
        // Create session
        const { error: sessionError } = await supabase
            .from('scraping_sessions')
            .insert({
                session_id: sessionId,
                status: 'starting',
                total_operations: 0,
                completed_operations: 0,
                total_cost: 0,
                total_results: 0
            });

        if (sessionError) {
            console.error('Error creating session:', sessionError);
            return res.status(500).json({ error: 'Failed to create session' });
        }

        // Start background processing (don't await!)
        runScrapingInBackground(sessionId, brands, cityBounds, cityCenter, apiKey)
            .catch(err => console.error('Background error:', err));

        // Return immediately
        res.json({
            success: true,
            sessionId: sessionId,
            message: 'Scraping started in background',
            statusEndpoint: `/api/status/${sessionId}`
        });

    } catch (error) {
        console.error('Error starting scraping:', error);
        res.status(500).json({ error: 'Failed to start scraping' });
    }
});

// GET STATUS (for polling)
app.get('/api/status/:sessionId', async (req, res) => {
    const { sessionId } = req.params;

    if (!supabase) {
        return res.status(500).json({ error: 'Supabase not configured' });
    }

    try {
        const { data: session, error } = await supabase
            .from('scraping_sessions')
            .select('*')
            .eq('session_id', sessionId)
            .single();

        if (error || !session) {
            return res.status(404).json({ error: 'Session not found' });
        }

        // Calculate progress percentage
        const percentage = session.total_operations > 0 
            ? Math.round((session.completed_operations / session.total_operations) * 100)
            : 0;

        res.json({
            sessionId: session.session_id,
            status: session.status,
            progress: {
                current: session.completed_operations,
                total: session.total_operations,
                percentage: percentage,
                currentBrand: session.current_brand,
                brandIndex: session.current_brand_index
            },
            cost: session.total_cost,
            totalResults: session.total_results,
            updatedAt: session.updated_at
        });

    } catch (error) {
        console.error('Error fetching status:', error);
        res.status(500).json({ error: 'Failed to fetch status' });
    }
});

// GET RESULTS
app.get('/api/results/:sessionId', async (req, res) => {
    const { sessionId } = req.params;

    if (!supabase) {
        return res.status(500).json({ error: 'Supabase not configured' });
    }

    try {
        const { data: session, error: sessionError } = await supabase
            .from('scraping_sessions')
            .select('*')
            .eq('session_id', sessionId)
            .single();

        if (sessionError || !session) {
            return res.status(404).json({ error: 'Session not found' });
        }

        // Fetch ALL results by setting a high limit
        // Supabase default is 1000, we need to override it
        const { data: results, error: resultsError } = await supabase
            .from('scraping_results')
            .select('*')
            .eq('session_id', sessionId)
            .order('created_at', { ascending: true })
            .limit(100000); // Set high limit to get all results

        if (resultsError) {
            console.error('Error fetching results:', resultsError);
            return res.status(500).json({ error: 'Failed to fetch results' });
        }

        console.log(`📊 Fetched ${results?.length || 0} results for session ${sessionId}`);

        res.json({
            session: session,
            results: results || [],
            totalCost: session.total_cost
        });

    } catch (error) {
        console.error('Error:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
    console.log(`🚀 Server running on port ${PORT}`);
    console.log(`📊 Mode: Background Processing with Polling`);
    console.log(`💾 Supabase: ${supabase ? '✅ Connected' : '❌ Not configured'}`);
});
