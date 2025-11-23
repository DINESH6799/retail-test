const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { createClient } = require('@supabase/supabase-js');

const app = express();

app.use(cors());
app.use(express.json());

// Initialize Supabase client
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_KEY;

if (!supabaseUrl || !supabaseKey) {
    console.error('⚠️  SUPABASE_URL and SUPABASE_KEY environment variables are required!');
    console.error('Please set them in your Render dashboard under Environment variables.');
}

const supabase = supabaseUrl && supabaseKey ? createClient(supabaseUrl, supabaseKey) : null;

// Health check endpoint
app.get('/', (req, res) => {
    res.json({ 
        status: 'ok', 
        message: 'Retail Brands Scraper API with Supabase storage',
        supabaseConnected: !!supabase,
        endpoints: [
            'POST /api/validate-key',
            'POST /api/scrape',
            'GET /api/results/:sessionId'
        ]
    });
});

app.get('/health', (req, res) => {
    res.json({ 
        status: 'healthy', 
        timestamp: new Date().toISOString(),
        supabase: !!supabase 
    });
});

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
                        timeout: 10000
                    }
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
                    console.log(`Rate limit hit, attempt ${attempt + 1}/${retries}`);
                    const backoffDelay = Math.pow(2, attempt + 1) * 1000;
                    await new Promise(resolve => setTimeout(resolve, backoffDelay));
                    attempt++;
                    
                    if (attempt >= retries) {
                        throw new Error('API quota exceeded after retries');
                    }
                } else if (response.data.status === 'INVALID_REQUEST') {
                    console.log(`Invalid request for ${keyword}, skipping`);
                    success = true;
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
                    console.log(`Failed after ${retries} attempts, continuing with ${places.length} places found`);
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
                error: 'API Key is invalid or Places API is not enabled.'
            });
        } else if (response.data.status === 'OVER_QUERY_LIMIT') {
            return res.status(400).json({
                valid: false,
                error: 'API Key has exceeded its quota.'
            });
        }

        res.json({ valid: true });
    } catch (error) {
        res.status(500).json({
            valid: false,
            error: 'Failed to validate API key.'
        });
    }
});

// Start scraping endpoint with Supabase storage
app.post('/api/scrape', async (req, res) => {
    const { brands, cityBounds, cityCenter, apiKey, sessionId } = req.body;

    if (!supabase) {
        return res.status(500).json({ 
            error: 'Supabase not configured. Please set SUPABASE_URL and SUPABASE_KEY environment variables.' 
        });
    }

    // Set up Server-Sent Events
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');

    const sendProgress = (data) => {
        res.write(`data: ${JSON.stringify(data)}\n\n`);
    };

    const heartbeatInterval = setInterval(() => {
        res.write(': heartbeat\n\n');
    }, 15000);

    req.on('close', () => {
        clearInterval(heartbeatInterval);
        console.log('Client disconnected');
    });

    try {
        const grid = generateGrid(cityBounds, 20, cityCenter[0]); // 20km spacing
        const totalOperations = brands.length * grid.length;
        let currentOperation = 0;
        let totalApiCalls = 0;
        const seenPlaceIds = new Set();

        // Create session in Supabase
        const { error: sessionError } = await supabase
            .from('scraping_sessions')
            .insert({
                session_id: sessionId,
                status: 'in_progress',
                total_operations: totalOperations,
                completed_operations: 0,
                total_cost: 0,
                total_results: 0
            });

        if (sessionError) {
            console.error('Error creating session:', sessionError);
            throw new Error('Failed to create scraping session');
        }

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

                    // Update session progress in Supabase
                    await supabase
                        .from('scraping_sessions')
                        .update({
                            completed_operations: currentOperation,
                            total_cost: currentCost
                        })
                        .eq('session_id', sessionId);

                    sendProgress({
                        type: 'cost-update',
                        cost: currentCost,
                        apiCalls: totalApiCalls
                    });

                    if (currentCost > 20000) {
                        throw new Error('Cost limit of ₹20,000 exceeded');
                    }

                    // Store results in Supabase (batch insert for efficiency)
                    const resultsToInsert = [];
                    
                    for (const place of places) {
                        const placeId = place.place_id;
                        
                        if (!placeId || seenPlaceIds.has(placeId)) {
                            continue;
                        }
                        seenPlaceIds.add(placeId);
                        
                        resultsToInsert.push({
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

                    // Insert results in Supabase (if any found)
                    if (resultsToInsert.length > 0) {
                        const { error: insertError } = await supabase
                            .from('scraping_results')
                            .insert(resultsToInsert);

                        if (insertError) {
                            console.error('Error inserting results:', insertError);
                        } else {
                            console.log(`✅ Inserted ${resultsToInsert.length} results for ${brand.brand}`);
                        }
                    }

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
                    console.error(`Error for ${brand.brand} at grid ${gridIndex + 1}:`, error.message);
                    await new Promise(resolve => setTimeout(resolve, 1000));
                }
            }
        }

        // Get total count of results
        const { count, error: countError } = await supabase
            .from('scraping_results')
            .select('*', { count: 'exact', head: true })
            .eq('session_id', sessionId);

        const totalResults = count || 0;

        // Update session as complete
        await supabase
            .from('scraping_sessions')
            .update({
                status: 'complete',
                total_results: totalResults,
                completed_operations: totalOperations,
                total_cost: totalApiCalls * (17 / 1000),
                updated_at: new Date().toISOString()
            })
            .eq('session_id', sessionId);

        console.log(`✅ Scraping complete! Found ${totalResults} unique results`);

        sendProgress({
            type: 'complete',
            sessionId: sessionId,
            totalFound: totalResults,
            totalCost: totalApiCalls * (17 / 1000)
        });

        await new Promise(resolve => setTimeout(resolve, 1000));
        clearInterval(heartbeatInterval);
        res.end();
    } catch (error) {
        console.error('💥 Scraping error:', error);
        
        // Update session as failed
        if (supabase) {
            await supabase
                .from('scraping_sessions')
                .update({
                    status: 'failed',
                    updated_at: new Date().toISOString()
                })
                .eq('session_id', sessionId);
        }
        
        sendProgress({
            type: 'error',
            message: error.message
        });
        clearInterval(heartbeatInterval);
        res.end();
    }
});

// Endpoint to fetch results from Supabase
app.get('/api/results/:sessionId', async (req, res) => {
    const { sessionId } = req.params;
    
    if (!supabase) {
        return res.status(500).json({ error: 'Supabase not configured' });
    }

    try {
        // Get session info
        const { data: session, error: sessionError } = await supabase
            .from('scraping_sessions')
            .select('*')
            .eq('session_id', sessionId)
            .single();

        if (sessionError || !session) {
            return res.status(404).json({ error: 'Session not found' });
        }

        // Get all results for this session
        const { data: results, error: resultsError } = await supabase
            .from('scraping_results')
            .select('*')
            .eq('session_id', sessionId)
            .order('created_at', { ascending: true });

        if (resultsError) {
            console.error('Error fetching results:', resultsError);
            return res.status(500).json({ error: 'Failed to fetch results' });
        }

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
    console.log(`Server running on port ${PORT}`);
    console.log(`Supabase: ${supabase ? '✅ Connected' : '❌ Not configured'}`);
});
