(function (globalScope) {
    const MODEL = globalScope.BrandCleanerModel || null;

    function normalizeText(value) {
        if (value === null || value === undefined) {
            return '';
        }

        return String(value)
            .normalize('NFKD')
            .replace(/[\u0300-\u036f]/g, '')
            .toLowerCase()
            .replace(/&/g, ' and ')
            .replace(/\+/g, ' plus ')
            .replace(/[^a-z0-9]+/g, ' ')
            .replace(/\s+/g, ' ')
            .trim();
    }

    function escapeRegex(value) {
        return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    }

    function matchesWholePhrase(text, phrase) {
        if (!text || !phrase) {
            return false;
        }

        const pattern = new RegExp('(^| )' + escapeRegex(phrase) + '( |$)');
        return pattern.test(text);
    }

    function getSearchBrand(row) {
        return row.search_brand || row.brand || '';
    }

    const CATEGORY_ALLOWED_TYPES = {
        'Apparel': ['clothing_store', 'shoe_store', 'department_store', 'shopping_mall', 'store'],
        'F&B': ['restaurant', 'cafe', 'meal_takeaway', 'bakery', 'food', 'meal_delivery', 'bar', 'coffee_shop'],
        'Electronics': ['electronics_store', 'appliance_store', 'store', 'home_goods_store', 'shopping_mall'],
        'Grocery': ['supermarket', 'grocery_or_supermarket', 'store', 'convenience_store'],
        'Pharmacy': ['pharmacy', 'drugstore', 'health', 'store'],
        'Cinema': ['movie_theater', 'movie_rental'],
        'Realty': ['real_estate_agency', 'point_of_interest'],
        'Salons': ['hair_care', 'beauty_salon', 'spa', 'store'],
        'Mobile Stores': ['electronics_store', 'store'],
        'Opticals': ['health', 'store', 'optician', 'optometrist'],
        'Banking': ['bank', 'atm', 'finance'],
        'Automobile': ['car_dealer', 'car_repair', 'car_rental', 'store'],
        'Fitness': ['gym', 'fitness_center', 'sports_activity_location', 'health', 'store']
    };

    const CATEGORY_NORMALIZATION_ALIASES = {
        'apparel': 'Apparel',
        'fashion apparel': 'Apparel',
        'fashion and apparel': 'Apparel',
        'fashion': 'Apparel',
        'clothing': 'Apparel',
        'clothing apparel': 'Apparel',
        'food and beverage': 'F&B',
        'food beverage': 'F&B',
        'f and b': 'F&B',
        'f b': 'F&B',
        'restaurant': 'F&B',
        'restaurants': 'F&B',
        'cafe': 'F&B',
        'cafes': 'F&B',
        'electronics': 'Electronics',
        'electronic': 'Electronics',
        'electronic appliances': 'Electronics',
        'consumer electronics': 'Electronics',
        'grocery': 'Grocery',
        'groceries': 'Grocery',
        'supermarket': 'Grocery',
        'supermarkets': 'Grocery',
        'pharmacy': 'Pharmacy',
        'medical': 'Pharmacy',
        'chemist': 'Pharmacy',
        'cinema': 'Cinema',
        'movie': 'Cinema',
        'movies': 'Cinema',
        'real estate': 'Realty',
        'realty': 'Realty',
        'property': 'Realty',
        'salon': 'Salons',
        'salons': 'Salons',
        'beauty salon': 'Salons',
        'hair salon': 'Salons',
        'mobile': 'Mobile Stores',
        'mobile store': 'Mobile Stores',
        'mobile stores': 'Mobile Stores',
        'phones': 'Mobile Stores',
        'optical': 'Opticals',
        'opticals': 'Opticals',
        'eyewear': 'Opticals',
        'bank': 'Banking',
        'banking': 'Banking',
        'banks financial services': 'Banking',
        'banking financial services': 'Banking',
        'financial services': 'Banking',
        'automobile': 'Automobile',
        'automobiles': 'Automobile',
        'auto': 'Automobile',
        'cars': 'Automobile',
        'gyms': 'Fitness',
        'gym': 'Fitness',
        'fitness': 'Fitness',
        'fitness center': 'Fitness',
        'fitness centres': 'Fitness',
        'fitness centre': 'Fitness',
        'sports': 'Fitness',
        'jewelry': 'Apparel',
        'jewellery': 'Apparel',
        'variety store': 'Grocery',
        'branded retail': 'Apparel'
    };

    const CATEGORY_NORMALIZATION_KEYWORDS = [
        { keywords: ['fashion', 'apparel'], canonical: 'Apparel' },
        { keywords: ['clothing'], canonical: 'Apparel' },
        { keywords: ['food', 'beverage'], canonical: 'F&B' },
        { keywords: ['restaurant'], canonical: 'F&B' },
        { keywords: ['cafe'], canonical: 'F&B' },
        { keywords: ['electronic'], canonical: 'Electronics' },
        { keywords: ['appliance'], canonical: 'Electronics' },
        { keywords: ['grocery'], canonical: 'Grocery' },
        { keywords: ['supermarket'], canonical: 'Grocery' },
        { keywords: ['pharmacy'], canonical: 'Pharmacy' },
        { keywords: ['chemist'], canonical: 'Pharmacy' },
        { keywords: ['medical'], canonical: 'Pharmacy' },
        { keywords: ['cinema'], canonical: 'Cinema' },
        { keywords: ['movie'], canonical: 'Cinema' },
        { keywords: ['realty'], canonical: 'Realty' },
        { keywords: ['real', 'estate'], canonical: 'Realty' },
        { keywords: ['property'], canonical: 'Realty' },
        { keywords: ['salon'], canonical: 'Salons' },
        { keywords: ['beauty'], canonical: 'Salons' },
        { keywords: ['hair'], canonical: 'Salons' },
        { keywords: ['mobile'], canonical: 'Mobile Stores' },
        { keywords: ['phone'], canonical: 'Mobile Stores' },
        { keywords: ['optical'], canonical: 'Opticals' },
        { keywords: ['eyewear'], canonical: 'Opticals' },
        { keywords: ['bank'], canonical: 'Banking' },
        { keywords: ['financial'], canonical: 'Banking' },
        { keywords: ['automobile'], canonical: 'Automobile' },
        { keywords: ['auto'], canonical: 'Automobile' },
        { keywords: ['car'], canonical: 'Automobile' },
        { keywords: ['gym'], canonical: 'Fitness' },
        { keywords: ['fitness'], canonical: 'Fitness' },
        { keywords: ['sports'], canonical: 'Fitness' }
    ];

    function normalizeCategoryValue(category) {
        const rawCategory = category || '';
        const normalizedCategory = normalizeText(rawCategory);
        const normalizedTokens = normalizedCategory ? normalizedCategory.split(' ') : [];

        if (!normalizedCategory) {
            return '';
        }

        if (CATEGORY_NORMALIZATION_ALIASES[normalizedCategory]) {
            return CATEGORY_NORMALIZATION_ALIASES[normalizedCategory];
        }

        for (const rule of CATEGORY_NORMALIZATION_KEYWORDS) {
            if (rule.keywords.every(keyword => normalizedTokens.includes(keyword))) {
                return rule.canonical;
            }
        }

        return rawCategory;
    }

    function getSearchCategory(row) {
        return normalizeCategoryValue(row.search_category || row.category || '');
    }

    const BRAND_ALIASES = {
        'Westside': ['westside'],
        'Croma': ['croma'],
        'Bikanervala': ['bikanervala'],
        'Café Coffee Day': ['cafe coffee day', 'ccd'],
        'Cafe Coffee Day': ['cafe coffee day', 'ccd'],
        'Starbucks': ['starbucks'],
        'ZUDIO': ['zudio'],
        'Zudio': ['zudio'],
        "Domino's Pizza": ['dominos pizza', 'domino s pizza', 'dominos', 'domino s', 'domino'],
        'Max': ['max fashion', 'max'],
        'Apollo Pharmacy': ['apollo pharmacy', 'apollopharmacy', 'applo pharmacy'],
        'Titan Eye+': ['titan eye plus', 'titan eye'],
        "McDonald's": ['mcdonalds', 'mcdonald s', 'mcdonald'],
        'KFC': ['kfc'],
        'Reliance Digital': ['reliance digital', 'reliance'],
        'Electronics Mart': ['electronics mart', 'electronic mart'],
        'Cut & Style Salon': ['cut and style salon', 'cut and style', 'cut style salon']
    };

    const BRAND_ALLOWED_TYPES = {
        'Westside': ['clothing_store', 'department_store', 'shopping_mall', 'store'],
        'ZUDIO': ['clothing_store', 'department_store', 'shopping_mall', 'store'],
        'Zudio': ['clothing_store', 'department_store', 'shopping_mall', 'store'],
        'Max': ['clothing_store', 'department_store', 'shopping_mall', 'store'],
        'Croma': ['electronics_store', 'appliance_store', 'shopping_mall', 'store'],
        'Reliance Digital': ['electronics_store', 'appliance_store', 'shopping_mall', 'store'],
        'Electronics Mart': ['electronics_store', 'appliance_store', 'shopping_mall', 'store'],
        'Apollo Pharmacy': ['pharmacy', 'drugstore', 'store', 'health'],
        'Titan Eye+': ['optician', 'optometrist', 'store', 'health'],
        "Domino's Pizza": ['restaurant', 'meal_delivery', 'meal_takeaway', 'pizza_restaurant', 'store'],
        "McDonald's": ['restaurant', 'fast_food_restaurant', 'meal_takeaway', 'meal_delivery', 'store'],
        'KFC': ['restaurant', 'fast_food_restaurant', 'meal_takeaway', 'meal_delivery', 'store'],
        'Café Coffee Day': ['cafe', 'coffee_shop', 'restaurant', 'store'],
        'Cafe Coffee Day': ['cafe', 'coffee_shop', 'restaurant', 'store'],
        'Starbucks': ['cafe', 'coffee_shop', 'restaurant', 'store'],
        'Bikanervala': ['restaurant', 'store', 'cafe', 'bakery', 'candy_store', 'sweet_shop', 'meal_takeaway', 'food'],
        'Cut & Style Salon': ['beauty_salon', 'hair_care', 'spa', 'store']
    };

    const GLOBAL_NEGATIVE_TERMS = [
        'corporate office',
        'head office',
        'registered office',
        'office',
        'factory',
        'warehouse',
        'godown',
        'depot',
        'bus stop',
        'atm',
        'bank',
        'petrol pump',
        'police station',
        'metro station',
        'hotel',
        'lodge',
        'hostel',
        'apartment',
        'residency',
        'society',
        'tower',
        'mall parking'
    ];

    const BRAND_NEGATIVE_TERMS = {
        'Westside': ['zudio', 'pantaloons', 'lifestyle', 'max', 'fabindia'],
        'ZUDIO': ['westside', 'pantaloons', 'lifestyle', 'max', 'fabindia', 'beauty'],
        'Zudio': ['westside', 'pantaloons', 'lifestyle', 'max', 'fabindia', 'beauty'],
        'Max': ['hospital', 'healthcare', 'life insurance', 'lab', 'super speciality', 'superspeciality'],
        'Croma': ['pharmacy', 'dyson croma', 'tata croma noida xd'],
        'Reliance Digital': ['reliance fresh', 'jio', 'smart bazaar'],
        'Electronics Mart': ['warehouse'],
        'Apollo Pharmacy': ['hospital', 'clinic', 'diagnostic'],
        'Titan Eye+': ['watch', 'world', 'titan company', 'tanishq'],
        "Domino's Pizza": ['bank', 'dry cleaner', 'pizza king', 'la pino', 'pizza mania', 'chicago pizza'],
        "McDonald's": ['warehouse'],
        'KFC': ['warehouse'],
        'Café Coffee Day': ['ccd academy', 'corporate office'],
        'Cafe Coffee Day': ['ccd academy', 'corporate office'],
        'Starbucks': ['barista', 'coffee time', 'star cafe', 'sardar ji bakhsh', 'love over coffee', 'mogli'],
        'Bikanervala': ['factory', 'corporate office', 'foods private limited'],
        'Cut & Style Salon': ['academy', 'training']
    };

    const REVIEW_TERMS = ['inside', 'near', 'opp', 'opposite', 'beside', 'next to', 'mall', 'plaza'];
    const BANKING_SERVICE_TERMS = [
        'atm',
        'cdm',
        'cash point',
        'cash deposit',
        'cash recycler',
        'atm cum cdm',
        'e lobby',
        'mini branch'
    ];
    const BANKING_COMPETITOR_BRANDS = [
        'Axis Bank',
        'Bank of Baroda',
        'Bank of India',
        'Canara Bank',
        'Central Bank of India',
        'Federal Bank',
        'HDFC Bank',
        'ICICI Bank',
        'IDBI Bank',
        'Indian Bank',
        'IndusInd Bank',
        'Punjab National Bank',
        'South Indian Bank',
        'State Bank of India',
        'Union Bank of India'
    ];

    function getBrandConfig(config, brand, fallbackValue) {
        if (config[brand]) {
            return config[brand];
        }

        const normalizedBrand = normalizeText(brand);
        for (const key of Object.keys(config)) {
            if (normalizeText(key) === normalizedBrand) {
                return config[key];
            }
        }

        return fallbackValue;
    }

    function getBrandAliases(brand) {
        return getBrandConfig(BRAND_ALIASES, brand, [brand]).map(normalizeText).filter(Boolean);
    }

    function getAllowedTypes(brand, category) {
        return getBrandConfig(BRAND_ALLOWED_TYPES, brand, CATEGORY_ALLOWED_TYPES[category] || []);
    }

    function hasConflictingBankBrand(nameNormalized, brand) {
        const normalizedBrand = normalizeText(brand);
        return BANKING_COMPETITOR_BRANDS.some(candidate => {
            const normalizedCandidate = normalizeText(candidate);
            if (!normalizedCandidate || normalizedCandidate === normalizedBrand) {
                return false;
            }
            return (
                matchesWholePhrase(nameNormalized, normalizedCandidate) &&
                normalizedCandidate.includes(normalizedBrand)
            );
        });
    }

    function hasRequiredData(result) {
        return Boolean(
            result &&
            result.name &&
            result.address &&
            result.latitude !== null &&
            result.latitude !== undefined &&
            result.longitude !== null &&
            result.longitude !== undefined
        );
    }

    function calculateDistance(lat1, lon1, lat2, lon2) {
        const earthRadius = 6371000;
        const dLat = (lat2 - lat1) * Math.PI / 180;
        const dLon = (lon2 - lon1) * Math.PI / 180;
        const a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
            Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
            Math.sin(dLon / 2) * Math.sin(dLon / 2);
        const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return earthRadius * c;
    }

    function nameSimilarity(name1, name2) {
        const n1 = normalizeText(name1).replace(/\s+/g, '');
        const n2 = normalizeText(name2).replace(/\s+/g, '');
        if (!n1 || !n2) {
            return false;
        }

        const shorter = n1.length <= n2.length ? n1 : n2;
        const longer = n1.length > n2.length ? n1 : n2;
        return longer.includes(shorter);
    }

    function normalizeModelText(value) {
        return normalizeText(value);
    }

    function getWordTokens(text) {
        const prepared = normalizeModelText(text);
        return prepared ? prepared.split(' ').filter(token => token.length >= 2) : [];
    }

    function buildWordNgrams(tokens, minN, maxN) {
        const ngrams = [];
        for (let size = minN; size <= maxN; size++) {
            for (let index = 0; index <= tokens.length - size; index++) {
                ngrams.push(tokens.slice(index, index + size).join(' '));
            }
        }
        return ngrams;
    }

    function buildCharWbNgrams(text, minN, maxN) {
        const tokens = normalizeModelText(text).split(' ').filter(Boolean);
        const ngrams = [];

        for (const token of tokens) {
            const padded = ` ${token} `;
            for (let size = minN; size <= maxN; size++) {
                for (let index = 0; index <= padded.length - size; index++) {
                    ngrams.push(padded.slice(index, index + size));
                }
            }
        }

        return ngrams;
    }

    function getFeatureOccurrences(text, spec) {
        if (!spec) {
            return [];
        }

        if (spec.analyzer === 'char_wb') {
            return buildCharWbNgrams(text, spec.ngram_range[0], spec.ngram_range[1]);
        }

        return buildWordNgrams(getWordTokens(text), spec.ngram_range[0], spec.ngram_range[1]);
    }

    function getVectorContribution(text, spec) {
        if (!MODEL || !spec) {
            return 0;
        }

        const counts = new Map();
        for (const feature of getFeatureOccurrences(text, spec)) {
            if (spec.features[feature]) {
                counts.set(feature, (counts.get(feature) || 0) + 1);
            }
        }

        let dotProduct = 0;
        let normSquare = 0;

        counts.forEach((count, feature) => {
            const featureValues = spec.features[feature];
            const tfidfValue = count * featureValues[0];
            normSquare += tfidfValue * tfidfValue;
            dotProduct += tfidfValue * featureValues[1];
        });

        if (!normSquare) {
            return 0;
        }

        return dotProduct / Math.sqrt(normSquare);
    }

    function getCategoricalContribution(value, spec) {
        if (!MODEL || !spec) {
            return 0;
        }

        const key = value === null || value === undefined ? '' : String(value);
        return spec[key] || 0;
    }

    function getModelProbability(result) {
        if (!MODEL) {
            return null;
        }

        const vectors = MODEL.vectorizers || {};
        const oneHot = MODEL.one_hot || {};
        const score = (MODEL.intercept || 0) +
            getVectorContribution(result.name || '', vectors.name_char) +
            getVectorContribution(result.name || '', vectors.name_word) +
            getVectorContribution(result.address || '', vectors.addr_word) +
            getVectorContribution((result.gmaps_category || 'unknown') || 'unknown', vectors.gmaps_word) +
            getCategoricalContribution(getSearchBrand(result), oneHot.brand) +
            getCategoricalContribution(getSearchCategory(result), oneHot.category) +
            getCategoricalContribution(result.business_status || '', oneHot.status);

        return 1 / (1 + Math.exp(-score));
    }

    function classifyResult(result) {
        const brand = getSearchBrand(result);
        const category = getSearchCategory(result);
        const aliases = getBrandAliases(brand);
        const name = result.name || '';
        const address = result.address || '';
        const nameNormalized = normalizeText(name);
        const addressNormalized = normalizeText(address);
        const businessStatus = String(result.business_status || '').trim().toUpperCase();
        const gmapsCategory = normalizeText(result.gmaps_category);
        const allowedTypes = getAllowedTypes(brand, category).map(normalizeText);
        const brandNegativeTerms = getBrandConfig(BRAND_NEGATIVE_TERMS, brand, []);
        const isBankingCategory = category === 'Banking';
        const modelProbability = getModelProbability(result);
        const probabilityConfidence = modelProbability !== null && modelProbability >= 0.75 ? 'High' : 'Medium';

        if (!hasRequiredData(result)) {
            return {
                cleaned_status: 'Not Valid',
                cleaner_confidence: 'High',
                cleaner_reason: 'missing_required_data',
                review_flag: 'No',
                model_probability: modelProbability
            };
        }

        const hasBrand = aliases.some(alias => matchesWholePhrase(nameNormalized, alias));
        const startsWithBrand = aliases.some(alias => nameNormalized.startsWith(alias));

        if (businessStatus && businessStatus !== 'OPERATIONAL') {
            return {
                cleaned_status: 'Not Valid',
                cleaner_confidence: 'High',
                cleaner_reason: 'business_not_operational',
                review_flag: 'No',
                model_probability: modelProbability
            };
        }

        if (!hasBrand) {
            return {
                cleaned_status: 'Not Valid',
                cleaner_confidence: 'High',
                cleaner_reason: 'brand_missing_from_name',
                review_flag: 'No',
                model_probability: modelProbability
            };
        }

        if (isBankingCategory && BANKING_SERVICE_TERMS.some(term => nameNormalized.includes(term))) {
            return {
                cleaned_status: 'Not Valid',
                cleaner_confidence: 'High',
                cleaner_reason: 'banking_service_point',
                review_flag: 'No',
                model_probability: modelProbability
            };
        }

        const isBankBranchLike =
            isBankingCategory &&
            (gmapsCategory === 'bank' || gmapsCategory === 'finance') &&
            !BANKING_SERVICE_TERMS.some(term => nameNormalized.includes(term));

        if (isBankBranchLike && hasConflictingBankBrand(nameNormalized, brand)) {
            return {
                cleaned_status: 'Not Valid',
                cleaner_confidence: 'High',
                cleaner_reason: 'conflicting_bank_brand',
                review_flag: 'No',
                model_probability: modelProbability
            };
        }

        if (!isBankBranchLike && GLOBAL_NEGATIVE_TERMS.some(term => nameNormalized.includes(term))) {
            return {
                cleaned_status: 'Not Valid',
                cleaner_confidence: 'Medium',
                cleaner_reason: 'non_store_or_landmark_identity',
                review_flag: 'Yes',
                model_probability: modelProbability
            };
        }

        if (brandNegativeTerms.some(term => nameNormalized.includes(normalizeText(term)))) {
            return {
                cleaned_status: 'Not Valid',
                cleaner_confidence: 'Medium',
                cleaner_reason: 'brand_specific_negative_pattern',
                review_flag: 'Yes',
                model_probability: modelProbability
            };
        }

        if (allowedTypes.length > 0 && gmapsCategory && !allowedTypes.includes(gmapsCategory)) {
            const shouldOverride = startsWithBrand && modelProbability !== null && modelProbability >= 0.92;

            if (!shouldOverride) {
                return {
                    cleaned_status: 'Not Valid',
                    cleaner_confidence: 'Medium',
                    cleaner_reason: `place_type_mismatch:${gmapsCategory}`,
                    review_flag: 'No',
                    model_probability: modelProbability
                };
            }

            return {
                cleaned_status: 'Valid',
                cleaner_confidence: 'Medium',
                cleaner_reason: 'brand_identity_override',
                review_flag: 'No',
                model_probability: modelProbability
            };
        }

        let cleanerReason = 'brand_identity_match';
        let confidence = modelProbability !== null ? probabilityConfidence : (result.is_brand_match ? 'High' : 'Medium');
        let reviewFlag = 'No';

        if ((modelProbability !== null && modelProbability < 0.60) || REVIEW_TERMS.some(term => addressNormalized.includes(term))) {
            reviewFlag = 'Yes';
            confidence = 'Medium';
        }

        if (nameNormalized.includes(' and ') || nameNormalized.includes(' | ')) {
            reviewFlag = 'Yes';
            confidence = 'Medium';
            cleanerReason = 'combined_listing_or_complex_name';
        }

        return {
            cleaned_status: 'Valid',
            cleaner_confidence: confidence,
            cleaner_reason: cleanerReason,
            review_flag: reviewFlag,
            model_probability: modelProbability
        };
    }

    function annotateResults(results) {
        return (results || []).map(result => Object.assign({}, result, classifyResult(result)));
    }

    function removeDuplicatesEnhanced(results) {
        const uniqueResults = [];

        for (const result of results) {
            let isDuplicate = false;

            for (const existing of uniqueResults) {
                if (getSearchBrand(existing) !== getSearchBrand(result)) {
                    continue;
                }

                if (existing.place_id && result.place_id && existing.place_id === result.place_id) {
                    isDuplicate = true;
                    break;
                }

                const distance = calculateDistance(
                    Number(existing.latitude),
                    Number(existing.longitude),
                    Number(result.latitude),
                    Number(result.longitude)
                );

                if (distance < 50 && nameSimilarity(existing.name, result.name)) {
                    isDuplicate = true;
                    break;
                }
            }

            if (!isDuplicate) {
                uniqueResults.push(result);
            }
        }

        return uniqueResults;
    }

    function cleanResults(results) {
        const annotated = annotateResults(results);
        const validResults = annotated.filter(row => row.cleaned_status === 'Valid');
        return removeDuplicatesEnhanced(validResults);
    }

    function getCleaningStats(rawResults, cleanedResults) {
        const annotated = annotateResults(rawResults);
        const validBeforeDedup = annotated.filter(row => row.cleaned_status === 'Valid');
        const finalValid = Array.isArray(cleanedResults) ? cleanedResults : removeDuplicatesEnhanced(validBeforeDedup);
        const duplicatesRemoved = Math.max(0, validBeforeDedup.length - finalValid.length);

        const removedByReason = {
            missingData: annotated.filter(row => row.cleaned_status === 'Not Valid' && row.cleaner_reason === 'missing_required_data').length,
            notOperational: annotated.filter(row => row.cleaned_status === 'Not Valid' && row.cleaner_reason === 'business_not_operational').length,
            brandMismatch: annotated.filter(row => row.cleaned_status === 'Not Valid' && row.cleaner_reason === 'brand_missing_from_name').length,
            invalidPattern: annotated.filter(row => row.cleaned_status === 'Not Valid' && (
                row.cleaner_reason === 'non_store_or_landmark_identity' ||
                row.cleaner_reason === 'brand_specific_negative_pattern'
            )).length,
            wrongCategory: annotated.filter(row => row.cleaned_status === 'Not Valid' && row.cleaner_reason.indexOf('place_type_mismatch:') === 0).length,
            duplicates: duplicatesRemoved,
            reviewFlagged: finalValid.filter(row => row.review_flag === 'Yes').length
        };

        return {
            total: rawResults.length,
            cleaned: finalValid.length,
            removed: rawResults.length - finalValid.length,
            retentionRate: rawResults.length > 0 ? Math.round((finalValid.length / rawResults.length) * 100) : 0,
            removedByReason: removedByReason
        };
    }

    const api = {
        annotateResults: annotateResults,
        cleanResults: cleanResults,
        classifyResult: classifyResult,
        getCleaningStats: getCleaningStats
    };

    if (typeof module !== 'undefined' && module.exports) {
        module.exports = api;
    }

    globalScope.BrandCleaner = api;
}(typeof window !== 'undefined' ? window : globalThis));
