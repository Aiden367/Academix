import { Request, Response } from 'express';
import scraperService from '../services/scraperService';

export class ScraperController {
    /**
     * Scrape books from a single source
     * POST /api/scraper/scrape
     */
    async scrapeBooks(req: Request, res: Response) {
        try {
            const { type, query, sourceName, baseUrl, customSelectors } = req.body;

            // Validation
            if (!type || !query || !sourceName || !baseUrl) {
                return res.status(400).json({
                    success: false,
                    error: 'Missing required fields: type, query, sourceName, baseUrl'
                });
            }

            // Validate type
            const validTypes = ['openlibrary', 'googlebooks', 'gutenberg', 'custom'];
            if (!validTypes.includes(type)) {
                return res.status(400).json({
                    success: false,
                    error: `Invalid type. Must be one of: ${validTypes.join(', ')}`
                });
            }

            // If custom type, require selectors
            if (type === 'custom' && !customSelectors) {
                return res.status(400).json({
                    success: false,
                    error: 'customSelectors required for custom scraping'
                });
            }

            const result = await scraperService.scrapeAndSave(
                type,
                query,
                sourceName,
                baseUrl,
                customSelectors
            );

            res.json({
                success: true,
                data: result
            });
        } catch (error: any) {
            console.error('Error in scrapeBooks:', error);
            res.status(500).json({
                success: false,
                error: error.message
            });
        }
    }

    /**
     * Scrape books from multiple sources
     * POST /api/scraper/scrape-multiple
     */
    async scrapeMultipleSources(req: Request, res: Response) {
        try {
            const { sources } = req.body;

            if (!sources || !Array.isArray(sources) || sources.length === 0) {
                return res.status(400).json({
                    success: false,
                    error: 'sources array is required and must not be empty'
                });
            }

            const results = await scraperService.scrapeMultipleSources(sources);

            res.json({
                success: true,
                data: results
            });
        } catch (error: any) {
            console.error('Error in scrapeMultipleSources:', error);
            res.status(500).json({
                success: false,
                error: error.message
            });
        }
    }

    /**
     * Get scraping statistics
     * GET /api/scraper/stats?limit=10
     */
    async getStats(req: Request, res: Response) {
        try {
            const limit = parseInt(req.query.limit as string) || 10;
            const stats = await scraperService.getScrapingStats(limit);

            res.json({
                success: true,
                data: stats
            });
        } catch (error: any) {
            console.error('Error in getStats:', error);
            res.status(500).json({
                success: false,
                error: error.message
            });
        }
    }

    /**
     * Get active sources
     * GET /api/scraper/sources
     */
    async getSources(req: Request, res: Response) {
        try {
            const sources = await scraperService.getActiveSources();

            res.json({
                success: true,
                data: sources
            });
        } catch (error: any) {
            console.error('Error in getSources:', error);
            res.status(500).json({
                success: false,
                error: error.message
            });
        }
    }
}

export default new ScraperController();