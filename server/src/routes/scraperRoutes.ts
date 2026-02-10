import { Router } from 'express';
import scraperController from '../controllers/scraperController';

const router = Router();

// Scrape from a single source
router.post('/scrape', scraperController.scrapeBooks);

// Scrape from multiple sources
router.post('/scrape-multiple', scraperController.scrapeMultipleSources);

// Get scraping statistics
router.get('/stats', scraperController.getStats);

// Get active sources
router.get('/sources', scraperController.getSources);

export default router;