import axios from 'axios';
import * as cheerio from 'cheerio';
import pool from '../config/database';

// ============================================================================
// INTERFACES
// ============================================================================

interface ScrapedBook {
    title: string;
    subtitle?: string;
    isbn?: string;
    publication_year?: number;
    publisher?: string;
    pages?: number;
    language?: string;
    description?: string;
    cover_image_url?: string;
    source_url?: string;
    authors: string[];
    categories: string[];
    pdf_url?: string;
    download_url?: string;
}

interface ScrapingResult {
    log_id: number;
    source_id: number;
    books_added: number;
    books_updated: number;
    errors: number;
    status: 'running' | 'completed' | 'failed';
    error_details?: string;
}

interface CustomSelectors {
    container: string;
    title: string;
    subtitle?: string;
    authors?: string;
    description?: string;
    coverImage?: string;
    link?: string;
    publisher?: string;
    year?: string;
    isbn?: string;
    categories?: string;
    pages?: string;
}

// ============================================================================
// SCRAPER SERVICE CLASS
// ============================================================================

class ScraperService {
    // ==========================================================================
    // UTILITY FUNCTIONS
    // ==========================================================================

    /**
     * Add delay between requests to be respectful to servers
     */
    private delay(ms: number): Promise<void> {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    /**
     * Clean and normalize text
     */
    private cleanText(text: string | undefined): string | undefined {
        if (!text) return undefined;
        return text.trim().replace(/\s+/g, ' ');
    }

    /**
     * Extract year from date string
     */
    private extractYear(dateString: string | undefined): number | undefined {
        if (!dateString) return undefined;
        const match = dateString.match(/\d{4}/);
        return match ? parseInt(match[0]) : undefined;
    }

    // ==========================================================================
    // DATABASE HELPER FUNCTIONS
    // ==========================================================================

    /**
     * Get or create a source in the database
     * @param sourceName - Name of the source (e.g., "Open Library")
     * @param baseUrl - Base URL of the source
     * @returns source_id
     */
    private async getOrCreateSource(sourceName: string, baseUrl: string): Promise<number> {
        const client = await pool.connect();
        try {
            // Check if source exists
            let result = await client.query(
                'SELECT source_id FROM sources WHERE source_name = $1',
                [sourceName]
            );

            if (result.rows.length > 0) {
                console.log(`Found existing source: ${sourceName} (ID: ${result.rows[0].source_id})`);
                return result.rows[0].source_id;
            }

            // Create new source
            result = await client.query(
                `INSERT INTO sources (source_name, base_url, is_active) 
         VALUES ($1, $2, TRUE) 
         RETURNING source_id`,
                [sourceName, baseUrl]
            );

            console.log(`Created new source: ${sourceName} (ID: ${result.rows[0].source_id})`);
            return result.rows[0].source_id;
        } catch (error) {
            console.error('Error in getOrCreateSource:', error);
            throw error;
        } finally {
            client.release();
        }
    }

    /**
     * Create a scraping log entry
     * @param sourceId - ID of the source being scraped
     * @returns log_id
     */
    private async createScrapingLog(sourceId: number): Promise<number> {
        try {
            const result = await pool.query(
                `INSERT INTO scraping_logs (source_id, status, started_at) 
         VALUES ($1, 'running', CURRENT_TIMESTAMP) 
         RETURNING log_id`,
                [sourceId]
            );
            console.log(`Created scraping log (ID: ${result.rows[0].log_id})`);
            return result.rows[0].log_id;
        } catch (error) {
            console.error('Error creating scraping log:', error);
            throw error;
        }
    }

    /**
     * Update scraping log with results
     * @param logId - ID of the log to update
     * @param data - Data to update
     */
    private async updateScrapingLog(
        logId: number,
        data: {
            completed_at?: Date;
            books_added?: number;
            books_updated?: number;
            errors?: number;
            status?: 'running' | 'completed' | 'failed';
            error_details?: string;
        }
    ): Promise<void> {
        try {
            const updates: string[] = [];
            const values: any[] = [];
            let paramCount = 1;

            if (data.completed_at !== undefined) {
                updates.push(`completed_at = $${paramCount++}`);
                values.push(data.completed_at);
            }
            if (data.books_added !== undefined) {
                updates.push(`books_added = $${paramCount++}`);
                values.push(data.books_added);
            }
            if (data.books_updated !== undefined) {
                updates.push(`books_updated = $${paramCount++}`);
                values.push(data.books_updated);
            }
            if (data.errors !== undefined) {
                updates.push(`errors = $${paramCount++}`);
                values.push(data.errors);
            }
            if (data.status) {
                updates.push(`status = $${paramCount++}`);
                values.push(data.status);
            }
            if (data.error_details) {
                updates.push(`error_details = $${paramCount++}`);
                values.push(data.error_details);
            }

            if (updates.length === 0) return;

            values.push(logId);

            await pool.query(
                `UPDATE scraping_logs SET ${updates.join(', ')} WHERE log_id = $${paramCount}`,
                values
            );

            console.log(`Updated scraping log (ID: ${logId}) - Status: ${data.status}`);
        } catch (error) {
            console.error('Error updating scraping log:', error);
            throw error;
        }
    }

    /**
     * Get or create an author
     * @param authorName - Name of the author
     * @param client - Database client (for transaction support)
     * @returns author_id
     */
    private async getOrCreateAuthor(authorName: string, client: any): Promise<number> {
        try {
            // Check if author exists
            let result = await client.query(
                'SELECT author_id FROM authors WHERE author_name = $1',
                [authorName]
            );

            if (result.rows.length > 0) {
                return result.rows[0].author_id;
            }

            // Create new author
            result = await client.query(
                'INSERT INTO authors (author_name) VALUES ($1) RETURNING author_id',
                [authorName]
            );

            return result.rows[0].author_id;
        } catch (error) {
            console.error('Error in getOrCreateAuthor:', error);
            throw error;
        }
    }

    /**
     * Get or create a category
     * @param categoryName - Name of the category
     * @param client - Database client (for transaction support)
     * @returns category_id
     */
    private async getOrCreateCategory(categoryName: string, client: any): Promise<number> {
        try {
            // Check if category exists
            let result = await client.query(
                'SELECT category_id FROM categories WHERE category_name = $1',
                [categoryName]
            );

            if (result.rows.length > 0) {
                return result.rows[0].category_id;
            }

            // Create new category
            result = await client.query(
                'INSERT INTO categories (category_name) VALUES ($1) RETURNING category_id',
                [categoryName]
            );

            return result.rows[0].category_id;
        } catch (error) {
            console.error('Error in getOrCreateCategory:', error);
            throw error;
        }
    }

    // ==========================================================================
    // SCRAPING FUNCTIONS
    // ==========================================================================

    /**
     * Scrape books from Open Library
     * @param subject - Subject/category to search for
     * @param limit - Maximum number of books to scrape
     * @returns Array of scraped books
     */
    async scrapeOpenLibrary(subject: string, limit: number = 20): Promise<ScrapedBook[]> {
        try {
            const url = `https://openlibrary.org/subjects/${subject}.json?limit=${limit}`;
            console.log(`\n📚 Scraping Open Library...`);
            console.log(`URL: ${url}`);

            const response = await axios.get(url, {
                timeout: 15000,
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
            });

            const data = response.data;
            const books: ScrapedBook[] = [];

            console.log(`Found ${data.works?.length || 0} works`);

            for (const work of data.works || []) {
                try {
                    // Extract description (can be string or object)
                    let description: string | undefined;
                    if (typeof work.description === 'string') {
                        description = work.description;
                    } else if (work.description?.value) {
                        description = work.description.value;
                    }

                    const book: ScrapedBook = {
                        title: this.cleanText(work.title) || 'Unknown Title',
                        publication_year: work.first_publish_year,
                        description: this.cleanText(description),
                        cover_image_url: work.cover_id
                            ? `https://covers.openlibrary.org/b/id/${work.cover_id}-L.jpg`
                            : undefined,
                        source_url: work.key ? `https://openlibrary.org${work.key}` : undefined,
                        language: 'English',
                        authors: work.authors?.map((a: any) => this.cleanText(a.name)).filter(Boolean) || [],
                        categories: [subject],
                        pdf_url: work.pdf_url,  // If available from API
                        download_url: work.download_url,
                    };

                    if (book.title && book.title !== 'Unknown Title') {
                        books.push(book);
                        console.log(`  ✓ ${book.title} (${book.authors.join(', ')})`);
                    }
                } catch (error) {
                    console.error('  ✗ Error parsing Open Library work:', error);
                }
            }

            console.log(`✅ Successfully scraped ${books.length} books from Open Library\n`);
            return books;
        } catch (error: any) {
            console.error('❌ Error scraping Open Library:', error.message);
            throw new Error(`Open Library scraping failed: ${error.message}`);
        }
    }

    /**
     * Scrape books from Google Books API
     * @param query - Search query
     * @param maxResults - Maximum number of results
     * @returns Array of scraped books
     */
    async scrapeGoogleBooks(query: string, maxResults: number = 20): Promise<ScrapedBook[]> {
        try {
            const apiKey = process.env.GOOGLE_BOOKS_API_KEY || '';
            const url = `https://www.googleapis.com/books/v1/volumes?q=${encodeURIComponent(query)}&maxResults=${maxResults}${apiKey ? `&key=${apiKey}` : ''}`;

            console.log(`\n📚 Scraping Google Books...`);
            console.log(`Query: ${query}`);

            const response = await axios.get(url, { timeout: 15000 });
            const data = response.data;
            const books: ScrapedBook[] = [];

            console.log(`Found ${data.totalItems || 0} total items, processing ${data.items?.length || 0}`);

            for (const item of data.items || []) {
                try {
                    const volumeInfo = item.volumeInfo;

                    // Extract ISBN
                    let isbn: string | undefined;
                    if (volumeInfo.industryIdentifiers) {
                        const isbn13 = volumeInfo.industryIdentifiers.find((id: any) => id.type === 'ISBN_13');
                        const isbn10 = volumeInfo.industryIdentifiers.find((id: any) => id.type === 'ISBN_10');
                        isbn = isbn13?.identifier || isbn10?.identifier;
                    }

                    const book: ScrapedBook = {
                        title: this.cleanText(volumeInfo.title) || 'Unknown Title',
                        subtitle: this.cleanText(volumeInfo.subtitle),
                        isbn: isbn,
                        publication_year: volumeInfo.publishedDate
                            ? this.extractYear(volumeInfo.publishedDate)
                            : undefined,
                        publisher: this.cleanText(volumeInfo.publisher),
                        pages: volumeInfo.pageCount,
                        language: volumeInfo.language || 'en',
                        description: this.cleanText(volumeInfo.description),
                        cover_image_url: volumeInfo.imageLinks?.thumbnail || volumeInfo.imageLinks?.smallThumbnail,
                        source_url: volumeInfo.canonicalVolumeLink || volumeInfo.previewLink || volumeInfo.infoLink,
                        authors: volumeInfo.authors?.map((a: string) => this.cleanText(a)).filter(Boolean) || [],
                        categories: volumeInfo.categories?.map((c: string) => this.cleanText(c)).filter(Boolean) || [],
                    };

                    if (book.title && book.title !== 'Unknown Title') {
                        books.push(book);
                        console.log(`  ✓ ${book.title} (${book.authors.join(', ')})`);
                    }
                } catch (error) {
                    console.error('  ✗ Error parsing Google Books item:', error);
                }
            }

            console.log(`✅ Successfully scraped ${books.length} books from Google Books\n`);
            return books;
        } catch (error: any) {
            console.error('❌ Error scraping Google Books:', error.message);
            throw new Error(`Google Books scraping failed: ${error.message}`);
        }
    }

    /**
     * Scrape books from Project Gutenberg
     * @param searchTerm - Search term
     * @returns Array of scraped books
     */
    async scrapeProjectGutenberg(searchTerm: string = 'science'): Promise<ScrapedBook[]> {
        try {
            const url = `https://www.gutenberg.org/ebooks/search/?query=${encodeURIComponent(searchTerm)}`;
            console.log(`\n📚 Scraping Project Gutenberg...`);
            console.log(`URL: ${url}`);

            const response = await axios.get(url, {
                timeout: 15000,
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
            });

            const $ = cheerio.load(response.data);
            const books: ScrapedBook[] = [];

            $('.booklink').each((index, element) => {
                try {
                    const $book = $(element);
                    const title = this.cleanText($book.find('.title').text());
                    const bookUrl = $book.find('a.link').attr('href');
                    const authorsText = this.cleanText($book.find('.subtitle').text());

                    // Extract author name (remove "by " prefix)
                    let authors: string[] = [];
                    if (authorsText) {
                        const authorName = authorsText.replace(/^by\s+/i, '').trim();
                        if (authorName) {
                            authors = [authorName];
                        }
                    }

                    const book: ScrapedBook = {
                        title: title || 'Unknown Title',
                        source_url: bookUrl ? `https://www.gutenberg.org${bookUrl}` : undefined,
                        language: 'English',
                        authors: authors,
                        categories: [searchTerm],
                    };

                    if (book.title && book.title !== 'Unknown Title') {
                        books.push(book);
                        console.log(`  ✓ ${book.title} (${book.authors.join(', ')})`);
                    }
                } catch (error) {
                    console.error('  ✗ Error parsing Gutenberg book:', error);
                }
            });

            console.log(`✅ Successfully scraped ${books.length} books from Project Gutenberg\n`);
            return books.slice(0, 20); // Limit to 20 books
        } catch (error: any) {
            console.error('❌ Error scraping Project Gutenberg:', error.message);
            throw new Error(`Project Gutenberg scraping failed: ${error.message}`);
        }
    }

    /**
     * Scrape books from a custom website
     * @param url - URL to scrape
     * @param selectors - CSS selectors for extracting data
     * @returns Array of scraped books
     */
    async scrapeCustomWebsite(url: string, selectors: CustomSelectors): Promise<ScrapedBook[]> {
        try {
            console.log(`\n📚 Scraping custom website...`);
            console.log(`URL: ${url}`);

            const response = await axios.get(url, {
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                },
                timeout: 15000,
            });

            const $ = cheerio.load(response.data);
            const books: ScrapedBook[] = [];

            console.log(`Looking for elements: ${selectors.container}`);

            $(selectors.container).each((index, element) => {
                try {
                    const $item = $(element);

                    // Extract basic info
                    const title = this.cleanText($item.find(selectors.title).text());
                    const subtitle = selectors.subtitle
                        ? this.cleanText($item.find(selectors.subtitle).text())
                        : undefined;
                    const description = selectors.description
                        ? this.cleanText($item.find(selectors.description).text())
                        : undefined;

                    // Extract image
                    let coverImageUrl: string | undefined;
                    if (selectors.coverImage) {
                        const imgSrc = $item.find(selectors.coverImage).attr('src');
                        if (imgSrc) {
                            // Make absolute URL if relative
                            coverImageUrl = imgSrc.startsWith('http')
                                ? imgSrc
                                : new URL(imgSrc, url).href;
                        }
                    }

                    // Extract link
                    let sourceUrl: string | undefined;
                    if (selectors.link) {
                        const linkHref = $item.find(selectors.link).attr('href');
                        if (linkHref) {
                            sourceUrl = linkHref.startsWith('http')
                                ? linkHref
                                : new URL(linkHref, url).href;
                        }
                    }

                    // Extract publisher
                    const publisher = selectors.publisher
                        ? this.cleanText($item.find(selectors.publisher).text())
                        : undefined;

                    // Extract year
                    let publicationYear: number | undefined;
                    if (selectors.year) {
                        const yearText = $item.find(selectors.year).text();
                        publicationYear = this.extractYear(yearText);
                    }

                    // Extract ISBN
                    const isbn = selectors.isbn
                        ? this.cleanText($item.find(selectors.isbn).text())
                        : undefined;

                    // Extract pages
                    let pages: number | undefined;
                    if (selectors.pages) {
                        const pagesText = $item.find(selectors.pages).text();
                        const pagesMatch = pagesText.match(/\d+/);
                        pages = pagesMatch ? parseInt(pagesMatch[0]) : undefined;
                    }

                    // Extract authors
                    const authors: string[] = [];
                    if (selectors.authors) {
                        $item.find(selectors.authors).each((i, el) => {
                            const authorName = this.cleanText($(el).text());
                            if (authorName) authors.push(authorName);
                        });
                    }

                    // Extract categories
                    const categories: string[] = [];
                    if (selectors.categories) {
                        $item.find(selectors.categories).each((i, el) => {
                            const categoryName = this.cleanText($(el).text());
                            if (categoryName) categories.push(categoryName);
                        });
                    }

                    const book: ScrapedBook = {
                        title: title || 'Unknown Title',
                        subtitle,
                        description,
                        cover_image_url: coverImageUrl,
                        source_url: sourceUrl,
                        publisher,
                        publication_year: publicationYear,
                        isbn,
                        pages,
                        language: 'English',
                        authors,
                        categories,
                    };

                    if (book.title && book.title !== 'Unknown Title') {
                        books.push(book);
                        console.log(`  ✓ ${book.title} (${book.authors.join(', ')})`);
                    }
                } catch (error) {
                    console.error('  ✗ Error parsing custom website book:', error);
                }
            });

            console.log(`✅ Successfully scraped ${books.length} books from custom website\n`);
            return books;
        } catch (error: any) {
            console.error('❌ Error scraping custom website:', error.message);
            throw new Error(`Custom website scraping failed: ${error.message}`);
        }
    }

    // ==========================================================================
    // DATABASE SAVE FUNCTION
    // ==========================================================================

    /**
     * Save scraped books to database
     * @param books - Array of scraped books
     * @param sourceId - ID of the source
     * @returns Object with count of added and updated books
     */
    async saveBooks(books: ScrapedBook[], sourceId: number): Promise<{ added: number; updated: number }> {
        const client = await pool.connect();
        let booksAdded = 0;
        let booksUpdated = 0;

        console.log(`\n💾 Saving ${books.length} books to database...`);

        try {
            await client.query('BEGIN');

            for (const book of books) {
                try {
                    // ====================================================================
                    // STEP 1: Check if book exists
                    // ====================================================================
                    let existingBook;

                    // First try by ISBN (most reliable)
                    if (book.isbn) {
                        const result = await client.query(
                            'SELECT book_id FROM books WHERE isbn = $1',
                            [book.isbn]
                        );
                        existingBook = result.rows[0];
                    }

                    // If not found by ISBN, try by title + year
                    if (!existingBook && book.title && book.publication_year) {
                        const result = await client.query(
                            'SELECT book_id FROM books WHERE title = $1 AND publication_year = $2',
                            [book.title, book.publication_year]
                        );
                        existingBook = result.rows[0];
                    }

                    // If still not found, try by title only (less reliable)
                    if (!existingBook && book.title) {
                        const result = await client.query(
                            'SELECT book_id FROM books WHERE title = $1',
                            [book.title]
                        );
                        existingBook = result.rows[0];
                    }

                    let bookId: number;

                    // ====================================================================
                    // STEP 2A: Update existing book
                    // ====================================================================
                    if (existingBook) {
                        await client.query(
                            `UPDATE books SET 
                subtitle = COALESCE($1, subtitle),
                publisher = COALESCE($2, publisher),
                pages = COALESCE($3, pages),
                description = COALESCE($4, description),
                cover_image_url = COALESCE($5, cover_image_url),
                isbn = COALESCE($6, isbn),
                last_updated = CURRENT_TIMESTAMP
               WHERE book_id = $7`,
                            [
                                book.subtitle,
                                book.publisher,
                                book.pages,
                                book.description,
                                book.cover_image_url,
                                book.isbn,
                                existingBook.book_id,
                            ]
                        );
                        bookId = existingBook.book_id;
                        booksUpdated++;
                        console.log(`  ↻ Updated: ${book.title}`);
                    }
                    // ====================================================================
                    // STEP 2B: Insert new book
                    // ====================================================================
                    else {
                        // In saveBooks function, add to INSERT:
                        const result = await client.query(
                            `INSERT INTO books 
   (title, subtitle, isbn, publication_year, publisher, pages, language, 
    description, cover_image_url, source_id, source_url, pdf_url, download_url)
   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
   RETURNING book_id`,
                            [
                                book.title,
                                book.subtitle,
                                book.isbn,
                                book.publication_year,
                                book.publisher,
                                book.pages,
                                book.language || 'English',
                                book.description,
                                book.cover_image_url,
                                sourceId,
                                book.source_url,
                                book.pdf_url,      
                                book.download_url, 
                            ]
                        );
                        bookId = result.rows[0].book_id;
                        booksAdded++;
                        console.log(`  + Added: ${book.title}`);
                    }

                    // ====================================================================
                    // STEP 3: Link authors
                    // ====================================================================
                    if (book.authors && book.authors.length > 0) {
                        // First, remove existing author links for this book
                        await client.query(
                            'DELETE FROM book_authors WHERE book_id = $1',
                            [bookId]
                        );

                        // Then add new author links
                        for (let i = 0; i < book.authors.length; i++) {
                            const authorName = book.authors[i];
                            if (authorName && authorName.trim()) {
                                const authorId = await this.getOrCreateAuthor(authorName.trim(), client);

                                await client.query(
                                    `INSERT INTO book_authors (book_id, author_id, author_position)
                   VALUES ($1, $2, $3)
                   ON CONFLICT (book_id, author_id) 
                   DO UPDATE SET author_position = $3`,
                                    [bookId, authorId, i + 1]
                                );
                            }
                        }
                    }

                    // ====================================================================
                    // STEP 4: Link categories
                    // ====================================================================
                    if (book.categories && book.categories.length > 0) {
                        for (const categoryName of book.categories) {
                            if (categoryName && categoryName.trim()) {
                                const categoryId = await this.getOrCreateCategory(categoryName.trim(), client);

                                await client.query(
                                    `INSERT INTO book_categories (book_id, category_id)
                   VALUES ($1, $2)
                   ON CONFLICT (book_id, category_id) DO NOTHING`,
                                    [bookId, categoryId]
                                );
                            }
                        }
                    }
                } catch (error) {
                    console.error(`  ✗ Error saving book "${book.title}":`, error);
                    // Continue with next book instead of failing entire batch
                }
            }

            await client.query('COMMIT');

            // Update source last_scraped timestamp
            await client.query(
                'UPDATE sources SET last_scraped = CURRENT_TIMESTAMP WHERE source_id = $1',
                [sourceId]
            );

            console.log(`\n✅ Database save complete:`);
            console.log(`   ${booksAdded} books added`);
            console.log(`   ${booksUpdated} books updated\n`);

            return { added: booksAdded, updated: booksUpdated };
        } catch (error) {
            await client.query('ROLLBACK');
            console.error('❌ Error in saveBooks transaction:', error);
            throw error;
        } finally {
            client.release();
        }
    }

    // ==========================================================================
    // MAIN ORCHESTRATOR FUNCTIONS
    // ==========================================================================

    /**
     * Main function to scrape and save books
     * @param type - Type of scraping source
     * @param query - Search query or subject
     * @param sourceName - Name of the source
     * @param baseUrl - Base URL of the source
     * @param customSelectors - Optional custom selectors for custom websites
     * @returns Scraping result with statistics
     */
    async scrapeAndSave(
        type: 'openlibrary' | 'googlebooks' | 'gutenberg' | 'custom',
        query: string,
        sourceName: string,
        baseUrl: string,
        customSelectors?: CustomSelectors
    ): Promise<ScrapingResult> {
        let logId: number | null = null;
        let sourceId: number | null = null;

        console.log(`\n${'='.repeat(70)}`);
        console.log(`🚀 Starting scraping process`);
        console.log(`   Type: ${type}`);
        console.log(`   Query: ${query}`);
        console.log(`   Source: ${sourceName}`);
        console.log(`${'='.repeat(70)}`);

        try {
            // ======================================================================
            // STEP 1: Get or create source
            // ======================================================================
            sourceId = await this.getOrCreateSource(sourceName, baseUrl);

            // ======================================================================
            // STEP 2: Create scraping log
            // ======================================================================
            logId = await this.createScrapingLog(sourceId);

            // ======================================================================
            // STEP 3: Scrape based on type
            // ======================================================================
            let books: ScrapedBook[] = [];

            switch (type) {
                case 'openlibrary':
                    books = await this.scrapeOpenLibrary(query, 20);
                    break;
                case 'googlebooks':
                    books = await this.scrapeGoogleBooks(query, 20);
                    break;
                case 'gutenberg':
                    books = await this.scrapeProjectGutenberg(query);
                    break;
                case 'custom':
                    if (!customSelectors) {
                        throw new Error('Custom selectors required for custom scraping');
                    }
                    books = await this.scrapeCustomWebsite(query, customSelectors);
                    break;
                default:
                    throw new Error(`Unknown scraping type: ${type}`);
            }

            // ======================================================================
            // STEP 4: Save books to database
            // ======================================================================
            const { added, updated } = await this.saveBooks(books, sourceId);

            // ======================================================================
            // STEP 5: Update scraping log with success
            // ======================================================================
            await this.updateScrapingLog(logId, {
                completed_at: new Date(),
                books_added: added,
                books_updated: updated,
                status: 'completed',
            });

            console.log(`${'='.repeat(70)}`);
            console.log(`✅ Scraping completed successfully!`);
            console.log(`${'='.repeat(70)}\n`);

            return {
                log_id: logId,
                source_id: sourceId,
                books_added: added,
                books_updated: updated,
                errors: 0,
                status: 'completed',
            };
        } catch (error: any) {
            console.error(`\n${'='.repeat(70)}`);
            console.error('❌ Scraping failed:', error.message);
            console.error(`${'='.repeat(70)}\n`);

            // Update scraping log with failure
            if (logId) {
                await this.updateScrapingLog(logId, {
                    completed_at: new Date(),
                    errors: 1,
                    status: 'failed',
                    error_details: error.message,
                });
            }

            return {
                log_id: logId || 0,
                source_id: sourceId || 0,
                books_added: 0,
                books_updated: 0,
                errors: 1,
                status: 'failed',
                error_details: error.message,
            };
        }
    }

    /**
     * Scrape multiple sources in sequence
     * @param sources - Array of source configurations
     * @returns Array of scraping results
     */
    async scrapeMultipleSources(sources: Array<{
        type: 'openlibrary' | 'googlebooks' | 'gutenberg' | 'custom';
        query: string;
        sourceName: string;
        baseUrl: string;
        customSelectors?: CustomSelectors;
    }>): Promise<ScrapingResult[]> {
        const results: ScrapingResult[] = [];

        console.log(`\n${'='.repeat(70)}`);
        console.log(`🔄 Scraping ${sources.length} sources...`);
        console.log(`${'='.repeat(70)}\n`);

        for (let i = 0; i < sources.length; i++) {
            const source = sources[i];
            console.log(`\n[${i + 1}/${sources.length}] Processing: ${source.sourceName}`);

            try {
                const result = await this.scrapeAndSave(
                    source.type,
                    source.query,
                    source.sourceName,
                    source.baseUrl,
                    source.customSelectors
                );
                results.push(result);

                // Add delay between sources to be respectful
                if (i < sources.length - 1) {
                    console.log(`⏳ Waiting 2 seconds before next source...`);
                    await this.delay(2000);
                }
            } catch (error: any) {
                console.error(`Failed to scrape ${source.sourceName}:`, error.message);
                results.push({
                    log_id: 0,
                    source_id: 0,
                    books_added: 0,
                    books_updated: 0,
                    errors: 1,
                    status: 'failed',
                    error_details: error.message,
                });
            }
        }

        // Print summary
        console.log(`\n${'='.repeat(70)}`);
        console.log(`📊 SCRAPING SUMMARY`);
        console.log(`${'='.repeat(70)}`);

        const totalAdded = results.reduce((sum, r) => sum + r.books_added, 0);
        const totalUpdated = results.reduce((sum, r) => sum + r.books_updated, 0);
        const totalErrors = results.reduce((sum, r) => sum + r.errors, 0);
        const successfulSources = results.filter(r => r.status === 'completed').length;

        console.log(`   Sources processed: ${sources.length}`);
        console.log(`   Successful: ${successfulSources}`);
        console.log(`   Failed: ${sources.length - successfulSources}`);
        console.log(`   Books added: ${totalAdded}`);
        console.log(`   Books updated: ${totalUpdated}`);
        console.log(`   Total errors: ${totalErrors}`);
        console.log(`${'='.repeat(70)}\n`);

        return results;
    }

    /**
     * Get scraping statistics from logs
     * @param limit - Number of recent logs to retrieve
     * @returns Array of scraping logs
     */
    async getScrapingStats(limit: number = 10): Promise<any[]> {
        try {
            const result = await pool.query(
                `SELECT sl.*, s.source_name, s.base_url
         FROM scraping_logs sl
         JOIN sources s ON sl.source_id = s.source_id
         ORDER BY sl.started_at DESC
         LIMIT $1`,
                [limit]
            );
            return result.rows;
        } catch (error) {
            console.error('Error fetching scraping stats:', error);
            throw error;
        }
    }

    /**
     * Get active sources
     * @returns Array of active sources
     */
    async getActiveSources(): Promise<any[]> {
        try {
            const result = await pool.query(
                `SELECT * FROM sources WHERE is_active = TRUE ORDER BY source_name`
            );
            return result.rows;
        } catch (error) {
            console.error('Error fetching active sources:', error);
            throw error;
        }
    }
}

// ============================================================================
// EXPORT SINGLETON INSTANCE
// ============================================================================

export default new ScraperService();