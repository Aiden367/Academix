// In your Book.ts model file
export interface Book {
  book_id?: number;
  title: string;
  subtitle?: string;
  isbn?: string;
  publication_year?: number;
  publisher?: string;
  pages?: number;
  language?: string;
  description?: string;
  cover_image_url?: string;
  source_id?: number;
  source_url?: string;
  pdf_url?: string;        // ← Add this
  download_url?: string;   // ← Add this
  scraped_at?: Date;
  last_updated?: Date;
}