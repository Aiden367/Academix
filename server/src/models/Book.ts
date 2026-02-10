export interface Book{
 book_id : number;
 category_id : number;
 source_id : number;
 title: string;
 subtitle: string;
 publication_year : number;
 publisher : string;
 pages : number;
 language: string;
 description: string;
 cover_image_url: string;
 source_url : string;
 scraped_at : Date;
 last_updated: Date;
}