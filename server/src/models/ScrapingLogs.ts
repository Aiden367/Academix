export interface ScrapingLogs{
    logs_id : number;
    source_id : number;
    started_at: Date;
    completed_at : Date;
    books_added: number;
    books_updated: number;
    errors : number;
    status: 'running' | 'completed' | 'failed';
    error_details: string;
}