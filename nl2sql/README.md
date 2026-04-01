# NL2SQL - Natural Language to SQL

A robust text-to-SQL solution that generates complex SQL queries from natural language, 
with self-correction capabilities and PostgreSQL as the data source.

## Architecture

```
User (Any Language) → FastAPI Backend → Google Gemini LLM
                                            ↓
                              PostgreSQL Metadata (RAG)
                                            ↓
                                    SQL Generation
                                            ↓
                              PostgreSQL Execution + Validation
                                            ↓
                              Error? → Self-Correction Loop (up to 3x)
                                            ↓
                                    Results → User
```

## Features

- 🌍 **Multi-language input** - Ask questions in any language
- 🧠 **RAG with DB metadata** - Auto-fetches table/column info for accurate SQL
- 🔄 **Self-correction loop** - Automatically fixes SQL errors (up to 3 attempts)
- 🎯 **PostgreSQL native** - Direct execution and validation
- 🎨 **Clean professional UI** - AWS-style clean design

## Setup

1. **Install dependencies:**
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

2. **Configure environment:**
```bash
cp .env.example .env
# Edit .env with your settings
```

3. **Get a free Gemini API key:**
   - Go to https://aistudio.google.com/app/apikey
   - Create a key and add it to `.env`

4. **Run the application:**
```bash
python app.py
```

5. **Open in browser:**
   - http://localhost:8000

## How It Works

1. User enters a natural language question
2. System fetches relevant database metadata (tables, columns, types)
3. Metadata + question are sent to Gemini LLM with carefully crafted prompt
4. LLM generates SQL query
5. SQL is executed against PostgreSQL
6. If error occurs, error message is fed back to LLM for correction
7. Results are displayed in a clean table format
