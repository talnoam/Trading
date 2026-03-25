# Trading Project

This project has:
- A **FastAPI backend** in `main.py`
- A **React frontend** in `stock-dashboard/`

## Prerequisites

- Python 3
- Node.js + npm

If `npm` is missing (`zsh: command not found: npm`), install Node first:

```bash
brew install node
exec zsh
node -v
npm -v
```

## Run the project

It is best to start the backend first, then the frontend.

### 1) Start backend (FastAPI)

From project root:

```bash
cd ~/Trading
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload
```

Backend runs at:
- `http://127.0.0.1:8000`
- API docs: `http://127.0.0.1:8000/docs`

### 2) Start frontend (React)

In a second terminal:

```bash
cd /Users/noamtal/python_projects/Trading/stock-dashboard
npm install
npm start
```

Frontend runs at:
- `http://localhost:3000`

## Quick verification

Test backend directly:

```bash
curl http://127.0.0.1:8000/stock/AAPL
```

If this returns JSON, backend is working.

## Run the Reddit comments fetch script

The script `data_fetch/fetch_reddit_comments.py` needs local credentials files.

### 1) Create `data_fetch/credentials.py`

Create `data_fetch/credentials.py` with your Reddit and Google Drive values:

```python
CLIENT_ID = "your_reddit_client_id"
CLIENT_SECRET = "your_reddit_client_secret"
USER_AGENT = "your_app_name/1.0 by u/your_username"
TRADING_FOLDER_ID = "your_google_drive_folder_id"
```

### 2) Add Google service account JSON

Place your Google service account file at:
- `Trading_Access.json` (project root)

### 3) Install required packages

```bash
source /Users/noamtal/python_projects/Trading/.venv/bin/activate
pip install -r /Users/noamtal/python_projects/Trading/requirements.txt
```

### 4) Run the script from parent directory

```bash
cd /Users/noamtal/python_projects
source Trading/.venv/bin/activate
python Trading/data_fetch/fetch_reddit_comments.py --interval 1
```

Note: run from the parent directory (`/Users/noamtal/python_projects`) because the script changes directory internally.

## Troubleshooting

### "Failed to fetch" in frontend

This usually means the frontend could not reach the backend.

Check:
- Backend is running on `127.0.0.1:8000`
- Frontend is running on `localhost:3000`
- In `stock-dashboard/src/StockDashboard.js`, fetch points to:
  - `http://127.0.0.1:8000/stock/${ticker}`

After backend is up:
- Hard refresh browser (`Cmd+Shift+R`)
- Change ticker (for example `AAPL` -> `MSFT` -> `AAPL`) to trigger a new fetch

### Start order

You can start either service first, but starting **backend first** avoids an initial fetch failure in the UI.
