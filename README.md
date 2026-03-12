# Fyers Sector Momentum Strategy
Balfund Trading Private Limited

## Files
| File | Purpose |
|------|---------|
| `strategy.py` | Main strategy |
| `fyers_token.py` | Automated TOTP login |
| `fyers_connect.py` | Credentials + CLIENT_ID |
| `bundler.py` | Merges files before PyInstaller |
| `requirements.txt` | Dependencies |

## Run Locally (Mac/Windows)
```bash
pip install -r requirements.txt
python strategy.py
```

## Build EXE
Push to `main` → GitHub Actions builds automatically → Download from **Actions → Artifacts**
