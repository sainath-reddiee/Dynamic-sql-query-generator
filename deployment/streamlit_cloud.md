# Streamlit Cloud Deployment

## Prerequisites

1. GitHub repository with your code
2. Streamlit Cloud account (https://share.streamlit.io/)

## Deployment Steps

### 1. Repository Setup

Your repository should be structured as follows:
```
/
├── streamlit_app.py          # Main entry point (required)
├── requirements.txt          # Dependencies (required)
├── config/
│   └── streamlit/
│       └── config.toml       # Streamlit configuration
├── src/                      # Application modules
└── assets/                   # Static assets
```

### 2. Streamlit Cloud Configuration

1. **Main App File**: `streamlit_app.py` (Streamlit Cloud looks for this file)
2. **Python Version**: Python 3.11 (specified in runtime.txt if needed)
3. **Dependencies**: All listed in `requirements.txt`

### 3. Deploy to Streamlit Cloud

1. Go to https://share.streamlit.io/
2. Click "New app"
3. Connect your GitHub repository
4. Set main file path: `streamlit_app.py`
5. Choose branch: `main` or `master`
6. Click "Deploy!"

### 4. Environment Variables (if needed)

In Streamlit Cloud settings, you can add:
- `STREAMLIT_CONFIG_FILE=/app/config/streamlit/config.toml`
- Any other environment variables your app needs

### 5. Custom Domain (Optional)

You can configure a custom domain in the Streamlit Cloud settings.

## Important Notes

- Streamlit Cloud automatically detects `streamlit_app.py` as the main file
- The app will be available at: `https://yourappname-yourrepo-yourusername.streamlit.app/`
- Builds automatically on git push to the configured branch
- Free tier has some limitations on resources and concurrent users