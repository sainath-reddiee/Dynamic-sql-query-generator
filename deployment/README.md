# 🚀 Production Deployment Guide

This guide covers multiple deployment options for the JSON-to-SQL Analyzer Streamlit application.

## 📁 Project Structure

```
/
├── streamlit_app.py              # Main entry point
├── requirements.txt              # Python dependencies
├── runtime.txt                   # Python version for Streamlit Cloud
├── Dockerfile                    # Docker configuration
├── docker-compose.yml            # Docker Compose setup
├── .env.example                  # Environment variables template
├── src/                          # Application source code
│   ├── __init__.py
│   ├── main.py                   # Main Streamlit app
│   ├── json_analyzer.py          # JSON analysis logic
│   ├── utils.py                  # Utility functions
│   ├── sql_generator.py          # SQL generation
│   ├── config.py                 # Configuration management
│   └── health_check.py           # Health monitoring
├── config/                       # Configuration files
│   ├── streamlit/
│   │   └── config.toml           # Streamlit settings
│   └── logging.conf              # Logging configuration
├── assets/                       # Static assets
│   └── sample_data/
├── logs/                         # Application logs
├── deployment/                   # Deployment documentation
└── scripts/                      # Snowflake stored procedures
```

## 🌐 Deployment Options

### 1. Streamlit Cloud (Recommended for Demo/Dev)

**Pros:**
- Free hosting
- Automatic deployments from GitHub
- Built-in SSL/HTTPS
- No server management

**Steps:**
1. Push code to GitHub repository
2. Go to [share.streamlit.io](https://share.streamlit.io/)
3. Connect your GitHub repository
4. Set main file: `streamlit_app.py`
5. Deploy!

**Configuration:**
- Uses `streamlit_app.py` as entry point
- Reads dependencies from `requirements.txt`
- Python version from `runtime.txt`

### 2. Docker Deployment (Recommended for Production)

**Pros:**
- Consistent environment
- Easy scaling
- Production-ready
- Full control over resources

**Single Container:**
```bash
# Build the image
docker build -t json-sql-analyzer .

# Run the container
docker run -p 8501:8501 \
  -v $(pwd)/logs:/app/logs \
  -e ENVIRONMENT=production \
  json-sql-analyzer
```

**Docker Compose (Development):**
```bash
# Start the application
docker-compose up -d

# View logs
docker-compose logs -f

# Stop the application
docker-compose down
```

**Docker Compose (Production with Nginx):**
```bash
# Start with production profile
docker-compose --profile production up -d

# This includes nginx reverse proxy
# Configure SSL certificates in ./certs/
```

### 3. Traditional Server Deployment

**Requirements:**
- Python 3.11+
- 2GB+ RAM
- 10GB+ disk space

**Steps:**
```bash
# Clone repository
git clone <your-repo-url>
cd <repo-name>

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt

# Set environment variables
cp .env.example .env
# Edit .env with your settings

# Run the application
streamlit run streamlit_app.py --server.port 8501 --server.address 0.0.0.0
```

### 4. Cloud Platforms

#### AWS EC2
1. Launch EC2 instance (t3.medium or larger)
2. Install Docker and docker-compose
3. Clone repository and deploy with Docker

#### Google Cloud Run
```bash
# Build and deploy
gcloud builds submit --tag gcr.io/PROJECT-ID/json-sql-analyzer
gcloud run deploy --image gcr.io/PROJECT-ID/json-sql-analyzer --platform managed
```

#### Heroku
```bash
# Create Heroku app
heroku create your-app-name

# Set buildpack
heroku buildpacks:set heroku/python

# Deploy
git push heroku main
```

## 🔧 Configuration

### Environment Variables

Copy `.env.example` to `.env` and configure:

```bash
# Application Settings
APP_NAME="JSON-to-SQL Analyzer for Snowflake"
APP_VERSION="1.0.0"
DEBUG=false

# Performance
MAX_UPLOAD_SIZE_MB=200
JSON_ANALYSIS_MAX_DEPTH=20
CACHE_TTL_SECONDS=3600

# Security (add if needed)
# SECRET_KEY=your-secret-key
```

### Streamlit Configuration

Edit `config/streamlit/config.toml`:

```toml
[server]
port = 8501
enableCORS = false
enableXsrfProtection = true

[browser]
gatherUsageStats = false

[theme]
primaryColor = "#1f77b4"
```

## 📊 Monitoring

### Health Check Endpoints

- **Application Health**: `http://localhost:8501/_stcore/health`
- **Custom Health Dashboard**: Add `?page=health` to your app URL

### Logging

Logs are written to:
- Console output (INFO level)
- File: `logs/app.log` (configurable)

### Metrics

Monitor these metrics:
- CPU usage
- Memory usage
- Disk space
- Response times
- Error rates

## 🔒 Security Considerations

### Production Security

1. **Use HTTPS**: Always use SSL/TLS in production
2. **Environment Variables**: Store secrets in environment variables
3. **User Permissions**: Run containers with non-root user
4. **Network Security**: Use firewalls and security groups
5. **Updates**: Keep dependencies updated

### Example Nginx Configuration

```nginx
server {
    listen 80;
    server_name your-domain.com;
    
    location / {
        proxy_pass http://streamlit-app:8501;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # WebSocket support
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

## 🔄 CI/CD Pipeline

### GitHub Actions Example

```yaml
name: Deploy to Production

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      
      - name: Build and push Docker image
        run: |
          docker build -t your-registry/json-sql-analyzer .
          docker push your-registry/json-sql-analyzer
      
      - name: Deploy to server
        run: |
          # SSH to server and update containers
          ssh user@server 'docker-compose pull && docker-compose up -d'
```

## 🐛 Troubleshooting

### Common Issues

1. **Import Errors**: Check Python path and module structure
2. **Memory Issues**: Increase container memory limits
3. **Port Conflicts**: Change port in configuration
4. **Permission Errors**: Check file permissions and user context

### Debug Mode

Enable debug mode by setting `DEBUG=true` in `.env`:
- Shows detailed error messages
- Displays health dashboard debug info
- Enables verbose logging

## 📞 Support

For deployment issues:
1. Check logs in `logs/app.log`
2. Review health dashboard at `/health`
3. Verify environment variables
4. Check Docker/container logs

## 🔄 Updates

To update the application:

1. **Streamlit Cloud**: Push to GitHub repository
2. **Docker**: Rebuild and restart containers
3. **Traditional**: Pull updates and restart service

```bash
# Docker update
docker-compose pull
docker-compose up -d

# Traditional update
git pull
pip install -r requirements.txt
# Restart service
```