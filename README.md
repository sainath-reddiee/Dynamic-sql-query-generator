<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dynamic SQL Generator - Feature Showcase</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
            padding: 20px;
        }
        
        .container {
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(20px);
            border-radius: 20px;
            padding: 40px;
            max-width: 1200px;
            width: 100%;
            box-shadow: 0 25px 50px rgba(0, 0, 0, 0.1);
            border: 1px solid rgba(255, 255, 255, 0.2);
        }
        
        .header {
            text-align: center;
            margin-bottom: 50px;
        }
        
        .logo-area {
            background: linear-gradient(135deg, #4f46e5, #7c3aed);
            color: white;
            padding: 20px;
            border-radius: 15px;
            margin-bottom: 20px;
            position: relative;
            overflow: hidden;
        }
        
        .logo-area::before {
            content: '';
            position: absolute;
            top: -50%;
            left: -50%;
            width: 200%;
            height: 200%;
            background: radial-gradient(circle, rgba(255,255,255,0.1) 0%, transparent 70%);
            animation: pulse 3s ease-in-out infinite;
        }
        
        @keyframes pulse {
            0%, 100% { transform: scale(1); opacity: 0.5; }
            50% { transform: scale(1.1); opacity: 0.8; }
        }
        
        .logo-area h1 {
            font-size: 2.5em;
            font-weight: 800;
            margin-bottom: 10px;
            position: relative;
            z-index: 2;
        }
        
        .tagline {
            font-size: 1.2em;
            color: rgba(255, 255, 255, 0.9);
            position: relative;
            z-index: 2;
        }
        
        .stats-row {
            display: flex;
            justify-content: center;
            gap: 30px;
            margin: 30px 0;
            flex-wrap: wrap;
        }
        
        .stat-box {
            background: linear-gradient(135deg, #10b981, #059669);
            color: white;
            padding: 15px 25px;
            border-radius: 12px;
            text-align: center;
            min-width: 120px;
            transform: translateY(0);
            transition: transform 0.3s ease;
        }
        
        .stat-box:hover {
            transform: translateY(-5px);
        }
        
        .stat-number {
            font-size: 2em;
            font-weight: bold;
            display: block;
        }
        
        .stat-label {
            font-size: 0.9em;
            opacity: 0.9;
        }
        
        .features-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 25px;
            margin: 40px 0;
        }
        
        .feature-card {
            background: white;
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.1);
            border-left: 5px solid;
            transition: all 0.3s ease;
            position: relative;
            overflow: hidden;
        }
        
        .feature-card::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 3px;
            background: linear-gradient(90deg, transparent, rgba(255,255,255,0.6), transparent);
            animation: shimmer 2s infinite;
        }
        
        @keyframes shimmer {
            0% { transform: translateX(-100%); }
            100% { transform: translateX(100%); }
        }
        
        .feature-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 20px 40px rgba(0, 0, 0, 0.15);
        }
        
        .mode-card { border-left-color: #3b82f6; }
        .export-card { border-left-color: #8b5cf6; }
        .smart-card { border-left-color: #f59e0b; }
        .performance-card { border-left-color: #ef4444; }
        .tech-card { border-left-color: #10b981; }
        .enterprise-card { border-left-color: #6366f1; }
        
        .feature-icon {
            font-size: 2.5em;
            margin-bottom: 15px;
            display: block;
        }
        
        .feature-title {
            font-size: 1.3em;
            font-weight: 700;
            margin-bottom: 15px;
            color: #1f2937;
        }
        
        .feature-desc {
            color: #6b7280;
            line-height: 1.6;
            margin-bottom: 15px;
        }
        
        .feature-highlights {
            list-style: none;
        }
        
        .feature-highlights li {
            color: #374151;
            margin: 8px 0;
            padding-left: 20px;
            position: relative;
        }
        
        .feature-highlights li::before {
            content: '✓';
            position: absolute;
            left: 0;
            color: #10b981;
            font-weight: bold;
        }
        
        .workflow-section {
            background: linear-gradient(135deg, #f8fafc, #e2e8f0);
            border-radius: 15px;
            padding: 30px;
            margin: 40px 0;
            text-align: center;
        }
        
        .workflow-title {
            font-size: 1.8em;
            font-weight: 700;
            margin-bottom: 30px;
            color: #1f2937;
        }
        
        .workflow-steps {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 20px;
            flex-wrap: wrap;
        }
        
        .workflow-step {
            flex: 1;
            min-width: 200px;
            position: relative;
        }
        
        .step-circle {
            width: 80px;
            height: 80px;
            border-radius: 50%;
            background: linear-gradient(135deg, #4f46e5, #7c3aed);
            color: white;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.5em;
            font-weight: bold;
            margin: 0 auto 15px;
            box-shadow: 0 10px 25px rgba(79, 70, 229, 0.3);
        }
        
        .step-title {
            font-weight: 600;
            margin-bottom: 10px;
            color: #1f2937;
        }
        
        .step-desc {
            color: #6b7280;
            font-size: 0.9em;
        }
        
        .arrow {
            font-size: 2em;
            color: #9ca3af;
            margin: 0 10px;
        }
        
        .cta-section {
            background: linear-gradient(135deg, #1f2937, #374151);
            color: white;
            padding: 40px;
            border-radius: 15px;
            text-align: center;
            margin-top: 40px;
        }
        
        .cta-title {
            font-size: 2em;
            font-weight: 800;
            margin-bottom: 15px;
        }
        
        .cta-subtitle {
            font-size: 1.1em;
            margin-bottom: 25px;
            opacity: 0.9;
        }
        
        .cta-button {
            background: linear-gradient(135deg, #10b981, #059669);
            color: white;
            padding: 15px 40px;
            border: none;
            border-radius: 50px;
            font-size: 1.1em;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s ease;
            text-decoration: none;
            display: inline-block;
            box-shadow: 0 10px 25px rgba(16, 185, 129, 0.3);
        }
        
        .cta-button:hover {
            transform: translateY(-3px);
            box-shadow: 0 15px 35px rgba(16, 185, 129, 0.4);
        }
        
        .tech-badges {
            display: flex;
            justify-content: center;
            gap: 15px;
            margin-top: 30px;
            flex-wrap: wrap;
        }
        
        .tech-badge {
            background: rgba(79, 70, 229, 0.1);
            color: #4f46e5;
            padding: 8px 16px;
            border-radius: 20px;
            font-size: 0.9em;
            font-weight: 600;
            border: 1px solid rgba(79, 70, 229, 0.2);
        }
        
        @media (max-width: 768px) {
            .workflow-steps {
                flex-direction: column;
            }
            
            .arrow {
                transform: rotate(90deg);
                margin: 10px 0;
            }
            
            .features-grid {
                grid-template-columns: 1fr;
            }
            
            .stats-row {
                flex-direction: column;
                align-items: center;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="logo-area">
                <h1>⚡ Dynamic SQL Generator</h1>
                <div class="tagline">Revolutionizing JSON Data Processing in Snowflake</div>
            </div>
            
            <div class="stats-row">
                <div class="stat-box">
                    <span class="stat-number">95%</span>
                    <span class="stat-label">Time Saved</span>
                </div>
                <div class="stat-box">
                    <span class="stat-number">30s</span>
                    <span class="stat-label">Query Generation</span>
                </div>
                <div class="stat-box">
                    <span class="stat-number">Multi-GB</span>
                    <span class="stat-label">Dataset Support</span>
                </div>
                <div class="stat-box">
                    <span class="stat-number">Zero</span>
                    <span class="stat-label">Setup Required</span>
                </div>
            </div>
        </div>
        
        <div class="features-grid">
            <div class="feature-card mode-card">
                <div class="feature-icon">🐍</div>
                <div class="feature-title">Dual Operation Modes</div>
                <div class="feature-desc">Work your way, anywhere, anytime</div>
                <ul class="feature-highlights">
                    <li>Python Mode: Instant portable SQL generation</li>
                    <li>Snowflake Mode: Live schema analysis</li>
                    <li>No database connection required for prototyping</li>
                    <li>Real-time data sampling for accuracy</li>
                </ul>
            </div>
            
            <div class="feature-card export-card">
                <div class="feature-icon">📊</div>
                <div class="feature-title">Multi-Format Exports</div>
                <div class="feature-desc">From idea to production in one click</div>
                <ul class="feature-highlights">
                    <li>Clean SQL files ready for deployment</li>
                    <li>Production-ready dbt models</li>
                    <li>Interactive Jupyter notebooks</li>
                    <li>Copy-paste ready code snippets</li>
                </ul>
            </div>
            
            <div class="feature-card smart-card">
                <div class="feature-icon">🧠</div>
                <div class="feature-title">AI-Powered Intelligence</div>
                <div class="feature-desc">Smart automation that just works</div>
                <ul class="feature-highlights">
                    <li>Automatic field disambiguation</li>
                    <li>Dynamic type casting & validation</li>
                    <li>Intelligent schema optimization</li>
                    <li>Complex nesting detection</li>
                </ul>
            </div>
            
            <div class="feature-card performance-card">
                <div class="feature-icon">⚡</div>
                <div class="feature-title">Enterprise Performance</div>
                <div class="feature-desc">Built for scale, optimized for speed</div>
                <ul class="feature-highlights">
                    <li>Memory-efficient JSON parsing</li>
                    <li>Adaptive batching algorithms</li>
                    <li>Intelligent schema caching</li>
                    <li>Multi-gigabyte dataset support</li>
                </ul>
            </div>
            
            <div class="feature-card tech-card">
                <div class="feature-icon">⚙️</div>
                <div class="feature-title">Advanced SQL Operations</div>
                <div class="feature-desc">Complete operator support for complex queries</div>
                <ul class="feature-highlights">
                    <li>BETWEEN, IN, NOT IN operators</li>
                    <li>LIKE pattern matching</li>
                    <li>Optimized FLATTEN operations</li>
                    <li>Nested array handling</li>
                </ul>
            </div>
            
            <div class="feature-card enterprise-card">
                <div class="feature-icon">🚀</div>
                <div class="feature-title">Production Ready</div>
                <div class="feature-desc">Enterprise-grade reliability and security</div>
                <ul class="feature-highlights">
                    <li>Robust retry mechanisms</li>
                    <li>Error handling & validation</li>
                    <li>Modular, maintainable codebase</li>
                    <li>Scalable architecture</li>
                </ul>
            </div>
        </div>
        
        <div class="workflow-section">
            <div class="workflow-title">🎯 From JSON Chaos to SQL Clarity in 3 Steps</div>
            <div class="workflow-steps">
                <div class="workflow-step">
                    <div class="step-circle">1</div>
                    <div class="step-title">Input JSON Data</div>
                    <div class="step-desc">Paste your complex JSON or connect to Snowflake</div>
                </div>
                <div class="arrow">→</div>
                <div class="workflow-step">
                    <div class="step-circle">2</div>
                    <div class="step-title">AI Analysis</div>
                    <div class="step-desc">Intelligent schema detection and optimization</div>
                </div>
                <div class="arrow">→</div>
                <div class="workflow-step">
                    <div class="step-circle">3</div>
                    <div class="step-title">Export & Deploy</div>
                    <div class="step-desc">Get production-ready SQL, dbt models, or notebooks</div>
                </div>
            </div>
        </div>
        
        <div class="cta-section">
            <div class="cta-title">🚀 Ready to Transform Your JSON Workflow?</div>
            <div class="cta-subtitle">Join thousands of data professionals who've already made the switch</div>
            <a href="https://sainath-reddie.streamlit.app/" class="cta-button" target="_blank">
                🎯 Try Live Demo Now
            </a>
            <div class="tech-badges">
                <span class="tech-badge">#Snowflake</span>
                <span class="tech-badge">#DataEngineering</span>
                <span class="tech-badge">#SQL</span>
                <span class="tech-badge">#Streamlit</span>
                <span class="tech-badge">#JSON</span>
                <span class="tech-badge">#Analytics</span>
            </div>
        </div>
    </div>
</body>
</html>
