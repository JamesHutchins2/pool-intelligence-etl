# Automated Swimming Pool Real Estate Intelligence Platform

> **Enterprise-grade ETL pipelines that identify swimming pool properties at scale, powering data-driven real estate insights for Zenith Vision Analytics**

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://python.org)
[![Apache Airflow](https://img.shields.io/badge/Apache-Airflow-red.svg)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-blue.svg)](https://postgresql.org/)
[![License: Proprietary](https://img.shields.io/badge/License-Proprietary-red.svg)](#license)

## What This Does

This production system automatically discovers and tracks swimming pool properties across Canada and the US, processing **terabytes of satellite imagery** and **millions of real estate listings** to create the most comprehensive swimming pool property database available. It powers location intelligence for pool service companies, real estate professionals, and property investors.

**Note this is a small part of a much larger project.***


---

## Architecture Overview

The system consists of **two interconnected Apache Airflow DAGs** that work together to create a complete real estate intelligence platform:

### Pipeline 1: `weekly_listings_etl`
**Primary Data Collection & Processing**

```
Search Locations → Extract Listings → Clean & Filter → Pool Detection → Address Validation → Database Storage → Client Notification
```

This pipeline crawls real estate listing websites, processes raw data, and builds a comprehensive property database.

### Pipeline 2: `client_data_update_etl`  
**Client Database Synchronization**

```
Property Updates → Master DB Sync → Pool Assignment 
```

This pipeline translates the processed data into client-facing databases and triggers automated reporting.

---

## Technical Deep Dive

### Pipeline 1: Weekly Listings ETL

**Data Extraction**
- **Custom pyRealtor Library**: Scrapes multiple MLS sources (Realtor.com, Realtor.ca, Housing.com)
- **Geographic Targeting**: Uses configurable location lists for focused extraction
- **Rate Limiting**: Implements intelligent delays to respect source websites

**Data Transformation**
```python
# Key cleaning steps implemented:
1. Filtering out non house listings    
2. Address cleaning, standardization and correction using the Google API   
3. Heuristic determination of private versus community pool access from listing descriptions
4. Continued detection of removed, added, and re-listed homes
5. Database uploads adding newly created data to the central source of truth
6. Email based notification for clients
```

**Pool Intelligence**
- **Description Analysis**: NLP processing of property descriptions to identify pool mentions
- **Classification Logic**: Distinguishes between private pools, community pools, and nearby pools
- **Accuracy Validation**: Cross-references multiple data sources for verification

**Address Validation**  
- **Google Maps API Integration**: Validates and corrects property addresses
- **Geocoding**: Converts addresses to precise lat/lon coordinates
- **Standardization**: Ensures consistent address formatting across the database

### Pipeline 2: Client Data Update ETL

**Property Matching**
- **Intelligent Deduplication**: Matches new listings against existing property records
- **UUID Management**: Assigns unique identifiers to ensure data integrity
- **Relationship Mapping**: Links properties, pools, and listings with proper foreign keys

**Database Synchronization**
```python
# Core data flow:
Properties (1007) → Pools (1007) → Listings (1007) → Client Updates
```

**Client Integration**
- **Master Database Updates**: Syncs with centralized property intelligence database  
- **Automated Reporting**: Generates and emails weekly property reports to active clients
- **API Integration**: Provides real-time access to property data via RESTful endpoints

---

## Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Orchestration** | Apache Airflow | Pipeline scheduling and monitoring |
| **Data Processing** | Pandas, PostgreSQL | Large-scale data transformation |
| **Web Scraping** | Custom pyRealtor Library | Multi-source real estate data extraction |
| **Geospatial** | PostGIS, Google Maps API | Location intelligence and validation |
| **AI/ML** | YOLO, SAM (Segment Anything) | Computer vision for satellite imagery |
| **Infrastructure** | Docker, Linux | Production deployment environment |

---

##  Data Pipeline Metrics

### Processing Scale
- **Input Sources**: 50+ geographic search areas
- **Daily Volume**: 1,000-2,000 new property listings  
- **Database Size**: 500,000+ property records with historical data
- **Processing Time**: 0.5-1 hours for complete weekly cycle

### Quality Assurance
- **Data Validation**: Multi-step verification of addresses, prices, and property details
- **Duplicate Detection**: Advanced deduplication using MLS IDs and address matching
- **Error Recovery**: Automated retry logic and failure notifications
- **Monitoring**: Comprehensive logging and alerting via Airflow UI

---

## Quick Start

### Prerequisites
```bash
Python 3.9+
Apache Airflow 2.7+
PostgreSQL 13+
Docker (recommended)
```

### Environment Setup
```bash
# Clone the repository
git clone <your-repo-url>
cd realestate-pool-intelligence

# Set up environment variables
cp .env.example .env
# Configure your database URLs and API keys

# Install dependencies
pip install -r requirements.txt

# Initialize Airflow database
airflow db init

# Start the pipeline
airflow dags trigger weekly_listings_etl
```

### Testing
Use the included Jupyter notebooks for testing individual components:
- `test_client_data.ipynb` - Test client data synchronization
- `weekly_run_dev.ipynb` - Test main ETL pipeline

---

## Business Impact

### For Pool Service Companies
- **Geographic Expansion**: Identify untapped markets with high pool density
- **Lead Generation**: Access verified contact information for pool owners


---

## Configuration

### Search Areas
```csv
country_code,province_state,search_area
CA,Ontario,Toronto
US,California,Los Angeles
```

### Pool Detection Parameters
Customize pool identification rules in `/include/transform/pool_inference.py`:
- Private pool keywords
- Community pool indicators  
- Exclusion patterns

---

## Testing & Quality

### Automated Testing
- **Unit Tests**: Individual component validation
- **Integration Tests**: End-to-end pipeline verification  
- **Data Quality Tests**: Schema validation and consistency checks

### Monitoring
- **Airflow UI**: Real-time pipeline monitoring and logs
- **Email Alerts**: Automated failure notifications
- **Metrics Dashboard**: Key performance indicators tracking

---

## Contributing

This is a production system for Zenith Vision Analytics. For questions about implementation details or potential collaboration opportunities, please reach out via [LinkedIn](https://www.linkedin.com/in/james-hutchins-5446aa175/).

### Development Guidelines
- Follow PEP 8 Python style guidelines
- Include comprehensive logging for debugging
- Maintain backward compatibility for database schemas
- Document any external API integrations

---

## License

**Proprietary Software** - This code is proprietary to Zenith Vision Analytics and is not licensed for public use, modification, or distribution. All rights reserved.

This repository serves as a portfolio demonstration of technical capabilities. Please contact the author for licensing inquiries or collaboration opportunities.

---

## Portfolio Highlights

This project demonstrates:
- **Production ETL Design**: Enterprise-grade data pipelines with proper error handling
- **Multi-Source Integration**: Complex data fusion from multiple real estate APIs
- **AI/ML Integration**: Computer vision models for satellite imagery analysis
- **Database Architecture**: Proper normalization and relationship design
- **Monitoring & Alerting**: Production-ready observability and incident response
- **Client-Facing Systems**: Automated reporting and data delivery

Built with precision for real-world business impact. # pool-intelligence-etl
