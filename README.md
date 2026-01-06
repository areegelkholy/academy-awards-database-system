# Academy Awards Database System

This project is a database-backed application built to explore and analyze Academy Awards (Oscars) data across 96 annual iterations.  
It focuses on relational modeling, data extraction from semi-structured sources, and query-driven analysis.

## Project Overview
The goal of this project was to design and implement a complete database system that allows users to explore nominations, winners, movies, and contributors across the history of the Academy Awards, while supporting user interaction and analytical queries not available on Wikipedia.

The system consists of:
- A relational database schema modeling movies, individuals, roles, nominations, and award outcomes
- A web crawler for extracting and cleaning data from Wikipedia
- A backend application enabling user registration, custom nominations, and statistical queries

## Data Source
- **Source:** Wikipedia Academy Awards pages (1st–96th iterations)
- **Data Type:** Semi-structured HTML
- **Extraction:** Python-based crawling and parsing

## Database Design
The database was designed using an Entity-Relationship Diagram (ERD) and normalized relational schema.

Key entities include:
- Movies
- People (actors, directors, producers)
- Award categories
- Nominations and wins
- Users and user-generated nominations

The schema enforces referential integrity and supports aggregation and filtering across years, categories, and countries.

## Data Collection
A Python-based crawler was implemented to:
- Iterate through Academy Awards pages
- Parse nomination tables and individual profile data
- Extract movie metadata and contributor information
- Populate the database with cleaned, structured records

## Application Layer
A Flask-based application connects to a remotely hosted MySQL database and provides the following functionality:
- User registration and authentication
- User-created nominations
- Viewing historical nominations and wins
- Aggregated statistics (e.g. top movies, countries, production companies)
- Query-based exploration of award trends

## Technologies Used
- **Language:** Python
- **Database:** MySQL
- **Backend:** Flask
- **Web Crawling:** BeautifulSoup
- **Frontend:** HTML / CSS


