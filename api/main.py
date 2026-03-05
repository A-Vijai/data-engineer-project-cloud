"""
FastAPI Application for Ride-Sharing Analytics
Provides REST API endpoints to query processed analytics data
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from google.cloud import bigquery
from datetime import datetime, date as date_class, timedelta
from typing import Optional
import os
import logging
import warnings

# Suppress FutureWarnings from google.api_core about Python version
warnings.filterwarnings("ignore", category=FutureWarning, module="google.api_core")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Ride-Sharing Analytics API",
    description="API for querying ride-sharing analytics data",
    version="1.0.0"
)

# BigQuery client
PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'hidden-phalanx-479212-a4')
DATASET = os.environ.get('BIGQUERY_DATASET', 'ridesharing_analytics')

client = bigquery.Client(project=PROJECT_ID)


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Ride-Sharing Analytics API",
        "version": "1.0.0",
        "endpoints": [
            "/api/health",
            "/api/rides/daily?date=YYYY-MM-DD",
            "/api/drivers/{driver_id}",
            "/api/revenue/summary?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD"
        ]
    }


@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Test BigQuery connection
        query = f"SELECT 1 as test"
        client.query(query).result()
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "bigquery": "connected"
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=503, detail=f"Service unhealthy: {str(e)}")


@app.get("/api/rides/daily")
async def get_daily_rides(
    date: Optional[str] = Query(None, description="Date in YYYY-MM-DD format (defaults to today)")
):
    """
    Get daily ride metrics for a specific date
    Returns total rides, revenue, average duration, and average distance for a given date
    
    Args:
        date: Date in YYYY-MM-DD format (e.g., 2025-01-15). If not provided, defaults to today.
    """
    try:
        # Set default to today if date not provided
        if date:
            # Validate date format
            try:
                datetime.strptime(date, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid date format. Use YYYY-MM-DD format (e.g., 2025-01-15)"
                )
            query_date = date
        else:
            # Default to today
            query_date = date_class.today().strftime("%Y-%m-%d")
        
        query = f"""
        SELECT 
            pickup_date,
            total_rides,
            total_revenue,
            total_tips,
            avg_duration_minutes,
            avg_distance_km,
            avg_fare,
            total_fare_with_tips
        FROM `{PROJECT_ID}.{DATASET}.agg_daily_rides`
        WHERE pickup_date = '{query_date}'
        """
        
        logger.info(f"Executing query for date: {query_date}")
        query_job = client.query(query)
        results = query_job.result()
        
        rows = list(results)
        if not rows:
            return {
                "date": query_date,
                "message": "No data found for this date",
                "data": None
            }
        
        row = rows[0]
        return {
            "date": query_date,
            "data": {
                "total_rides": row.total_rides,
                "total_revenue": float(row.total_revenue) if row.total_revenue else 0.0,
                "total_tips": float(row.total_tips) if row.total_tips else 0.0,
                "avg_duration_minutes": float(row.avg_duration_minutes) if row.avg_duration_minutes else 0.0,
                "avg_distance_km": float(row.avg_distance_km) if row.avg_distance_km else 0.0,
                "avg_fare": float(row.avg_fare) if row.avg_fare else 0.0,
                "total_fare_with_tips": float(row.total_fare_with_tips) if row.total_fare_with_tips else 0.0
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching daily rides: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")


@app.get("/api/drivers/{driver_id}")
async def get_driver_performance(driver_id: str):
    """
    Get driver performance metrics
    Returns driver details, total trips, earnings, ratings, and completion rate
    """
    try:
        # Get driver details
        driver_query = f"""
        SELECT 
            driver_id,
            name,
            email,
            vehicle_type,
            city,
            rating,
            is_active
        FROM `{PROJECT_ID}.{DATASET}.dim_drivers`
        WHERE driver_id = '{driver_id}' AND is_current = TRUE
        LIMIT 1
        """
        
        driver_job = client.query(driver_query)
        driver_results = list(driver_job.result())
        
        if not driver_results:
            raise HTTPException(status_code=404, detail=f"Driver {driver_id} not found")
        
        driver = driver_results[0]
        
        # Get driver performance
        performance_query = f"""
        SELECT 
            total_trips,
            total_earnings,
            total_tips,
            avg_rating,
            avg_trip_duration,
            avg_trip_distance,
            completion_rate
        FROM `{PROJECT_ID}.{DATASET}.agg_driver_performance`
        WHERE driver_id = '{driver_id}'
        LIMIT 1
        """
        
        perf_job = client.query(performance_query)
        perf_results = list(perf_job.result())
        
        performance = perf_results[0] if perf_results else None
        
        return {
            "driver_id": driver_id,
            "driver_info": {
                "name": driver.name,
                "email": driver.email,
                "vehicle_type": driver.vehicle_type,
                "city": driver.city,
                "rating": float(driver.rating) if driver.rating else None,
                "is_active": driver.is_active
            },
            "performance": {
                "total_trips": performance.total_trips if performance else 0,
                "total_earnings": float(performance.total_earnings) if performance and performance.total_earnings else 0.0,
                "total_tips": float(performance.total_tips) if performance and performance.total_tips else 0.0,
                "avg_rating": float(performance.avg_rating) if performance and performance.avg_rating else None,
                "avg_trip_duration_minutes": float(performance.avg_trip_duration) if performance and performance.avg_trip_duration else 0.0,
                "avg_trip_distance_km": float(performance.avg_trip_distance) if performance and performance.avg_trip_distance else 0.0,
                "completion_rate": float(performance.completion_rate) if performance and performance.completion_rate else 0.0
            } if performance else None
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching driver performance: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")


@app.get("/api/revenue/summary")
async def get_revenue_summary(
    start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format (defaults to 30 days ago)"),
    end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format (defaults to today)")
):
    """
    Get revenue summary for a date range
    Returns total revenue, ride count, average fare, and daily breakdown
    
    Args:
        start_date: Start date in YYYY-MM-DD format (e.g., 2025-01-01). If not provided, defaults to 30 days ago.
        end_date: End date in YYYY-MM-DD format (e.g., 2025-01-31). If not provided, defaults to today.
    """
    try:
        # Set defaults if not provided
        if not end_date:
            end_date = date_class.today().strftime("%Y-%m-%d")
        
        if not start_date:
            # Default to 30 days ago
            start_date = (date_class.today() - timedelta(days=30)).strftime("%Y-%m-%d")
        
        # Validate dates
        try:
            datetime.strptime(start_date, "%Y-%m-%d")
            datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid date format. Use YYYY-MM-DD format (e.g., 2025-01-01)"
            )
        
        # Validate date range
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        if start_dt > end_dt:
            raise HTTPException(
                status_code=400,
                detail="start_date must be before or equal to end_date"
            )
        
        # Get summary
        summary_query = f"""
        SELECT 
            SUM(total_rides) as total_rides,
            SUM(total_revenue) as total_revenue,
            SUM(total_tips) as total_tips,
            AVG(avg_fare) as avg_fare,
            SUM(total_fare_with_tips) as total_fare_with_tips
        FROM `{PROJECT_ID}.{DATASET}.agg_daily_rides`
        WHERE pickup_date BETWEEN '{start_date}' AND '{end_date}'
        """
        
        summary_job = client.query(summary_query)
        summary_results = list(summary_job.result())
        summary = summary_results[0] if summary_results else None
        
        # Get daily breakdown
        daily_query = f"""
        SELECT 
            pickup_date,
            total_rides,
            total_revenue,
            avg_fare
        FROM `{PROJECT_ID}.{DATASET}.agg_daily_rides`
        WHERE pickup_date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY pickup_date
        """
        
        daily_job = client.query(daily_query)
        daily_results = daily_job.result()
        
        daily_breakdown = [
            {
                "date": str(row.pickup_date),
                "total_rides": row.total_rides,
                "total_revenue": float(row.total_revenue) if row.total_revenue else 0.0,
                "avg_fare": float(row.avg_fare) if row.avg_fare else 0.0
            }
            for row in daily_results
        ]
        
        return {
            "start_date": start_date,
            "end_date": end_date,
            "summary": {
                "total_rides": summary.total_rides if summary and summary.total_rides else 0,
                "total_revenue": float(summary.total_revenue) if summary and summary.total_revenue else 0.0,
                "total_tips": float(summary.total_tips) if summary and summary.total_tips else 0.0,
                "avg_fare": float(summary.avg_fare) if summary and summary.avg_fare else 0.0,
                "total_fare_with_tips": float(summary.total_fare_with_tips) if summary and summary.total_fare_with_tips else 0.0
            } if summary else None,
            "daily_breakdown": daily_breakdown
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching revenue summary: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching data: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)

