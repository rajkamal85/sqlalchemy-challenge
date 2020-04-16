import numpy as np
import sqlalchemy
import datetime as dt
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, and_, or_
from flask import Flask, jsonify


#engine = create_engine("sqlite:///C:/Users/rajka/Desktop/UofT/Assignments/Assignment 8 - SqlAlchemy/sqlalchemy-challenge/Resources/hawaii.sqlite")
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

Base = automap_base()
Base.prepare(engine, reflect=True)
Base.classes.keys()

measurement = Base.classes.measurement
Station = Base.classes.station

app = Flask(__name__)

@app.route("/")
def welcome():
    return (
        f"Welcome to Home page<br/>"
        f"Available routes are<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end>"
        )

@app.route("/api/v1.0/precipitation")
def precipitation():
    session = Session(engine)
    result = session.query(measurement.date, measurement.prcp).all()
    session.close()

    all_prcp = []
    for date, prcp in result:
        precipitation = {}
        precipitation.__setitem__(date, prcp) 
        #precipitation["date"] = date
        #precipitation["prcp"] = prcp
        all_prcp.append(precipitation)

    return jsonify(all_prcp)

@app.route("/api/v1.0/stations")
def stations():
    session = Session(engine)
    result = session.query(Station.station, Station.name).all()
    session.close()

    all_stations = []
    for station, name in result:
        stations = {}
        stations.__setitem__(station, name)
        all_stations.append(stations)

    return jsonify(all_stations)

@app.route("/api/v1.0/tobs")
def tobs():
    session = Session(engine)
    last_date = session.query(measurement.date).order_by(measurement.date.desc()).first()
    last_date = list(np.ravel(last_date))[0]
    last_date = dt.datetime.strptime(last_date, "%Y-%m-%d")

    last_date_year = int(dt.datetime.strftime(last_date, "%Y"))
    last_date_month = int(dt.datetime.strftime(last_date, "%m"))
    last_date_day = int(dt.datetime.strftime(last_date, "%d"))

    year_ago = dt.date(last_date_year-1 , last_date_month, last_date_day)

    result = session.query(measurement.date, measurement.tobs).filter(and_(measurement.date>=year_ago, measurement.station == 'USC00519281')).order_by(measurement.date).all()

    session.close()

    most_tobs = []
    for date, tobs in result:
        date_tobs = {}
        date_tobs.__setitem__(date, tobs)
#        date_tobs = {result.date: result.prcp, "Station": result.station}
        most_tobs.append(date_tobs)

    return jsonify(most_tobs)

@app.route("/api/v1.0/<start>")
def startdate(start):
    session = Session(engine)

    sel = [measurement.date, func.min(measurement.tobs), func.max(measurement.tobs), func.avg(measurement.tobs)]

    results = session.query(*sel).filter(func.strftime("%Y-%m-%d", measurement.date) >= start).group_by(measurement.date).all()

    session.close()

    all_dates = []
    for date, min, max, avg in results:
        start_stats = {}
        start_stats["Date"] = date
        start_stats["TMIN"] = min
        start_stats["TMAX"] = max
        start_stats["TAVG"] = round(avg, 2)
        all_dates.append(start_stats)

    return jsonify(all_dates)

@app.route("/api/v1.0/<start>/<end>")
def start_end(start, end):
    session = Session(engine)

    sel = [measurement.date, func.min(measurement.tobs), func.max(measurement.tobs), func.avg(measurement.tobs)]

    results = session.query(*sel).filter(and_(func.strftime("%Y-%m-%d", measurement.date) >= start, func.strftime("%Y-%m-%d", measurement.date) <= end)).\
        group_by(measurement.date).all()

    session.close()

    allfilter_dates = []
    for date, min, max, avg in results:
        tobs_stats = {}
        tobs_stats["Date"] = date
        tobs_stats["TMIN"] = min
        tobs_stats["TMAX"] = max
        tobs_stats["TAVG"] = round(avg, 2)
        allfilter_dates.append(tobs_stats)

    return jsonify(allfilter_dates)

if __name__ == "__main__":
    app.run(debug=True)