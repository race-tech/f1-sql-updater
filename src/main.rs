use std::env;

use chrono::Datelike;
use mysql::{prelude::*, Transaction};
use sea_query::{MysqlQueryBuilder, Query};

mod macros;
mod models;
mod tables;

use simple_logger::SimpleLogger;
use tables::*;

fn main() -> anyhow::Result<()> {
    SimpleLogger::new().init()?;

    log::info!("starting with args: {:?}", env::args());
    let base_path = env::var("F1_SQL_UPDATER_CSV_FOLDER").unwrap_or("csv".into());
    let base_path = std::path::Path::new(&base_path);
    let round = env::args().nth(1).unwrap().parse::<u16>()?;
    let is_sprint = env::args().nth(2).unwrap().parse::<bool>()?;
    let year = chrono::Utc::now().year();

    let user = env::var("MYSQL_USER").unwrap_or("user".into());
    let password = env::var("MYSQL_PWD").unwrap_or("password".into());
    let port = env::var("MYSQL_TCP_PORT")
        .map(|port| port.parse().unwrap_or(3306))
        .unwrap_or(3306);
    let db_name = env::var("MYSQL_DATABASE").unwrap_or("f1db".into());
    let mut conn = mysql::Conn::new(
        mysql::OptsBuilder::new()
            .ip_or_hostname("localhost".into())
            .tcp_port(port)
            .user(Some(user))
            .pass(Some(password))
            .db_name(Some(db_name)),
    )?;

    let race_id = *conn
        .query_map(
            format!("SELECT raceId FROM races WHERE round = {round} AND year = {year}"),
            |race_id: i32| race_id,
        )?
        .first()
        .expect("race not found");

    let mut tx = conn.start_transaction(mysql::TxOpts::default())?;

    lap_times(race_id, &base_path, &mut tx)?;
    pit_stops(race_id, &base_path, &mut tx)?;
    qualifying_results(race_id, &base_path, &mut tx)?;
    results(race_id, &base_path, &mut tx)?;
    driver_standings(race_id, &base_path, &mut tx)?;
    constructor_standings(race_id, &base_path, &mut tx)?;
    constructor_results(race_id, base_path, &mut tx)?;

    if is_sprint {
        // constructor_sprint_results(race_id, &mut tx)?;
        // sprint_lap_times(race_id, &mut tx)?;
        // driver_sprint_results(race_id, &mut tx)?;
    }

    tx.commit()?;
    log::info!("transaction committed");

    Ok(())
}

fn lap_times(
    race_id: i32,
    base_path: &std::path::Path,
    tx: &mut Transaction,
) -> anyhow::Result<()> {
    let file = base_path.join("lap_times.csv");
    let mut rdr = csv::Reader::from_path(file)?;

    for r in rdr.deserialize::<models::LapTime>() {
        let la = r?;
        log::info!("inserting lap analysis: {:?}", la);

        let time = format!(
            "{}:{}.{:03}",
            la.time.num_minutes(),
            la.time.num_seconds(),
            la.time.num_milliseconds()
        );

        let q = Query::insert()
            .into_table(LapTimes::Table)
            .columns([
                LapTimes::RaceID,
                LapTimes::DriverID,
                LapTimes::Lap,
                LapTimes::Position,
                LapTimes::Time,
                LapTimes::Milliseconds,
            ])
            .values([
                race_id.into(),
                la.driver_id.into(),
                la.lap.into(),
                la.position.into(),
                time.into(),
                la.time.num_milliseconds().into(),
            ])?
            .to_string(MysqlQueryBuilder);

        bypass_duplicates!(tx.exec_drop(q, ()))?;
    }

    log::info!("lap_times inserted");
    Ok(())
}

fn pit_stops(
    race_id: i32,
    base_path: &std::path::Path,
    tx: &mut Transaction,
) -> anyhow::Result<()> {
    let file = base_path.join("pit_stops.csv");
    let mut rdr = csv::Reader::from_path(file)?;

    for r in rdr.deserialize::<models::PitStop>() {
        let ps = r?;
        log::info!("inserting pit stop: {:?}", ps);

        let duration = format!(
            "{}.{:03}",
            ps.duration.num_seconds(),
            ps.duration.num_milliseconds()
        );

        let q = Query::insert()
            .into_table(PitStops::Table)
            .columns([
                PitStops::RaceID,
                PitStops::DriverID,
                PitStops::Stop,
                PitStops::Lap,
                PitStops::Time,
                PitStops::Duration,
                PitStops::Milliseconds,
            ])
            .values([
                race_id.into(),
                ps.driver_id.into(),
                ps.stop.into(),
                ps.lap.into(),
                ps.time.format("%H:%M:%S").to_string().into(),
                duration.into(),
                ps.duration.num_milliseconds().into(),
            ])?
            .to_string(MysqlQueryBuilder);

        tx.exec_drop(q, ())?;
    }

    log::info!("pit_stops inserted");
    Ok(())
}

fn qualifying_results(
    race_id: i32,
    base_path: &std::path::Path,
    tx: &mut Transaction,
) -> anyhow::Result<()> {
    let file = base_path.join("qualifying.csv");
    let mut rdr = csv::Reader::from_path(file)?;

    for r in rdr.deserialize::<models::Qualifying>() {
        let qo = r?;
        log::info!("qualification order: {:?}", qo);

        // TODO: Handle status

        let q = Query::insert()
            .into_table(Qualifying::Table)
            .columns([
                Qualifying::RaceID,
                Qualifying::DriverID,
                Qualifying::ConstructorID,
                Qualifying::Number,
                Qualifying::Position,
                Qualifying::Q1,
                Qualifying::Q2,
                Qualifying::Q3,
            ])
            .values([
                race_id.into(),
                qo.driver_id.into(),
                qo.constructor_id.into(),
                qo.number.into(),
                qo.position.into(),
                qo.q1.into(),
                qo.q2.into(),
                qo.q3.into(),
            ])?
            .to_string(MysqlQueryBuilder);

        tx.exec_drop(q, ())?;
    }

    log::info!("qualifying_results inserted");
    Ok(())
}

fn results(race_id: i32, base_path: &std::path::Path, tx: &mut Transaction) -> anyhow::Result<()> {
    let file = base_path.join("results.csv");
    let mut rdr = csv::Reader::from_path(file)?;

    for r in rdr.deserialize::<models::RaceResult>() {
        let rr = r?;
        log::info!("inserting driver race result: {:?}", rr);

        // TODO: Handle status

        let q = Query::insert()
            .into_table(Results::Table)
            .columns([
                Results::RaceID,
                Results::DriverID,
                Results::ConstructorID,
                Results::Number,
                Results::Grid,
                Results::Position,
                Results::PositionText,
                Results::PositionOrder,
                Results::Points,
                Results::Laps,
                Results::Time,
                Results::Milliseconds,
                Results::FastestLap,
                Results::Rank,
                Results::FastestLapTime,
                Results::FastestLapSpeed,
            ])
            .values([
                race_id.into(),
                rr.driver_id.into(),
                rr.constructor_id.into(),
                rr.driver_number.into(),
                rr.grid.into(),
                rr.position.into(),
                rr.position_text.into(),
                rr.position_order.into(),
                rr.points.into(),
                rr.laps.into(),
                rr.time.into(),
                rr.milliseconds.into(),
                rr.fastest_lap.into(),
                rr.rank.into(),
                rr.fatest_lap_time.into(),
                rr.fastest_lap_speed.into(),
            ])?
            .to_string(MysqlQueryBuilder);

        tx.exec_drop(q, ())?;
    }

    log::info!("driver race results inserted");
    Ok(())
}

fn constructor_results(
    race_id: i32,
    base_path: &std::path::Path,
    tx: &mut Transaction,
) -> anyhow::Result<()> {
    let file = base_path.join("constructor_results.csv");
    let mut rdr = csv::Reader::from_path(file)?;

    for r in rdr.deserialize::<models::ConstructorResult>() {
        let cr = r?;
        log::info!("inserting constructor result: {:?}", cr);

        let q = Query::insert()
            .into_table(ConstructorResults::Table)
            .columns([
                ConstructorResults::RaceID,
                ConstructorResults::ConstructorID,
                ConstructorResults::Points,
            ])
            .values([race_id.into(), cr.constructor_id.into(), cr.points.into()])?
            .to_string(MysqlQueryBuilder);

        tx.exec_drop(q, ())?;
    }

    log::info!("constructor race results inserted");
    Ok(())
}

fn driver_standings(
    race_id: i32,
    base_path: &std::path::Path,
    tx: &mut Transaction,
) -> anyhow::Result<()> {
    let file = base_path.join("driver_standings.csv");
    let mut rdr = csv::Reader::from_path(file)?;

    for r in rdr.deserialize::<models::DriverStanding>() {
        let dd = r?;
        log::info!("inserting driver championship: {:?}", dd);

        let q = Query::insert()
            .into_table(DriverStandings::Table)
            .columns([
                DriverStandings::RaceID,
                DriverStandings::DriverID,
                DriverStandings::Points,
                DriverStandings::Position,
                DriverStandings::PositionText,
                DriverStandings::Wins,
            ])
            .values([
                race_id.into(),
                dd.driver_id.into(),
                dd.points.into(),
                dd.position.into(),
                dd.position_text.into(),
                dd.wins.into(),
            ])?
            .to_string(MysqlQueryBuilder);

        tx.exec_drop(q, ())?;
    }

    log::info!("driver championship inserted");
    Ok(())
}

fn constructor_standings(
    race_id: i32,
    base_path: &std::path::Path,
    tx: &mut Transaction,
) -> anyhow::Result<()> {
    let file = base_path.join("constructor_standings.csv");
    let mut rdr = csv::Reader::from_path(file)?;

    for r in rdr.deserialize::<models::ConstructorStanding>() {
        let cc = r?;
        log::info!("inserting constructor championship: {:?}", cc);

        let q = Query::insert()
            .into_table(ConstructorStandings::Table)
            .columns([
                ConstructorStandings::RaceID,
                ConstructorStandings::ConstructorID,
                ConstructorStandings::Points,
                ConstructorStandings::Position,
                ConstructorStandings::PositionText,
                ConstructorStandings::Wins,
            ])
            .values([
                race_id.into(),
                cc.constructor_id.into(),
                cc.points.into(),
                cc.position.into(),
                cc.position.into(),
                cc.wins.into(),
            ])?
            .to_string(MysqlQueryBuilder);

        tx.exec_drop(q, ())?;
    }

    log::info!("constructor championship inserted");
    Ok(())
}

fn sprint_lap_times(race_id: i32, tx: &mut Transaction) -> anyhow::Result<()> {
    let file = "/etc/csv/sprint_laps_analysis.csv";
    let mut rdr = csv::Reader::from_path(file)?;

    for r in rdr.deserialize::<models::LapTime>() {
        let la = r?;
        log::info!("inserting sprint lap time: {:?}", la);

        let time = format!(
            "{}:{}.{:3}",
            la.time.num_minutes(),
            la.time.num_seconds(),
            la.time.num_milliseconds()
        );

        let q = Query::insert()
            .into_table(LapTimes::Table)
            .columns([
                LapTimes::RaceID,
                LapTimes::DriverID,
                LapTimes::Lap,
                LapTimes::Position,
                LapTimes::Time,
                LapTimes::Milliseconds,
            ])
            .values([
                race_id.into(),
                la.driver_id.into(),
                la.lap.into(),
                la.position.into(),
                time.into(),
                la.time.num_milliseconds().into(),
            ])?
            .to_string(MysqlQueryBuilder);

        tx.exec_drop(q, ())?;
    }

    log::info!("sprint lap times inserted");
    Ok(())
}

fn driver_sprint_results(race_id: i32, tx: &mut Transaction) -> anyhow::Result<()> {
    let file = "/etc/csv/driver_sprint_result.csv";
    let mut rdr = csv::Reader::from_path(file)?;

    for r in rdr.deserialize::<models::DriverSprintResult>() {
        let dsr = r?;
        log::info!("inserting driver sprint result: {:?}", dsr);

        let driver_id = *tx
            .query_map(
                format!(
                    "SELECT driverId FROM drivers WHERE number = {}",
                    driver_number(dsr.no)
                ),
                |driver_id: i32| driver_id,
            )?
            .first()
            .expect("driver not found");
        let constructor_id = *tx
            .query_map(
                format!(
                    "SELECT constructorId FROM constructors WHERE name = '{}'",
                    dsr.entrant
                ),
                |constructor_id: i32| constructor_id,
            )?
            .first()
            .expect("constructor not found");

        // TODO: Handle status

        let q = Query::insert()
            .into_table(SprintResults::Table)
            .columns([
                SprintResults::RaceID,
                SprintResults::DriverID,
                SprintResults::ConstructorID,
                SprintResults::Number,
                SprintResults::Grid,
                SprintResults::Position,
                SprintResults::PositionText,
                SprintResults::PositionOrder,
                SprintResults::Points,
                SprintResults::Laps,
                SprintResults::Time,
                SprintResults::Milliseconds,
                SprintResults::FastestLap,
                SprintResults::FastestLapTime,
            ])
            .values([
                race_id.into(),
                driver_id.into(),
                constructor_id.into(),
                dsr.no.into(),
                dsr.grid.into(),
                dsr.position.parse::<u16>().ok().into(),
                dsr.position.into(),
                dsr.position_order.into(),
                dsr.points.into(),
                dsr.laps.into(),
                dsr.time.into(),
                dsr.milliseconds.into(),
                dsr.fastest_lap.into(),
                dsr.fatest_lap_time.into(),
            ])?
            .to_string(MysqlQueryBuilder);

        tx.exec_drop(q, ())?;
    }

    log::info!("driver sprint results inserted");
    Ok(())
}

fn constructor_sprint_results(race_id: i32, tx: &mut Transaction) -> anyhow::Result<()> {
    let file = "/etc/csv/constructor_race_result.csv";
    let mut rdr = csv::Reader::from_path(file)?;

    let driver_sprint_file = "/etc/csv/driver_sprint_result.csv";
    let mut driver_sprint_rdr = csv::Reader::from_path(driver_sprint_file)?;

    let drivers = driver_sprint_rdr
        .deserialize::<models::DriverSprintResult>()
        .collect::<Result<Vec<_>, _>>()?;

    for r in rdr.deserialize::<models::ConstructorResult>() {
        let cr = r?;
        log::info!("inserting constructor sprint result: {:?}", cr);

        let q = Query::insert()
            .into_table(ConstructorResults::Table)
            .columns([
                ConstructorResults::RaceID,
                ConstructorResults::ConstructorID,
                ConstructorResults::Points,
            ])
            .values([race_id.into(), cr.constructor_id.into(), cr.points.into()])?
            .to_string(MysqlQueryBuilder);

        tx.exec_drop(q, ())?;
    }

    log::info!("constructor sprint result inserted");
    Ok(())
}

fn driver_number(no: u16) -> u16 {
    if no == 1 {
        33
    } else {
        no
    }
}
