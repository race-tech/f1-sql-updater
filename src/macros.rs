#[macro_export]
macro_rules! bypass_duplicates {
    ($res:expr) => {
        match $res {
            Ok(_) => Ok(()),
            Err(mysql::Error::MySqlError(e)) if e.code == 1062 => Ok(()),
            Err(e) => Err(e),
        }
    };
}
