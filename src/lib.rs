pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

pub fn make_migrator() -> sqlx::migrate::Migrator {
    sqlx::migrate!("src/postgres/migrations")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
