use chrono::Local;
use rand::Rng;

pub fn generate_run_directory_name() -> String {
    // Get current local date in YYYY-MM-DD
    let date = Local::now().format("%Y-%m-%d").to_string();

    // Generate 10 random lowercase latin letters
    let mut rng = rand::rng();
    let rand_string: String = (0..10)
        .map(|_| {
            let idx = rng.random_range(0..26); // 0..=25
            (b'a' + idx) as char
        })
        .collect();

    // Combine as "YYYY-MM-DD-xxxxxxxxxx"
    format!("{}-{}", date, rand_string)
}
