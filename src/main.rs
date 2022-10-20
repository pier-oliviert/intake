use clap::Parser;
use env_logger;

mod events;
mod source;
mod storage;

#[derive(Parser, Debug)]
#[clap(version)]
struct Args {
    #[clap(short, long, value_parser)]
    config: String,
}

#[tokio::main]
async fn main() {
    use std::fs::File;
    use std::io::Read;
    use yaml_rust::YamlLoader;

    env_logger::init();

    let args = Args::parse();
    let mut file = File::open(args.config).expect("can't open file at that location");
    let mut content = String::new();

    file.read_to_string(&mut content)
        .expect("couldn't read the content of config.");

    let config =
        &YamlLoader::load_from_str(&content).expect("content of the config file is not YAML");

    if config.len() != 1 {
        panic!("YAML configuration requires the file to contain exactly 1 top level object(found: {} top level objects).", config.len());
    }

    let sender = events::listen();

    loop {
        let (mut src, failure) = source::initialize(&config[0]["source"]).await.unwrap();

        // Cloning is needed here for the sender because the loop will re-execute and
        // the sender will be moved after the first iteration.
        src.connect(sender.clone()).await;

        match failure.recv() {
            Ok(e) => println!("Disconnected: {}. Attempting reconnection.", e),
            Err(e) => panic!(
                "unexpected failure on the channel monitoring connection health. This is a bug: {}",
                e
            ),
        }
    }
}
