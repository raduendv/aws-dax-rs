use clap::Parser;

fn main() -> ubrn_cli::Result<()> {
    ubrn_cli::cli::CliArgs::parse().run()
}
