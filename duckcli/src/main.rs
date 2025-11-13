use std::fs::File;
use std::io::{self};
use std::path::Path;

use clap::{Parser, Subcommand, ValueEnum, CommandFactory};
use clap_complete::{Shell as CompleteShell, generate};
use color_eyre::eyre::{Result, WrapErr};
use duckdb::Connection;
use rustyline::DefaultEditor;

/// Top-level CLI
#[derive(Parser, Debug)]
#[command(
    name = "duckcli",
    version,
    about = "Tiny DuckDB-powered CLI using embedded DuckDB"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

/// Shell completion targets
#[derive(ValueEnum, Clone, Debug)]
enum Shell {
    Bash,
    Zsh,
    Fish,
    PowerShell,
    Elvish,
}

impl From<Shell> for CompleteShell {
    fn from(s: Shell) -> Self {
        match s {
            Shell::Bash => CompleteShell::Bash,
            Shell::Zsh => CompleteShell::Zsh,
            Shell::Fish => CompleteShell::Fish,
            Shell::PowerShell => CompleteShell::PowerShell,
            Shell::Elvish => CompleteShell::Elvish,
        }
    }
}

/// Subcommands
#[derive(Subcommand, Debug)]
enum Commands {
    /// Run a one-shot SQL query and pretty-print the result
    Query {
        /// Path to DuckDB database
        db: String,
        /// SQL to run (everything after <db> is concatenated)
        #[arg(required = true)]
        sql: Vec<String>,
    },

    /// Start an interactive SQL shell
    Shell {
        /// Path to DuckDB database
        db: String,
    },

    /// Import CSV into a table using COPY
    Import {
        /// Path to DuckDB database
        db: String,
        /// Target table name (will be created if not exists)
        table: String,
        /// CSV file to import
        csv_path: String,
        /// Delimiter (default ',')
        #[arg(long, default_value_t = ',')]
        delimiter: char,
        /// Treat first row as header
        #[arg(long)]
        header: bool,
    },

    /// Export query result to CSV
    Export {
        /// Path to DuckDB database
        db: String,
        /// SQL to export (everything up to <csv_path> is the SQL)
        #[arg(required = true)]
        sql: Vec<String>,
        /// Output CSV file
        csv_path: String,
    },

    /// Generate shell completion script
    Completions {
        /// Which shell to generate completions for
        #[arg(value_enum)]
        shell: Shell,
    },
}

fn main() -> Result<()> {
    // Install pretty panic + error reporting
    color_eyre::install()?;

    let cli = Cli::parse();

    match cli.command {
        Commands::Query { db, sql } => {
            let sql = sql.join(" ");
            let conn = open_db(&db)?;
            run_query_pretty(&conn, &sql)?;
        }
        Commands::Shell { db } => {
            let conn = open_db(&db)?;
            interactive_shell(conn)?;
        }
        Commands::Import {
            db,
            table,
            csv_path,
            delimiter,
            header,
        } => {
            let conn = open_db(&db)?;
            import_csv(&conn, &table, &csv_path, delimiter, header)?;
        }
        Commands::Export { db, sql, csv_path } => {
            let conn = open_db(&db)?;
            let sql = sql.join(" ");
            export_csv(&conn, &sql, &csv_path)?;
        }
        Commands::Completions { shell } => {
            let mut cmd = Cli::command();
            let name = cmd.get_name().to_string();
            let shell: CompleteShell = shell.into();
            generate(shell, &mut cmd, name, &mut io::stdout());
        }
    }

    Ok(())
}

/// Open (or create) a DuckDB database
fn open_db(path: &str) -> Result<Connection> {
    Connection::open(path).wrap_err_with(|| format!("failed to open DuckDB database at {path}"))
}

/// Pretty-print a query result using Arrow
fn run_query_pretty(conn: &Connection, sql: &str) -> Result<()> {
    use duckdb::arrow::util::pretty::print_batches;

    let mut stmt = conn
        .prepare(sql)
        .wrap_err_with(|| format!("failed to prepare query: {sql}"))?;

    let arrow = stmt.query_arrow([]).wrap_err("arrow query failed")?;
    let batches: Vec<_> = arrow.collect();


    if batches.is_empty() {
        println!("OK (no rows)");
    } else {
        print_batches(&batches).wrap_err("failed to pretty-print result")?;
    }
    Ok(())
}

/// Super-minimal interactive shell using rustyline
fn interactive_shell(conn: Connection) -> Result<()> {
    println!("Connected to DuckDB. Enter SQL, or `\\q` to quit.");

    let mut readline_editor = DefaultEditor::new()?;

    loop {
        let line = readline_editor.readline("duckdb> ");

        let line = match line {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let _ = readline_editor.add_history_entry(trimmed);
                trimmed.to_string()
            }
            Err(rustyline::error::ReadlineError::Interrupted)
            | Err(rustyline::error::ReadlineError::Eof) => {
                println!();
                break;
            }
            Err(e) => {
                eprintln!("readline error: {e}");
                break;
            }
        };

        if line == "\\q" {
            break;
        }

        // Allow multiple statements separated by ';'
        for stmt in line.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            if let Err(err) = run_query_pretty(&conn, stmt) {
                eprintln!("error: {err:?}");
            }
        }
    }

    Ok(())
}

/// Import CSV via DuckDB COPY
fn import_csv(
    conn: &Connection,
    table: &str,
    csv_path: &str,
    delimiter: char,
    header: bool,
) -> Result<()> {
    // Simple-ish escaping for quote characters
    let escaped_path = csv_path.replace('\'', "''");
    let escaped_table = table.replace('"', "\"\"");

    // Create table if not exists using DuckDB's auto-detection
    let create_sql = format!(
        r#"
        CREATE TABLE IF NOT EXISTS "{table}" AS
        SELECT * FROM read_csv_auto('{path}', HEADER {header}, DELIM '{delim}')
        LIMIT 0;
        "#,
        table = escaped_table,
        path = escaped_path,
        header = if header { "TRUE" } else { "FALSE" },
        delim = delimiter,
    );
    conn.execute_batch(&create_sql)
        .wrap_err("failed to create table from CSV schema")?;

    let copy_sql = format!(
        r#"
        COPY "{table}" FROM '{path}'
        (FORMAT 'csv', HEADER {header}, DELIMITER '{delim}');
        "#,
        table = escaped_table,
        path = escaped_path,
        header = if header { "TRUE" } else { "FALSE" },
        delim = delimiter,
    );

    conn.execute_batch(&copy_sql)
        .wrap_err("COPY FROM CSV failed")?;

    println!(
        "Imported CSV `{csv}` into table `{table}`",
        csv = csv_path,
        table = table
    );
    Ok(())
}

/// Export query result to CSV using COPY ( SELECT ... ) TO ...
fn export_csv(conn: &Connection, sql: &str, csv_path: &str) -> Result<()> {
    // Ensure directory exists
    if let Some(parent) = Path::new(csv_path).parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .wrap_err_with(|| format!("failed to create directory `{}`", parent.display()))?;
        }
    }

    // Touch file early to give friendlier error if path is bad
    File::create(csv_path)
        .wrap_err_with(|| format!("failed to create output file `{csv_path}`"))?;

    let escaped_path = csv_path.replace('\'', "''");

    let copy_sql = format!(
        r#"
        COPY (
            {sql}
        )
        TO '{path}'
        (FORMAT 'csv', HEADER TRUE);
        "#,
        sql = sql,
        path = escaped_path,
    );

    conn.execute_batch(&copy_sql)
        .wrap_err("COPY TO CSV failed")?;

    println!("Exported query result to `{csv}`", csv = csv_path);
    Ok(())
}
