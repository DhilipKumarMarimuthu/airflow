CREATE TABLE IF NOT EXISTS well (
    well_name TEXT NOT NULL,
    is_newly_added BOOLEAN NOT NULL,
    PRIMARY KEY (well_name)
);