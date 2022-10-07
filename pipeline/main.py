from pipeline.settings import db_connection


def init_database():
    """
    Initializes database and creates default tables
    """
    db_connection.execute(
        """
        CREATE TABLE energy_vectors(
            id INTEGER PRIMARY KEY,
            technology_type VARCHAR,
            parameter_name VARCHAR,
            input_vector VARCHAR,
            output_vector VARCHAR
        )
        """
    )


if __name__ == "__main__":
    init_database()
