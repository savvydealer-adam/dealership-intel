"""Database schema creation and migration management for dealership intel."""

import logging
import os
from typing import Optional

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


class DatabaseSchema:
    """Manages database schema creation and migrations."""

    def __init__(self, database_url: Optional[str] = None) -> None:
        """Initialize schema manager.

        Args:
            database_url: PostgreSQL connection URL. Falls back to DATABASE_URL env var.
        """
        self.database_url = database_url or os.getenv("DATABASE_URL")
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable not set")

    def create_all_tables(self) -> bool:
        """Create all required tables and indexes.

        Returns:
            True if successful, False otherwise.
        """
        try:
            with psycopg2.connect(self.database_url) as conn:
                with conn.cursor() as cur:
                    # Create tables in proper order (to respect foreign keys)
                    self._create_analysis_runs_table(cur)
                    self._create_companies_table(cur)
                    self._create_contacts_table(cur)
                    self._create_company_analysis_runs_table(cur)
                    self._create_dealership_intel_table(cur)
                    self._create_crawl_logs_table(cur)

                    # Create indexes
                    self._create_indexes(cur)

                    # Create triggers for updated_at fields
                    self._create_triggers(cur)

                    conn.commit()
                    logger.info("All database tables, indexes, and triggers created successfully")
                    return True

        except Exception as e:
            logger.error(f"Failed to create database tables: {e}")
            return False

    def _create_analysis_runs_table(self, cursor) -> None:
        """Create analysis_runs table."""
        sql = """
        CREATE TABLE IF NOT EXISTS analysis_runs (
            id SERIAL PRIMARY KEY,
            run_name VARCHAR(255) NOT NULL,
            google_sheet_url TEXT NOT NULL,
            website_column VARCHAR(100) DEFAULT 'Website',
            batch_size INTEGER DEFAULT 10,
            delay_seconds FLOAT DEFAULT 1.0,
            companies_processed INTEGER DEFAULT 0,
            companies_successful INTEGER DEFAULT 0,
            companies_failed INTEGER DEFAULT 0,
            contacts_found INTEGER DEFAULT 0,
            status VARCHAR(50) DEFAULT 'running',
            error_message TEXT,
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(sql)
        logger.debug("Created analysis_runs table")

    def _create_companies_table(self, cursor) -> None:
        """Create companies table."""
        sql = """
        CREATE TABLE IF NOT EXISTS companies (
            id SERIAL PRIMARY KEY,
            domain VARCHAR(255) UNIQUE NOT NULL,
            original_website TEXT,
            company_name VARCHAR(500),
            apollo_id VARCHAR(100),
            industry VARCHAR(255),
            company_size VARCHAR(100),
            company_phone VARCHAR(50),
            company_address TEXT,
            linkedin_url TEXT,
            status VARCHAR(50) DEFAULT 'success',
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(sql)
        logger.debug("Created companies table")

    def _create_contacts_table(self, cursor) -> None:
        """Create contacts table."""
        sql = """
        CREATE TABLE IF NOT EXISTS contacts (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
            name VARCHAR(255),
            title VARCHAR(255),
            email VARCHAR(255),
            phone VARCHAR(50),
            linkedin_url TEXT,
            confidence_score FLOAT DEFAULT 0,
            quality_flags TEXT,
            data_completeness FLOAT DEFAULT 0,
            domain_consistency FLOAT DEFAULT 0,
            professional_title FLOAT DEFAULT 0,
            linkedin_presence FLOAT DEFAULT 0,
            data_consistency FLOAT DEFAULT 0,
            email_quality FLOAT DEFAULT 0,
            email_verification_status VARCHAR(20) DEFAULT 'unverified',
            email_verification_confidence FLOAT DEFAULT 0,
            email_verification_level VARCHAR(20),
            email_verification_issues TEXT,
            email_verification_timestamp TIMESTAMP,
            contact_order INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(sql)
        logger.debug("Created contacts table")

    def _create_company_analysis_runs_table(self, cursor) -> None:
        """Create company_analysis_runs junction table."""
        sql = """
        CREATE TABLE IF NOT EXISTS company_analysis_runs (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
            analysis_run_id INTEGER NOT NULL REFERENCES analysis_runs(id) ON DELETE CASCADE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(company_id, analysis_run_id)
        );
        """
        cursor.execute(sql)
        logger.debug("Created company_analysis_runs table")

    def _create_dealership_intel_table(self, cursor) -> None:
        """Create dealership_intel table for enriched dealership data."""
        sql = """
        CREATE TABLE IF NOT EXISTS dealership_intel (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
            platform VARCHAR(100),
            new_inventory_count INTEGER,
            used_inventory_count INTEGER,
            social_links JSONB,
            review_scores JSONB,
            last_crawled_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(sql)
        logger.debug("Created dealership_intel table")

    def _create_crawl_logs_table(self, cursor) -> None:
        """Create crawl_logs table for tracking crawl operations."""
        sql = """
        CREATE TABLE IF NOT EXISTS crawl_logs (
            id SERIAL PRIMARY KEY,
            company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
            crawl_type VARCHAR(50),
            status VARCHAR(50),
            pages_visited INTEGER DEFAULT 0,
            duration_ms INTEGER,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(sql)
        logger.debug("Created crawl_logs table")

    def _create_indexes(self, cursor) -> None:
        """Create performance indexes."""
        indexes = [
            # Companies indexes
            "CREATE INDEX IF NOT EXISTS idx_companies_domain ON companies(domain);",
            "CREATE INDEX IF NOT EXISTS idx_companies_status ON companies(status);",
            "CREATE INDEX IF NOT EXISTS idx_companies_updated_at ON companies(updated_at);",
            "CREATE INDEX IF NOT EXISTS idx_companies_industry ON companies(industry);",
            # Contacts indexes
            "CREATE INDEX IF NOT EXISTS idx_contacts_company_id ON contacts(company_id);",
            "CREATE INDEX IF NOT EXISTS idx_contacts_confidence_score ON contacts(confidence_score);",
            "CREATE INDEX IF NOT EXISTS idx_contacts_contact_order ON contacts(contact_order);",
            "CREATE INDEX IF NOT EXISTS idx_contacts_email ON contacts(email);",
            "CREATE INDEX IF NOT EXISTS idx_contacts_ev_status ON contacts(email_verification_status);",
            "CREATE INDEX IF NOT EXISTS idx_contacts_ev_confidence ON contacts(email_verification_confidence);",
            # Analysis runs indexes
            "CREATE INDEX IF NOT EXISTS idx_runs_status ON analysis_runs(status);",
            "CREATE INDEX IF NOT EXISTS idx_runs_started ON analysis_runs(started_at);",
            # Junction table indexes
            "CREATE INDEX IF NOT EXISTS idx_car_company ON company_analysis_runs(company_id);",
            "CREATE INDEX IF NOT EXISTS idx_car_run ON company_analysis_runs(analysis_run_id);",
            # Dealership intel indexes
            "CREATE INDEX IF NOT EXISTS idx_dealership_intel_company_id ON dealership_intel(company_id);",
            "CREATE INDEX IF NOT EXISTS idx_dealership_intel_platform ON dealership_intel(platform);",
            "CREATE INDEX IF NOT EXISTS idx_dealership_intel_last_crawled_at ON dealership_intel(last_crawled_at);",
            # Crawl logs indexes
            "CREATE INDEX IF NOT EXISTS idx_crawl_logs_company_id ON crawl_logs(company_id);",
            "CREATE INDEX IF NOT EXISTS idx_crawl_logs_crawl_type ON crawl_logs(crawl_type);",
            "CREATE INDEX IF NOT EXISTS idx_crawl_logs_status ON crawl_logs(status);",
            "CREATE INDEX IF NOT EXISTS idx_crawl_logs_created_at ON crawl_logs(created_at);",
        ]

        for index_sql in indexes:
            try:
                cursor.execute(index_sql)
            except Exception as e:
                logger.warning(f"Index creation warning: {e}")

        logger.debug("Created database indexes")

    def _create_triggers(self, cursor) -> None:
        """Create database triggers for updated_at fields."""
        trigger_function = """
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ language 'plpgsql';
        """
        cursor.execute(trigger_function)

        # Add triggers to tables with updated_at columns
        tables_with_updated_at = [
            "companies",
            "contacts",
            "analysis_runs",
            "dealership_intel",
        ]
        for table in tables_with_updated_at:
            trigger_sql = f"""
            DROP TRIGGER IF EXISTS update_{table}_updated_at ON {table};
            CREATE TRIGGER update_{table}_updated_at
                BEFORE UPDATE ON {table}
                FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
            """
            cursor.execute(trigger_sql)

        logger.debug("Created database triggers")

    def check_tables_exist(self) -> dict[str, bool]:
        """Check which tables exist in the database.

        Returns:
            Dictionary with table names as keys and existence status as values.
        """
        required_tables = [
            "analysis_runs",
            "companies",
            "contacts",
            "company_analysis_runs",
            "dealership_intel",
            "crawl_logs",
        ]
        table_status: dict[str, bool] = {}

        try:
            with psycopg2.connect(self.database_url) as conn:
                with conn.cursor() as cur:
                    for table in required_tables:
                        cur.execute(
                            """
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables
                                WHERE table_schema = 'public'
                                AND table_name = %s
                            );
                            """,
                            (table,),
                        )
                        result = cur.fetchone()
                        table_status[table] = result[0] if result else False

            return table_status

        except Exception as e:
            logger.error(f"Failed to check table existence: {e}")
            return {table: False for table in required_tables}

    def get_table_counts(self) -> dict[str, int]:
        """Get row counts for all tables.

        Returns:
            Dictionary with table names as keys and row counts as values.
        """
        tables = [
            "analysis_runs",
            "companies",
            "contacts",
            "company_analysis_runs",
            "dealership_intel",
            "crawl_logs",
        ]
        counts: dict[str, int] = {}

        try:
            with psycopg2.connect(self.database_url) as conn:
                with conn.cursor() as cur:
                    for table in tables:
                        try:
                            cur.execute(f"SELECT COUNT(*) FROM {table};")
                            result = cur.fetchone()
                            counts[table] = result[0] if result else 0
                        except psycopg2.Error:
                            counts[table] = 0

            return counts

        except Exception as e:
            logger.error(f"Failed to get table counts: {e}")
            return {table: 0 for table in tables}

    def test_database_connection(self) -> tuple[bool, str]:
        """Test database connection and basic functionality.

        Returns:
            Tuple of (success, message).
        """
        try:
            with psycopg2.connect(self.database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
                    result = cur.fetchone()
                    if result and result[0] == 1:
                        return True, "Database connection successful"
                    else:
                        return False, "Database connection test failed"

        except Exception as e:
            return False, f"Database connection failed: {str(e)}"

    def migrate_database(self) -> bool:
        """Apply database migrations to existing database.

        Returns:
            True if successful, False otherwise.
        """
        try:
            with psycopg2.connect(self.database_url) as conn:
                with conn.cursor() as cur:
                    # Check if contacts table needs email verification columns
                    if self._needs_email_verification_migration(cur):
                        logger.info("Applying email verification columns migration")
                        if not self._migrate_email_verification_columns(cur):
                            return False

                    # Check if intel tables need to be created
                    if self._needs_intel_tables_migration(cur):
                        logger.info("Applying intel tables migration")
                        if not self._migrate_intel_tables(cur):
                            return False

                    # Check if autotrader columns need to be added
                    if self._needs_autotrader_migration(cur):
                        logger.info("Applying autotrader columns migration")
                        if not self._migrate_autotrader_columns(cur):
                            return False

                    conn.commit()
                    logger.info("Database migrations applied successfully")
                    return True

        except Exception as e:
            logger.error(f"Database migration failed: {e}")
            return False

    def _needs_email_verification_migration(self, cursor) -> bool:
        """Check if contacts table needs email verification columns migration.

        Args:
            cursor: Database cursor.

        Returns:
            True if migration is needed, False otherwise.
        """
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'contacts'
            AND column_name = 'email_verification_status'
            """
        )
        result = cursor.fetchone()
        return result is None

    def _migrate_email_verification_columns(self, cursor) -> bool:
        """Add email verification columns to contacts table.

        Args:
            cursor: Database cursor.

        Returns:
            True if successful, False otherwise.
        """
        try:
            migration_sql = [
                "ALTER TABLE contacts ADD COLUMN IF NOT EXISTS"
                " email_verification_status VARCHAR(20) DEFAULT 'unverified';",
                "ALTER TABLE contacts ADD COLUMN IF NOT EXISTS email_verification_confidence FLOAT DEFAULT 0;",
                "ALTER TABLE contacts ADD COLUMN IF NOT EXISTS email_verification_level VARCHAR(20);",
                "ALTER TABLE contacts ADD COLUMN IF NOT EXISTS email_verification_issues TEXT;",
                "ALTER TABLE contacts ADD COLUMN IF NOT EXISTS email_verification_timestamp TIMESTAMP;",
            ]

            for sql in migration_sql:
                cursor.execute(sql)
                logger.debug(f"Executed migration: {sql[:60]}...")

            # Add indexes for email verification columns
            index_sql = [
                "CREATE INDEX IF NOT EXISTS idx_contacts_ev_status ON contacts(email_verification_status);",
                "CREATE INDEX IF NOT EXISTS idx_contacts_ev_confidence ON contacts(email_verification_confidence);",
            ]

            for sql in index_sql:
                cursor.execute(sql)
                logger.debug(f"Created index: {sql}")

            logger.info("Email verification columns migration completed successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to migrate email verification columns: {e}")
            return False

    def _needs_intel_tables_migration(self, cursor) -> bool:
        """Check if dealership_intel and crawl_logs tables need to be created.

        Args:
            cursor: Database cursor.

        Returns:
            True if migration is needed, False otherwise.
        """
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = 'dealership_intel'
            );
            """
        )
        intel_exists = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = 'crawl_logs'
            );
            """
        )
        crawl_exists = cursor.fetchone()[0]

        return not intel_exists or not crawl_exists

    def _migrate_intel_tables(self, cursor) -> bool:
        """Create dealership_intel and crawl_logs tables if they don't exist.

        Args:
            cursor: Database cursor.

        Returns:
            True if successful, False otherwise.
        """
        try:
            self._create_dealership_intel_table(cursor)
            self._create_crawl_logs_table(cursor)

            # Create indexes for the new tables
            intel_indexes = [
                "CREATE INDEX IF NOT EXISTS idx_dealership_intel_company_id ON dealership_intel(company_id);",
                "CREATE INDEX IF NOT EXISTS idx_dealership_intel_platform ON dealership_intel(platform);",
                "CREATE INDEX IF NOT EXISTS idx_dealership_intel_last_crawled_at ON dealership_intel(last_crawled_at);",
                "CREATE INDEX IF NOT EXISTS idx_crawl_logs_company_id ON crawl_logs(company_id);",
                "CREATE INDEX IF NOT EXISTS idx_crawl_logs_crawl_type ON crawl_logs(crawl_type);",
                "CREATE INDEX IF NOT EXISTS idx_crawl_logs_status ON crawl_logs(status);",
                "CREATE INDEX IF NOT EXISTS idx_crawl_logs_created_at ON crawl_logs(created_at);",
            ]

            for sql in intel_indexes:
                try:
                    cursor.execute(sql)
                except Exception as e:
                    logger.warning(f"Intel index creation warning: {e}")

            # Add updated_at trigger for dealership_intel
            trigger_sql = """
            DROP TRIGGER IF EXISTS update_dealership_intel_updated_at ON dealership_intel;
            CREATE TRIGGER update_dealership_intel_updated_at
                BEFORE UPDATE ON dealership_intel
                FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
            """
            cursor.execute(trigger_sql)

            logger.info("Intel tables migration completed successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to migrate intel tables: {e}")
            return False

    def _needs_autotrader_migration(self, cursor) -> bool:
        """Check if companies table needs autotrader_dealer_id column."""
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'companies'
            AND column_name = 'autotrader_dealer_id'
            """
        )
        return cursor.fetchone() is None

    def _migrate_autotrader_columns(self, cursor) -> bool:
        """Add autotrader_dealer_id column and index to companies table."""
        try:
            cursor.execute("ALTER TABLE companies ADD COLUMN IF NOT EXISTS autotrader_dealer_id VARCHAR(20);")
            cursor.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_companies_autotrader_dealer_id"
                " ON companies(autotrader_dealer_id)"
                " WHERE autotrader_dealer_id IS NOT NULL;"
            )
            # Ensure dealership_intel has a unique constraint on company_id
            # for upsert support (ON CONFLICT)
            cursor.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_dealership_intel_company_id_unique"
                " ON dealership_intel(company_id);"
            )
            logger.info("Autotrader columns migration completed successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to migrate autotrader columns: {e}")
            return False

    def reset_database(self) -> bool:
        """DANGEROUS: Drop all tables and recreate them. Use only for development/testing.

        Returns:
            True if successful, False otherwise.
        """
        try:
            with psycopg2.connect(self.database_url) as conn:
                with conn.cursor() as cur:
                    # Drop tables in reverse order due to foreign keys
                    drop_order = [
                        "crawl_logs",
                        "dealership_intel",
                        "company_analysis_runs",
                        "contacts",
                        "companies",
                        "analysis_runs",
                    ]
                    for table in drop_order:
                        cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")

                    conn.commit()
                    logger.info("All tables dropped successfully")

                    # Recreate all tables
                    return self.create_all_tables()

        except Exception as e:
            logger.error(f"Failed to reset database: {e}")
            return False
