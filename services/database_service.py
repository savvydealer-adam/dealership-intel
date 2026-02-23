"""Database service for managing all CRUD operations for dealership intel."""

import logging
import os
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Optional

import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool

from services.database_schema import DatabaseSchema

logger = logging.getLogger(__name__)


class DatabaseService:
    """Service for managing database operations for dealership intel."""

    def __init__(self, database_url: Optional[str] = None, auto_initialize: bool = True) -> None:
        """Initialize database service.

        Args:
            database_url: PostgreSQL connection URL. Falls back to DATABASE_URL env var.
            auto_initialize: Whether to automatically initialize database schema.
        """
        self.database_url = database_url or os.getenv("DATABASE_URL")
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable not set")

        # Initialize schema manager
        self.schema = DatabaseSchema(self.database_url)

        # Create connection pool for better performance
        try:
            self.pool = SimpleConnectionPool(1, 10, dsn=self.database_url)
            logger.info("Database connection pool created successfully")

            # Initialize database schema and run migrations if requested
            if auto_initialize:
                self.initialize_database()
                self.run_migrations()

        except Exception as e:
            logger.error(f"Failed to create database connection pool: {e}")
            raise

    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = None
        try:
            conn = self.pool.getconn()
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database operation failed: {e}")
            raise
        finally:
            if conn:
                self.pool.putconn(conn)

    def create_analysis_run(
        self,
        run_name: str,
        google_sheet_url: str,
        website_column: str = "Website",
        batch_size: int = 10,
        delay_seconds: float = 1.0,
    ) -> int:
        """Create a new analysis run record.

        Returns:
            analysis_run_id: ID of the created analysis run.
        """
        query = """
        INSERT INTO analysis_runs (run_name, google_sheet_url, website_column,
                                 batch_size, delay_seconds, status)
        VALUES (%s, %s, %s, %s, %s, 'running')
        RETURNING id
        """

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query,
                    (run_name, google_sheet_url, website_column, batch_size, delay_seconds),
                )
                conn.commit()
                return cur.fetchone()[0]

    def update_analysis_run_stats(
        self,
        analysis_run_id: int,
        companies_processed: Optional[int] = None,
        companies_successful: Optional[int] = None,
        companies_failed: Optional[int] = None,
        contacts_found: Optional[int] = None,
        status: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """Update analysis run statistics."""
        updates: list[str] = []
        params: list[Any] = []

        if companies_processed is not None:
            updates.append("companies_processed = %s")
            params.append(companies_processed)
        if companies_successful is not None:
            updates.append("companies_successful = %s")
            params.append(companies_successful)
        if companies_failed is not None:
            updates.append("companies_failed = %s")
            params.append(companies_failed)
        if contacts_found is not None:
            updates.append("contacts_found = %s")
            params.append(contacts_found)
        if status is not None:
            updates.append("status = %s")
            params.append(status)
            if status == "completed":
                updates.append("completed_at = CURRENT_TIMESTAMP")
        if error_message is not None:
            updates.append("error_message = %s")
            params.append(error_message)

        if not updates:
            return

        query = f"""
        UPDATE analysis_runs
        SET {", ".join(updates)}
        WHERE id = %s
        """
        params.append(analysis_run_id)

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                conn.commit()

    def get_company_by_domain(self, domain: str) -> Optional[dict[str, Any]]:
        """Get company by domain if it exists.

        Returns:
            Company record or None if not found.
        """
        query = """
        SELECT id, domain, original_website, company_name, apollo_id, industry,
               company_size, company_phone, company_address, linkedin_url,
               status, error_message, created_at, updated_at
        FROM companies
        WHERE domain = %s
        """

        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, (domain,))
                result = cur.fetchone()
                return dict(result) if result else None

    def save_company(self, company_data: dict[str, Any], analysis_run_id: int) -> int:
        """Save or update company data.

        Args:
            company_data: Company information dictionary.
            analysis_run_id: ID of the current analysis run.

        Returns:
            company_id: ID of the saved/updated company.
        """
        domain = company_data.get("domain")
        if not domain:
            raise ValueError("Domain is required for company data")

        # Check if company already exists
        existing_company = self.get_company_by_domain(domain)

        if existing_company:
            # Update existing company
            company_id = existing_company["id"]
            self._update_company(company_id, company_data)
        else:
            # Create new company
            company_id = self._create_company(company_data)

        # Link company to analysis run
        self._link_company_to_analysis_run(company_id, analysis_run_id)

        return company_id

    def _create_company(self, company_data: dict[str, Any]) -> int:
        """Create new company record."""
        query = """
        INSERT INTO companies (domain, original_website, company_name, apollo_id,
                             industry, company_size, company_phone, company_address,
                             linkedin_url, status, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query,
                    (
                        company_data.get("domain"),
                        company_data.get("original_website"),
                        company_data.get("company_name"),
                        company_data.get("company_id"),  # Apollo ID
                        company_data.get("industry"),
                        company_data.get("company_size"),
                        company_data.get("company_phone"),
                        company_data.get("company_address"),
                        company_data.get("linkedin_url"),
                        company_data.get("status", "success"),
                        company_data.get("error_message"),
                    ),
                )
                conn.commit()
                return cur.fetchone()[0]

    def _update_company(self, company_id: int, company_data: dict[str, Any]) -> None:
        """Update existing company record with new data."""
        # Only update fields that have values and are potentially better
        updates: list[str] = []
        params: list[Any] = []

        # Always update these fields if provided
        for field in [
            "company_name",
            "industry",
            "company_size",
            "company_phone",
            "company_address",
            "linkedin_url",
            "status",
            "error_message",
            "company_id",
        ]:
            if company_data.get(field) is not None:
                db_field = "apollo_id" if field == "company_id" else field
                updates.append(f"{db_field} = %s")
                params.append(company_data[field])

        if updates:
            query = f"""
            UPDATE companies
            SET {", ".join(updates)}
            WHERE id = %s
            """
            params.append(company_id)

            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    conn.commit()

    def _link_company_to_analysis_run(self, company_id: int, analysis_run_id: int) -> None:
        """Link company to analysis run (if not already linked)."""
        query = """
        INSERT INTO company_analysis_runs (company_id, analysis_run_id)
        VALUES (%s, %s)
        ON CONFLICT (company_id, analysis_run_id) DO NOTHING
        """

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (company_id, analysis_run_id))
                conn.commit()

    def save_contacts(self, company_id: int, contacts_data: list[dict[str, Any]]) -> None:
        """Save contacts for a company (replacing existing contacts).

        Args:
            company_id: ID of the company.
            contacts_data: List of contact dictionaries.
        """
        # Clear existing contacts for this company
        self._delete_company_contacts(company_id)

        # Insert new contacts
        if not contacts_data:
            return

        query = """
        INSERT INTO contacts (company_id, name, title, email, phone, linkedin_url,
                            confidence_score, quality_flags, data_completeness,
                            domain_consistency, professional_title, linkedin_presence,
                            data_consistency, email_quality, email_verification_status,
                            email_verification_confidence, email_verification_level,
                            email_verification_issues, email_verification_timestamp, contact_order)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                for i, contact in enumerate(contacts_data):
                    cur.execute(
                        query,
                        (
                            company_id,
                            contact.get("name"),
                            contact.get("title"),
                            contact.get("email"),
                            contact.get("phone"),
                            contact.get("linkedin_url"),
                            contact.get("confidence_score", 0),
                            contact.get("quality_flags"),
                            contact.get("data_completeness", 0),
                            contact.get("domain_consistency", 0),
                            contact.get("professional_title", 0),
                            contact.get("linkedin_presence", 0),
                            contact.get("data_consistency", 0),
                            contact.get("email_quality", 0),
                            contact.get("email_verification_status", "unverified"),
                            contact.get("email_verification_confidence", 0),
                            contact.get("email_verification_level"),
                            contact.get("email_verification_issues"),
                            contact.get("email_verification_timestamp"),
                            i + 1,  # contact_order
                        ),
                    )
                conn.commit()

    def _delete_company_contacts(self, company_id: int) -> None:
        """Delete all contacts for a company."""
        query = "DELETE FROM contacts WHERE company_id = %s"

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (company_id,))
                conn.commit()

    def search_companies(
        self,
        search_term: str = "",
        industry: str = "",
        status: Optional[str] = None,
        min_confidence: float = 0,
        date_from: str = "",
        date_to: str = "",
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        """Search companies with filters.

        Returns:
            Tuple of (results_list, total_count).
        """
        conditions: list[str] = []
        params: list[Any] = []

        # Build WHERE conditions
        if search_term:
            conditions.append("(c.company_name ILIKE %s OR c.domain ILIKE %s)")
            params.extend([f"%{search_term}%", f"%{search_term}%"])

        if industry:
            conditions.append("c.industry ILIKE %s")
            params.append(f"%{industry}%")

        if status:
            conditions.append("c.status = %s")
            params.append(status)

        if date_from:
            conditions.append("c.created_at >= %s")
            params.append(date_from)

        if date_to:
            conditions.append("c.created_at <= %s")
            params.append(date_to)

        # Add minimum confidence filter
        if min_confidence > 0:
            conditions.append(
                """
            c.id IN (
                SELECT DISTINCT company_id
                FROM contacts
                WHERE confidence_score >= %s
            )
            """
            )
            params.append(min_confidence)

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

        # Count query
        count_query = f"""
        SELECT COUNT(DISTINCT c.id)
        FROM companies c
        {where_clause}
        """

        # Main query with contacts
        main_query = f"""
        SELECT c.*,
               COALESCE(
                   json_agg(
                       json_build_object(
                           'name', ct.name,
                           'title', ct.title,
                           'email', ct.email,
                           'phone', ct.phone,
                           'linkedin_url', ct.linkedin_url,
                           'confidence_score', ct.confidence_score,
                           'quality_flags', ct.quality_flags,
                           'contact_order', ct.contact_order
                       ) ORDER BY ct.contact_order
                   ) FILTER (WHERE ct.id IS NOT NULL),
                   '[]'::json
               ) as contacts
        FROM companies c
        LEFT JOIN contacts ct ON c.id = ct.company_id
        {where_clause}
        GROUP BY c.id
        ORDER BY c.updated_at DESC
        LIMIT %s OFFSET %s
        """

        params_main = params + [limit, offset]

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Get total count (use regular cursor for simple count)
                cur.execute(count_query, params)
                count_result = cur.fetchone()
                total_count = count_result[0] if count_result else 0

            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                # Get results (use RealDictCursor for complex results)
                cur.execute(main_query, params_main)
                results = [dict(row) for row in cur.fetchall()]

                return results, total_count

    def get_analysis_runs(self, limit: int = 50) -> list[dict[str, Any]]:
        """Get recent analysis runs."""
        query = """
        SELECT id, run_name, google_sheet_url, website_column,
               companies_processed, companies_successful, companies_failed,
               contacts_found, batch_size, delay_seconds,
               started_at, completed_at, status, error_message
        FROM analysis_runs
        ORDER BY started_at DESC
        LIMIT %s
        """

        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, (limit,))
                return [dict(row) for row in cur.fetchall()]

    def get_database_stats(self) -> dict[str, Any]:
        """Get database statistics."""
        query = """
        SELECT
            (SELECT COUNT(*) FROM companies) as total_companies,
            (SELECT COUNT(*) FROM contacts) as total_contacts,
            (SELECT COUNT(*) FROM analysis_runs) as total_analysis_runs,
            (SELECT COUNT(*) FROM companies WHERE status = 'Success') as successful_companies,
            (SELECT AVG(confidence_score) FROM contacts WHERE confidence_score > 0) as avg_confidence_score,
            (SELECT MAX(updated_at) FROM companies) as last_company_update,
            (SELECT MAX(started_at) FROM analysis_runs) as last_analysis_run
        """

        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query)
                return dict(cur.fetchone())

    def export_companies_to_dataframe(self, include_contacts: bool = True) -> pd.DataFrame:
        """Export all companies and optionally contacts to DataFrame."""
        if include_contacts:
            query = """
            SELECT c.*,
                   ct.name as contact_name,
                   ct.title as contact_title,
                   ct.email as contact_email,
                   ct.phone as contact_phone,
                   ct.linkedin_url as contact_linkedin,
                   ct.confidence_score as contact_confidence_score,
                   ct.quality_flags as contact_quality_flags,
                   ct.contact_order
            FROM companies c
            LEFT JOIN contacts ct ON c.id = ct.company_id
            ORDER BY c.id, ct.contact_order
            """
        else:
            query = "SELECT * FROM companies ORDER BY updated_at DESC"

        with self.get_connection() as conn:
            return pd.read_sql(query, conn)

    def cleanup_old_analysis_runs(self, days_old: int = 30) -> int:
        """Clean up analysis runs older than specified days.

        Returns:
            Number of deleted runs.
        """
        query = """
        DELETE FROM analysis_runs
        WHERE started_at < CURRENT_TIMESTAMP - INTERVAL '%s days'
        AND status IN ('completed', 'failed', 'cancelled')
        """

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (days_old,))
                deleted_count = cur.rowcount
                conn.commit()
                return deleted_count

    def initialize_database(self) -> bool:
        """Initialize database schema if not already done.

        Returns:
            True if successful, False otherwise.
        """
        try:
            # Check if tables exist
            table_status = self.schema.check_tables_exist()
            missing_tables = [table for table, exists in table_status.items() if not exists]

            if missing_tables:
                logger.info(f"Creating missing tables: {missing_tables}")
                success = self.schema.create_all_tables()
                if success:
                    logger.info("Database schema initialized successfully")
                    return True
                else:
                    logger.error("Failed to initialize database schema")
                    return False
            else:
                logger.debug("All database tables already exist")
                return True

        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            return False

    def run_migrations(self) -> bool:
        """Run database migrations to update existing schema.

        Returns:
            True if successful, False otherwise.
        """
        try:
            success = self.schema.migrate_database()
            if success:
                logger.info("Database migrations completed successfully")
            else:
                logger.error("Database migrations failed")
            return success
        except Exception as e:
            logger.error(f"Migration execution failed: {e}")
            return False

    def get_database_health(self) -> dict[str, Any]:
        """Get comprehensive database health information.

        Returns:
            Dictionary with health status, table info, and statistics.
        """
        health_info: dict[str, Any] = {
            "connection_healthy": False,
            "tables_exist": {},
            "table_counts": {},
            "error_message": None,
            "last_check": datetime.now().isoformat(),
        }

        try:
            # Test connection
            connection_ok, connection_msg = self.schema.test_database_connection()
            health_info["connection_healthy"] = connection_ok

            if not connection_ok:
                health_info["error_message"] = connection_msg
                return health_info

            # Check tables
            health_info["tables_exist"] = self.schema.check_tables_exist()
            health_info["table_counts"] = self.schema.get_table_counts()

            # Get database stats if tables exist
            all_tables_exist = all(health_info["tables_exist"].values())
            if all_tables_exist:
                stats = self.get_database_stats()
                health_info.update(stats)

        except Exception as e:
            health_info["error_message"] = str(e)
            logger.error(f"Database health check failed: {e}")

        return health_info

    def force_initialize_database(self) -> bool:
        """Force recreate all database tables (DANGEROUS - for development only).

        Returns:
            True if successful, False otherwise.
        """
        try:
            logger.warning("DANGER: Force recreating all database tables")
            success = self.schema.reset_database()
            if success:
                logger.info("Database force-reinitialized successfully")
            return success
        except Exception as e:
            logger.error(f"Force database initialization failed: {e}")
            return False

    def get_contacts_by_verification_status(
        self, status: str, limit: int = 100, offset: int = 0
    ) -> list[dict[str, Any]]:
        """Get contacts filtered by email verification status.

        Args:
            status: Verification status ('verified', 'invalid', 'unverified', 'error').
            limit: Maximum number of results.
            offset: Offset for pagination.

        Returns:
            List of contact dictionaries.
        """
        query = """
        SELECT c.*, comp.company_name, comp.domain
        FROM contacts c
        JOIN companies comp ON c.company_id = comp.id
        WHERE c.email_verification_status = %s
        ORDER BY c.email_verification_confidence DESC, c.confidence_score DESC
        LIMIT %s OFFSET %s
        """

        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, (status, limit, offset))
                results = cur.fetchall()
                return [dict(row) for row in results]

    def get_email_verification_statistics(self) -> dict[str, Any]:
        """Get email verification statistics across all contacts.

        Returns:
            Dictionary with verification statistics.
        """
        query = """
        SELECT
            email_verification_status,
            COUNT(*) as count,
            AVG(email_verification_confidence) as avg_confidence,
            AVG(confidence_score) as avg_overall_confidence
        FROM contacts
        WHERE email IS NOT NULL AND email != ''
        GROUP BY email_verification_status
        """

        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query)
                results = cur.fetchall()

                # Build statistics dictionary
                stats: dict[str, Any] = {
                    "total_contacts_with_email": 0,
                    "verification_breakdown": {},
                    "overall_verification_rate": 0.0,
                }

                verified_count = 0
                total_count = 0

                for row in results:
                    verification_status = row["email_verification_status"]
                    count = row["count"]

                    stats["verification_breakdown"][verification_status] = {
                        "count": count,
                        "avg_verification_confidence": float(row["avg_confidence"] or 0),
                        "avg_overall_confidence": float(row["avg_overall_confidence"] or 0),
                    }

                    total_count += count
                    if verification_status == "verified":
                        verified_count += count

                stats["total_contacts_with_email"] = total_count
                stats["overall_verification_rate"] = (verified_count / total_count * 100) if total_count > 0 else 0

                return stats

    def update_contact_verification(self, contact_id: int, verification_data: dict[str, Any]) -> None:
        """Update email verification data for a specific contact.

        Args:
            contact_id: ID of the contact to update.
            verification_data: Dictionary with verification fields.
        """
        query = """
        UPDATE contacts
        SET email_verification_status = %s,
            email_verification_confidence = %s,
            email_verification_level = %s,
            email_verification_issues = %s,
            email_verification_timestamp = %s
        WHERE id = %s
        """

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query,
                    (
                        verification_data.get("status", "unverified"),
                        verification_data.get("confidence", 0),
                        verification_data.get("level"),
                        verification_data.get("issues"),
                        verification_data.get("timestamp"),
                        contact_id,
                    ),
                )
                conn.commit()

    def get_unverified_contacts(self, limit: int = 100) -> list[dict[str, Any]]:
        """Get contacts that haven't been verified yet for batch processing.

        Args:
            limit: Maximum number of contacts to return.

        Returns:
            List of contact dictionaries.
        """
        query = """
        SELECT c.*, comp.domain
        FROM contacts c
        JOIN companies comp ON c.company_id = comp.id
        WHERE c.email IS NOT NULL
        AND c.email != ''
        AND (c.email_verification_status IS NULL OR c.email_verification_status = 'unverified')
        ORDER BY c.confidence_score DESC
        LIMIT %s
        """

        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, (limit,))
                results = cur.fetchall()
                return [dict(row) for row in results]

    def close(self) -> None:
        """Close database connection pool."""
        if hasattr(self, "pool") and self.pool:
            self.pool.closeall()
            logger.info("Database connection pool closed")
