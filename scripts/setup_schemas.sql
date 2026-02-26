-- ============================================================================
-- SEC Smart Money - Schema Setup
-- Creates schemas for Bronze/Silver/Gold/Audit
-- ============================================================================

-- Update catalog name as needed
CREATE CATALOG IF NOT EXISTS fintech_analytics;

USE CATALOG fintech_analytics;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS audit;

-- Optional: add comments/owners if your environment supports it
-- ALTER SCHEMA bronze SET OWNER TO `data_engineers`;
-- ALTER SCHEMA silver SET OWNER TO `data_engineers`;
-- ALTER SCHEMA gold   SET OWNER TO `data_engineers`;
-- ALTER SCHEMA audit  SET OWNER TO `data_engineers`;

