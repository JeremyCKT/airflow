USE test_schema;
CREATE TABLE IF NOT EXISTS `WPScraper` (
      `title` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
      `date` datetime COLLATE utf8mb4_unicode_ci NOT NULL,
      `link` varchar(4096) COLLATE utf8mb4_unicode_ci NOT NULL,
      PRIMARY KEY (`title`)
)