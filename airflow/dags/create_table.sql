USE test_schema;
CREATE TABLE IF NOT EXISTS `BlogScraper` (
      `title` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
      `article_id` int COLLATE utf8mb4_unicode_ci NOT NULL,
      `article_url` varchar(4096) COLLATE utf8mb4_unicode_ci NOT NULL,
      `year` int COLLATE utf8mb4_unicode_ci NOT NULL,
      `month` int COLLATE utf8mb4_unicode_ci NOT NULL,
      PRIMARY KEY (`article_id`)
)