-- phpMyAdmin SQL Dump
-- version 4.5.4.1deb2ubuntu2
-- http://www.phpmyadmin.net
--
-- Host: localhost
-- Generation Time: Dec 18, 2016 at 05:09 PM
-- Server version: 5.7.16-0ubuntu0.16.04.1
-- PHP Version: 7.0.8-0ubuntu0.16.04.3

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `researchgraph`
--

-- --------------------------------------------------------

--
-- Table structure for table `doi_author`
--

CREATE TABLE `doi_author` (
  `author_id` bigint(20) UNSIGNED NOT NULL,
  `resolution_id` bigint(20) UNSIGNED NOT NULL,
  `first_name` varchar(255) NOT NULL,
  `last_name` varchar(255) NOT NULL,
  `full_name` varchar(255) NOT NULL,
  `orcid` varchar(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `doi_autority`
--

CREATE TABLE `doi_autority` (
  `id` bigint(20) UNSIGNED NOT NULL,
  `doi` varchar(1024) NOT NULL,
  `autority` varchar(32) NOT NULL,
  `created` datetime NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `doi_resolution`
--

CREATE TABLE `doi_resolution` (
  `id` bigint(20) UNSIGNED NOT NULL,
  `source` varchar(32) DEFAULT NULL,
  `source_url` varchar(64) DEFAULT NULL,
  `doi` varchar(1024) NOT NULL,
  `url` varchar(1024) DEFAULT NULL,
  `title` varchar(1024) DEFAULT NULL,
  `year` smallint(5) UNSIGNED DEFAULT NULL,
  `created` datetime NOT NULL,
  `resolved` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Indexes for dumped tables
--

--
-- Indexes for table `doi_author`
--
ALTER TABLE `doi_author`
  ADD PRIMARY KEY (`author_id`),
  ADD UNIQUE KEY `resolution_id` (`resolution_id`,`full_name`,`orcid`);

--
-- Indexes for table `doi_autority`
--
ALTER TABLE `doi_autority`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `doi` (`doi`),
  ADD KEY `autority` (`autority`);

--
-- Indexes for table `doi_resolution`
--
ALTER TABLE `doi_resolution`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `doi` (`doi`),
  ADD KEY `resolved` (`resolved`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `doi_author`
--
ALTER TABLE `doi_author`
  MODIFY `author_id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT;
--
-- AUTO_INCREMENT for table `doi_autority`
--
ALTER TABLE `doi_autority`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT;
--
-- AUTO_INCREMENT for table `doi_resolution`
--
ALTER TABLE `doi_resolution`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT;
--
-- Constraints for dumped tables
--

--
-- Constraints for table `doi_author`
--
ALTER TABLE `doi_author`
  ADD CONSTRAINT `doi_author_ibfk_1` FOREIGN KEY (`resolution_id`) REFERENCES `doi_resolution` (`id`) ON DELETE CASCADE ON UPDATE CASCADE;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
