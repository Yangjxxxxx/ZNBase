-- MySQL dump 10.13  Distrib 8.0.12, for osx10.14 (x86_64)
--
-- Host: localhost    Database: znbasetestdata
-- ------------------------------------------------------
-- Server version	8.0.12

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
 SET NAMES utf8mb4 ;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `everything`
--

DROP TABLE IF EXISTS `everything`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `everything` (
  `i` int(11) NOT NULL,
  `c` char(10) NOT NULL,
  `s` varchar(100) DEFAULT 'this is s''s default value',
  `tx` text,
  `e` enum('Small','Medium','Large') DEFAULT NULL,
  `bin` binary(100) NOT NULL,
  `vbin` varbinary(100) DEFAULT NULL,
  `bl` blob,
  `dt` datetime NOT NULL DEFAULT '2000-01-01 00:00:00',
  `d` date DEFAULT NULL,
  `ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `t` time DEFAULT NULL,
  `de` decimal(10,0) DEFAULT NULL,
  `nu` decimal(10,0) DEFAULT NULL,
  `d53` decimal(5,3) DEFAULT NULL,
  `iw` int(5) NOT NULL,
  `iz` int(10) unsigned zerofill DEFAULT NULL,
  `ti` tinyint(4) DEFAULT '5',
  `si` smallint(6) DEFAULT NULL,
  `mi` mediumint(9) DEFAULT NULL,
  `bi` bigint(20) DEFAULT NULL,
  `fl` float NOT NULL,
  `rl` double DEFAULT NULL,
  `db` double DEFAULT NULL,
  `f17` float DEFAULT NULL,
  `f47` double DEFAULT NULL,
  `f75` float(7,5) DEFAULT NULL,
  `j` json DEFAULT NULL,
  PRIMARY KEY (`i`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `everything`
--

LOCK TABLES `everything` WRITE;
/*!40000 ALTER TABLE `everything` DISABLE KEYS */;
INSERT INTO `everything` VALUES (1,'c','this is s\'s default value',NULL,'Small',_binary 'bin\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0',NULL,NULL,'2000-01-01 00:00:00',NULL,'2018-11-19 20:27:42',NULL,NULL,NULL,-12.345,-2,0000000001,5,NULL,NULL,NULL,-1.5,NULL,NULL,NULL,NULL,NULL,'{\"a\": \"b\", \"c\": {\"d\": [\"e\", 11, null]}}'),(2,'c2','this is s\'s default value',NULL,'Large',_binary 'bin2\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0',NULL,NULL,'2000-01-01 00:00:00',NULL,'2018-11-19 20:27:42',NULL,NULL,NULL,12.345,3,3525343334,5,NULL,NULL,NULL,1.2,NULL,NULL,NULL,NULL,NULL,'{}');
/*!40000 ALTER TABLE `everything` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `SECOND`
--

DROP TABLE IF EXISTS `SECOND`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `SECOND` (
  `i` int(11) NOT NULL,
  `k` int(11) DEFAULT NULL,
  PRIMARY KEY (`i`),
  UNIQUE KEY `ik` (`i`,`k`),
  KEY `ki` (`k`,`i`),
  CONSTRAINT `second_ibfk_1` FOREIGN KEY (`k`) REFERENCES `simple` (`i`) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `SECOND`
--

LOCK TABLES `SECOND` WRITE;
/*!40000 ALTER TABLE `SECOND` DISABLE KEYS */;
INSERT INTO `SECOND` VALUES (-7,7),(-6,6),(-5,5),(-4,4),(-3,3),(-2,2),(-1,1);
/*!40000 ALTER TABLE `SECOND` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `simple`
--

DROP TABLE IF EXISTS `simple`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `simple` (
  `i` int(11) NOT NULL AUTO_INCREMENT,
  `s` text,
  `b` binary(200) DEFAULT NULL,
  PRIMARY KEY (`i`)
) ENGINE=InnoDB AUTO_INCREMENT=31 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `simple`
--

LOCK TABLES `simple` WRITE;
/*!40000 ALTER TABLE `simple` DISABLE KEYS */;
INSERT INTO `simple` VALUES (1,'str',NULL),(2,'',NULL),(3,' ',NULL),(4,',',NULL),(5,'\n',NULL),(6,'\\n',NULL),(7,'\r\n',NULL),(8,'\r',NULL),(9,'\"',NULL),(10,NULL,NULL),(11,'\\N',NULL),(12,'NULL',NULL),(13,'¢',NULL),(14,' ¢ ',NULL),(15,'✅',NULL),(16,'\",\"\\n,™¢',NULL),(17,'\0',NULL),(18,'✅¢©ƒƒƒƒåß∂√œ∫∑∆πœ∑˚¬≤µµç∫ø∆œ∑∆¬œ∫œ∑´´†¥¨ˆˆπ‘“æ…¬…¬˚ß∆å˚˙ƒ∆©˙©∂˙≥≤Ω˜˜µ√∫∫Ω¥∑',NULL),(19,'a quote \" or two quotes \"\" and a quote-comma \", , and then a quote and newline \"\n',NULL),(20,'\"a slash \\, a double slash \\\\, a slash+quote \\\",  \\\n',NULL),(21,'\nॹ\";\n\r;	ᐿॹ\n�\n\"π�<\"	ᐿ✅ᐿaπa,\"✅\r\n	\r\0ॹπᐿ,<a✅;�\r\"<\n\\✅\n�\0ᐿ\r\"\0\";π\n\"\r\"	a\"	\n;\"\n\\\0\\ॹॹ\r<<\rπ\r\n\n\n�<,�aa;,\"\r\n<✅�ॹ\";\r�\0\nᐿ\n<✅ππᐿ✅✅�;ॹa✅\r\0�	\rᐿ\0π\n✅π�\"\"\rᐿᐿ,\n✅✅\n\r✅\nπ\n\r\r✅✅,\n,\\\\a✅ᐿ\"\rॹ\0\n\r\"\"<\"�\r\r\nπ\r\0\nπaᐿ;π\"✅\r;\\ᐿ	\"\\ᐿa�\\ᐿॹ\raᐿ\nπ\\\n,,,π	\n\n\0\n\0�\r✅;\0\n<;<\n,\n	π\\πa		�\"\n�✅<π	\0	\n\r�\0π✅\\ᐿ<\r<\nॹ�\n\0\0π\r\n\n,\r\n,<	\n\n\n\r;✅\n,✅\nᐿ\0\\��ॹ	\nॹ,\rॹ;\"\0�,πa\r;\\\\\n<\\π\n\n\"\r	a\0,✅��\nᐿ\\\r\naπ�	,,\"\\�✅π\\;✅	�ॹᐿ✅\n\r\n\0✅\0\n\"\n\\\"\"�\0	�\n<�\\�\"�✅\"\\	\n✅\0\n�,	✅aᐿ�,a\"\r<<\0\\\\π\"		\n\n�\r\r,ᐿa\"\\a,;aa\\\nπᐿ\\\\,π;,;	\naπ,\n✅<<a\raᐿ\rॹ✅\n\rπa\r\\\\\nᐿ	,✅✅ᐿ;\n✅	;\n\\\n\"\ra,<π\nπ\0<<	;\0aa\\\0;,ᐿ\0<\"<\"ॹ\\;<ᐿ\na		✅a\nॹ	\n�\n✅,	\nπॹπ�\n\n	\0\n\nᐿ,a\"\"✅�\n<\n	�\r;\\ᐿॹ	\0	πa\r\n\n\"\n��\\ᐿ\nॹᐿ\n\\ॹ\0	;\n<\n;;,\",ॹ✅;\r\ra	ॹ\r,\\\n	ᐿᐿ✅π\\\\\\�;	�;\nπ<\0<πॹ\";\nॹᐿ\"	ॹ;<✅✅\0\"ॹ<ॹ;✅\\\0π\\aॹ✅	\r�;\n	ॹॹ\"\ra<<,π	π\n\nπ;\\\n	�πa\n✅	<\n<\r\\ᐿ\n\r,✅\n✅\\✅π\n\\<\\πᐿ,,	\r<\\\\\r\n\\\n�a\rॹ\nॹ;ᐿ\\ᐿ<,\"✅\"\nππ\0<,	<\\\0ॹᐿॹ✅\n,,;✅	\nᐿ	\n	aॹ<✅\0✅a\nॹ\n��;\n\n\\π\n\0��✅,\n�\"ॹπ	<\n;ᐿ\0\r�\\,\n\n\\	\n	\\✅\0\"	\r;ॹπ\n;;,<π	ᐿ\"\rॹ;aᐿ\"ᐿππ\\a�\rॹॹ\ra\0\nπᐿ\0<\n\0\0\"a	�✅<;aॹ�π\n\0\r\n,\"\"✅✅ॹ\\\\\nॹ,\";��aॹa\\ॹ\n\n;✅<\ra\";a✅\r<<	,\r\"π,\r✅�<\\π;π\";✅�\r\r<π�a✅\\,;π\rπ�\n\ra,\n\"\n\r�\na\r,;<✅\0ॹaᐿ✅a\n<	\n\\\na	ॹ\n\r;,\"π\\\"\rॹ✅\\ॹ	ॹa\nᐿ;\0\n	\n,\r\"a<\n		�ᐿ\n\\	,a\naπᐿॹ\"π\"�,\rπ,aॹ\0π;;ᐿॹ;\r\nॹ;,\\π\n✅\\aॹᐿ\r<\0\n,\n',_binary '��\��/��A\��[s\�nO�_\�b�\�\�*�\�J-u�\r�H��\�\r9FQ�ԡx�.\�\�\�QUxu\�N\�\�\�\�\�k���L\�_\�DƱ�;��;��z��ŲR\�B�2󨮷��V\�Y��\r\�\�w\�^z��\�\'_g\�\�B\�<\�T\�\�\�־\�N��^�Rj�JW�˞\�ԦSv�m)�a\�OZ��\��xƪ\���� \� \�\�lꄶ�^`{\�cqo�\�\�\�u\\?\0'),(22,'<\\	\0�\\\n✅<ॹ\nॹ\nπ,;,ᐿ\"�✅\\✅\\<	;	π<\0,ᐿॹ\nπ\"✅\n�\n\0	π\r<\r✅<\n\"�✅;ॹ✅\0✅,,\nᐿ	\n,\"<	,ॹᐿ;<\0ॹ<π\n\\ᐿaπ✅�\r	;,✅<\rᐿπॹ�\n\rॹᐿ\0\\\"<\\ᐿ\0\\	\na�	,\n\na�\nπ\0\n	<\n\n;�;π\n\n<�ππᐿ<ᐿa�aᐿᐿ<		π;�\0�\r✅\"�a�\r	<π\\\nॹaa\"�π	a\\\"�✅ᐿ	<π<\"a,�aᐿ\n,�ॹ\n,\\ॹ\0;;\r\"π		\"\n\\ॹ\n\"<π	\n�ॹॹ\n	�ॹॹ✅\"ॹ\\�<\ra\r\na\"\\\0\0\\\rᐿ\\\raπa	\0ᐿ\\\"ॹ\ra	\nᐿ,\\<;π<a;<;<��ॹᐿ,\0	a	\\\r\n<\nπ✅ᐿ,\n\n,π\0\",✅ᐿ<,\n<a\nᐿ\\\"\n,;\n\\π<\r✅	<ॹππ✅�\\a	✅\\\n\\π\n�\"\nॹ\0\r�;\"a<✅ᐿ\"π�π✅\n\\\r<a	✅,ᐿπ\n,\n\\π\r\0πᐿ\0\r\nॹ;ᐿ\nॹπॹᐿ\r<	\n\n�<\"ॹ\\ᐿ\0\n✅\r;,ॹ\0\0✅ॹ\0�\"\"\0✅ᐿॹ✅��\"ॹᐿπ	\0ᐿ\n;\r\nᐿ,π\0\nॹॹॹ�	\rᐿ�π;,ᐿπ;,\\;aa\r;\n\\\nπ\0ᐿ	✅ᐿ<\\\r;\"<\n<a�\\\n,aππ	\",π\0\n;\\ॹπ�ᐿ\\ᐿॹ\rॹ	aa	<ᐿ\nॹ\"πॹ,ॹ\0✅\r	\0,\\\n\0\ra	ॹ<;	π;	\nπ\\<π\"\0\n,;aᐿ;✅\\✅aॹ	\r\0;\r,\"\n\r�π✅\0	�\"\"\0\0;\"ᐿ\n\r✅π\r\nᐿ\n\n\r\"✅	π,,✅✅ॹ\\\r✅ᐿ;πᐿ;✅\nᐿa�<;a\";\n\0ॹ\0✅\n\"\n\r\r<<\n\ra\"ᐿπ<	\n\r�\n✅\n\"πॹ\n\n✅\\	\\π;ᐿ\n,✅\n;�\nᐿ\0\r;,aᐿ	\na<\n<ॹॹ\\;,π\\\0,π�<��\n\n,;<	\n;\n\0<;<aπ\\\"\0a\n\n\0a;\"\0\0�\0\",\0✅\r\\;\\<a�,ᐿ		\"\n\\		\n\0ॹa\r	�<\r	,;ᐿ	a\"ॹ<;π<\"πa\"\\\r\0\n\0π,�,\"\n<	\rॹ	\\,\n\"a✅<\"\"\"���;ᐿ\0ॹ\\,\"\"πaπaᐿ;\";	\n\0\"	\n✅\n\0✅;	\\	ᐿ\0�\n�✅\"	\r\n	a�,<\0<ᐿ\n✅<	\\	\";	\\\r;,\nॹa\0ॹ��a\n\n��ᐿ✅	✅✅,ॹᐿ;	\"\r<\\;;�ॹ,\"�,\0\n\n,\n�,	\n\n\0✅;a<\\\n\r;\r\n\n\naπ;\\ॹॹ�ᐿa\0a;	\0\nπᐿॹ\n<\r\\\n;;	\n\nπ\0	\0,\n✅<<<\\<πᐿ✅\\\nπ<\n\n\n\\ᐿॹ;\"✅a<<\n;\r;ॹ\n\r\n\rॹ\\\"\0ॹ	,ॹπ\r\nπ\r\nᐿ;\\,\r�;\"π\n\n\\�;;	\\�ॹaa\n\n\n\n\\✅✅\rᐿ',_binary 'R��!�eO?_�br�f\�M|M{�\�\�\�I��Z\�h\r�\�\�\0y9\�f�\�\�\"�\��)9Hi�\��G�]�\�\�|\�\'F镯Z%6yQ���l\�qă\�_���|X!�\�U&�\Z�hN|�v:Iԕ\\��!c%%?\�s�ש\��!�D��\�1?j��h\�\��u�f�[\�,ĄE�\�W+\�h\�\�\�/PT\�Ѓk�Lqt\�tv'),(23,'\n,π\n�\0aॹॹ✅	\\\n	π✅ॹ	<\n,✅\nॹ\n,\\�;,�ᐿ\\\n;✅\"ᐿ�πᐿπᐿ\\�;✅πᐿॹ�\"ᐿ	\0,\n�;aπᐿ�\"	�π	<ᐿ;a\n\n	ॹ<\n	\na\n;\\<	\\\nπॹa\\\r	π\n\r;\n<✅\nᐿa\r�aπᐿ<<,\nπ\0\\\"\\\0�\n<✅ॹ\"\"a�<	\n\n✅;;	ॹ	\0\0ॹ\rॹॹ	\n<\n\nᐿ✅;\\,\\\0�ॹπ	\"<π�\na\0�;�\"ॹᐿ	;\r✅\n\r\\✅,ॹ\"a��;ॹॹ	\\	\\\r✅\"\n<aπ;ᐿ\\aππ\rπ\nॹ\n\0;ॹ\rᐿ\nॹ�<✅πaa\nᐿ\"\\ॹ;;ॹ\r\n;	aa\"a<ॹ\n\n;\0	;\"π;	\n<\"a✅\n;\r�\n\"\naॹॹ\0\\\0<aॹ\0;\n�\n	π\\a\"a;\0ॹa\n�aa\nπ\r\na	<\0<\rᐿ<	\"a✅\"�a\\\n\\π\"<✅�\n\n;\",���\n,\nॹ✅ॹ	ᐿᐿ\nᐿ<	,\nॹ<\r\n\n	\"a,ॹ\r	\n\r\"\\\rॹ✅,a\\\n,\n\n;ππ<\naॹ✅<\n\r,\n<\0\0;a\0,;<\"ॹ\0ᐿॹ\n;\n��ॹ\rᐿ\0ᐿ�\"�\n	✅;;\r;;,ᐿᐿ\nॹ�π\r	✅ᐿ\\a\\;\rᐿ\"ᐿ\r✅π\"�	ᐿ\\<,✅\\a<\n\"	\"�<;�;a\r\\\r\\ॹ\\\n\n,\"a\\\0,ᐿπ�✅ᐿᐿa,a	�;ॹ\"π\0\\π<\n\n✅ᐿॹ\n<a✅,	ॹ\r\"\n\n\\<a\rᐿ\"\"��\n,\0π		ॹ�\n;;✅,\0\0a\0✅✅	✅<aॹ,,<ॹ\n;a\"\n\0;;ᐿॹ<\"\n�ॹᐿॹ\0;✅✅\nπᐿ✅\r\0	<	\"\\\0ᐿ\n\nॹᐿ,ππॹ✅<	π,<�;ᐿ✅\r✅\n\n✅\nॹ\n\n\0;	✅ॹ\n	a✅\r�\0✅\",ॹᐿ\n\\,\n\r\n\n;✅,✅\\,\"π✅✅\"π�✅\r\r�<	\n\0	\n<\r✅\0\\�\"ᐿ\"π,ᐿ\0	\0;\\\"ᐿπ\\ॹॹ\nᐿ\na	aa\\✅ᐿ\rॹ,	\\\r	�✅<<ᐿa<�ॹ\n,✅\\ᐿ\0\"✅;\"a\nॹॹᐿ<\\�\0\n;✅,,\0ᐿπ\n\\\"\r\\ॹᐿπ;\0,\"ॹᐿॹ\rᐿ\0aॹ\r\r\0<\\ॹ<✅,,<�\r\\<\n\\\ra	\n�<\n✅\"	;\",π\"\naॹ\n,ॹ	\n,\n\n\0\0\\\r\\\\\0\r<,	ᐿ\n;\r\0<ᐿ✅;π,ᐿ\r\"a	\nᐿπ;ॹᐿ;\\\r;\"ॹ\n\n\n,✅,\0<✅\n�\n<\0\n	a\n;<a	πᐿ\nᐿ\0\na;\0✅,�	\r;aॹ\nπππ\\\n	\"\n	ॹ✅\rॹ\\a\0�<,\0πa;ᐿa\n\\,�;✅\\\n;π\\\n✅\n\n;ॹ\"\",	<\0\n\n\n;,;\0\"\nπ�✅ᐿ,ᐿ\rॹa\n\"\r\"�\";ॹ;ॹ;✅π\\,ॹ\"✅\"\rॹ\0ॹ\ra�π\0ᐿॹ,\0<,\n\0\naॹᐿ,\nπ\0;,	;\r\r✅\"✅\n\ra<a\n;	;',_binary '/��\�\��io1D��L\�V\�\�g\�(��j\�ئ:\�\�hk�\� ��\�e�p�=�kf��ЄCc�	\�jw>!�#j7�(>�\'6n\�T7��@Cr]^�\�;�/˳�\�@ڬb%B<��ݠ�9�x���\�G\�\Z\�|\�\0\�*3\�H�֙3@�%�\��vo\�Ffh\�\�-rz+I\�F���\�^O��\�Q�(ŀfҪ\�ZN\�\�\�b�Z�_�G\�e�\�\�z�i\��'),(24,'a\n�ᐿ\n\n	;ॹ\r✅ॹ	\\a\\π\n\0✅ᐿᐿ�;	ॹ✅\r\r\n		ॹॹᐿa<\0\0ॹ\0<ॹaπ<\nॹπaᐿπ�π\r\nπ	\r\n	ॹ\\\n<,\r✅;\",ᐿ;\n\";,ᐿ\naa\"ᐿ\0<<ॹa,\0✅ॹ<aaॹ\n\r<,a\"\0�a\\\";<��ᐿ\"<;π\r\r<,✅	✅✅\nπ\n\0,\n\0✅\0ॹ\"a	<\\π\n�π,	\\\n\0✅;\\	✅a\n,�\0;✅<a\"\r\0a<\"\n\r	<\0\\ᐿᐿπ,\\	,\\\nπ;ᐿ\ra<�\"ᐿ	\n\nπ✅ॹ✅�,✅\0✅\rπ	πॹᐿπ✅�,\"�\n<\\✅✅\n\r\n\na�π\\�\n	\r<\n	,\0�ॹ\n\\,<�	�\r\0\\✅\0\"\\	ᐿ\0ॹ\nॹ\n	,\r\0<\0\"\n✅✅✅ᐿ\"ॹᐿ;\\\"�πa\0\r\na\nᐿa	,\"\n;,<a\n;\r,\nॹ�\"ॹ\\;✅,a✅\\ॹॹ�;	\"\"�\r\n\rᐿ\\✅✅�ᐿaॹ\0�\n	✅π,\\\nॹ\n,ᐿ<\r���,\0\\,\0\0\n\"ᐿ✅ᐿ	;\0\"ᐿ\r,\r;\nπ✅\0\0✅<\"�\"\0;	ᐿ,\r\0\\ᐿ;\"ᐿ<	\n\n\r,a\"✅aπ\"\"\"\"�ᐿᐿ\n;,\n\r\n;\"\"ᐿ\rॹ\"ॹ\0\0,ॹ,✅\"ॹॹ,\n\\a\"aॹ✅ॹ\n	\"\\ॹ,,;ᐿ\n<\"\ra\n	π\nπ<\r\\,\"\nᐿ\"\rπ\r\"\\	\\\0a<�\"	\rππॹ;�	π;aᐿ;ॹ�ॹ\n<ॹᐿ\nᐿॹ�\0\n,π\"ᐿ<a;\\\\\"	\n\0\0,<�ᐿ\r\nॹ�ᐿ\n;,\rᐿπ✅\\\rॹ\n✅\"ॹ,✅✅	\\\"�\"ᐿ�	ππ\0	ॹ✅<✅;\r✅��a\0\nπ\\ᐿ	ᐿ	\0ॹᐿ;,\\\n\\\n\\	;aॹ\n,;;✅✅\\ᐿ;\n✅�\r\n\n�ॹπ✅	�\nπᐿ�✅	\"ᐿ\"a\"ᐿᐿ;ॹπ✅;\\<<<�<ॹ\n<\"a\\\n,\\\\	\ra\"\\✅\0	ᐿ,ᐿ;;πa\\,ᐿ\rॹ\"\"✅\0�	<a✅\n✅	\n\r\n\na�a\\\n\r<��\0\r,ᐿ	✅\"✅πᐿ,<\r,\\<,✅\0\"a,ᐿ�✅<,,;π\\\0\"	ᐿ<�	\0\n<\r\n\nπ;�ᐿ	\0✅<,ᐿπa\0\"\r,a\"<,a\n�\\\0a\0ॹ;\na\0\\�\n\n,ॹ\\aπ\"<,<		\n✅π\\✅	π;,\"π\n,ᐿ�,\\\\�;	\0�\n\\\n,\\ᐿॹ	\0\";\nॹ,	\n✅,\0\n,	;ॹ;<\0ᐿ	\n�a\n;\\<,,�\r\\\n\\\"\0,ॹ<aπa\nᐿᐿπᐿ✅ᐿ✅π\\�ॹ;\n\0✅	ᐿᐿ\n�\r\0ॹ;<	π\\<✅✅a\"�;�a�\\\r;\r✅\0ॹ	\r\"✅ᐿ,π\0\n\n;\r\r\n\r\rᐿᐿ\"ॹ,\r;a\nᐿ✅;	�aπᐿ\nॹ<\0a,\r\"✅\nπ\\\nπ\n\0;ᐿ\n;��,a	✅,π\n�\\ॹॹaπaॹ✅\"<ᐿ	a�;ᐿ\\\"\n\\\n\nππa\n<	\r\0π\\ॹ\n�',_binary '��\�+`d(��1�g��\�1�\�)��\�@!�PRCK\�\�!K_\�	�+�\nR\"������I]\��\�}u�{�\�X`�+�\��6G\"\�\�w\�c\�\�M؊\�VU堔\�\�\��\'t���\�\�.\��y\�\�\���㩚\�\�b��ô�9=r\�S|�u�VP\�\�\�,�\n\�xA�K˽��z\�\�\�\�f��Ϭڳ9Q�\�\��F\�\�E;[F� Ľ\"\�\�\�\�\�Bo�C'),(25,'�		\"a�\0\\;\nᐿ\\✅ॹ\"ॹ\\\naᐿ\\\n\n	\n✅ᐿ�ॹ✅\"\"\"ᐿ�ᐿᐿ;\\<	\n;\"\n;,�ॹᐿ✅�\"\0\"ᐿ<ᐿ,✅ᐿ\n\nπॹॹ✅\n\r✅\r\0�ॹa	a	\naaa\r;	<;	\\\n,\"\n\\π	<\n<;\"ॹ\n\0\nπ\n\"\"πᐿπ\0;�	\na	\n\\ππ�\r\\�,a\n;\n\\\"\n,�;\"ᐿ�\"✅ॹ<\na\"<,\"\n\r\0\nॹ;\n\nπa\nॹπa�\n\r\\ॹ\0<π\r\\\r✅	;			\\a\n;✅\0\"\"aa;\n\n\0\n✅✅\\\\ᐿ\r\0\0a,\na<ᐿa\0<\0�\n\n	\0<\0,\0,π✅	\"ᐿ;\\\n\nπa\\ᐿπ\na,;\0\n\n\"\n<\0π\naπ\nᐿॹ\0\r\r�\r	<,;ॹ;✅\0✅\0✅ᐿ\n✅\r;\nᐿ\0\r<π✅✅	ॹ\n<π\0;\n\n<\n,\n✅\0<\0\"\0\n;\\\"\ra�ॹ\\\"ᐿ�✅\0ᐿ,	,\r<\n\0\"aπ\n,✅<aॹ\nᐿ,<\n,\\<✅a\\a<\0π✅ᐿ✅;\\ॹ\"	\0		\r\0\n\r\r�\0\0π\r\r	\\✅✅\0�		\0\n<�\n\0π\\✅\0\\ᐿ,\"\0,<;\0	\\;,π\n\n\r	\n\0\ra\r\n,a\"\r✅<�;�,<✅ॹ\nᐿᐿ\"πa✅✅\r,\"�\r	\0ᐿᐿ	;π<\nπ�,;\na\n\n		\rᐿ\n\"\r;�\r\\\0\r\r\r\"aᐿ\r\\\"ॹ\\\r	\\\"�✅	\0\n	a;;aॹ\n<;\0�ᐿ	ᐿ	\n\"π,π\nπ\\\"\na\n\0,\rπ\rॹ\0<\"\nॹ\n;✅\\\rπ✅ॹ✅\0a<\r,,\"<,a�\n\\\r\n\0\r	\r�\\\n\"π\na	ॹ\n\"\0\rᐿ;<�\"a	<\nπ\\\0\"�ॹ\"π\n\"	\"\na	\\π\\ᐿ�;π\n;\n\n\n�;�\r\\\n\"\rॹ✅<✅<\\�ᐿ\n✅a\r�<<	a\n\"\"<<,\\<\0πॹπ✅		\0aॹ\nπa\n✅\nπa\\✅\n�\rπ<,,\n\n,	�;�✅;;	\naᐿ\0\"	π\0ᐿ,<\n,<\\�;✅<\r;\r\0\\,ॹ\r\\<\n\nॹ<,ᐿa✅,ॹ�	\n\nॹ\0<\0\n�\0ॹ;\0ॹ\nᐿॹ\\;\n<	\"\"π<ᐿ\r	π,�\n�\n\0✅\\\0a✅✅\n�,ॹππ,ᐿ\naaa\n;π\n\\ॹ\0,✅�;	a\\,\r\nπ		\0\\<\";\n�	\",✅�\n\r�aaᐿ\n�<\n<✅�,\0\r,\n<ᐿ\\	\\\";a;\naaᐿ\"\\\"<�	aॹa�\r\rπ\r;\n		\r;a\r\r✅ᐿ<ॹ�\",✅\0�\0π\n<<\\\na\n\n	\n\0πॹ	ᐿ✅\r✅\0\\ᐿ;;�	\r✅\r<;π✅,\"<ᐿ<;a\n\0a\n\r<ᐿaᐿᐿ	\0ᐿ\na✅a\0\"πॹ✅	�,\n;ᐿ	ॹ\\\r\rπ	ॹᐿᐿॹ	,\0\n<	\n✅✅\n\r✅ॹ\r\\\\\n,,✅\\a�	\nᐿ\n✅\n;a;✅	\rॹ\n<\0✅\0\\\0\0\"\n✅\r\n;\r\0a\n',_binary '\�}�\�&�\0ʁ\��\�>��u.�\�\�\�O-��A���\�\�wh�\nɥB��t*,\�H=\�j:	?�\�\�_�\�7�7���^�\�]��\��dB0���bC�\��#ud��*\�Y�z��D\�b\�@�U3�\�\�|Sq2�����x\�;\\<O�l�;�Ze$M�my4_\�\�ߝ\�=�\�\�� �\�\\\�	��U�rJdp=����=\�\�^\�%?�\�'),(26,'<ॹᐿ\\ᐿaπ✅✅�;\n\n\n	π;<,\0a<\\a	,✅ᐿ\"✅πᐿ,ᐿ\n\\ॹॹ✅\nॹ���ॹ✅a;\";	✅\\\n✅	ᐿᐿ✅\\ᐿ\n\rπ\0\\\"ॹa\0	✅\"ᐿ\0π\nπ\nᐿa\rᐿ<	ᐿ\n\r,\nᐿ\0✅ᐿa✅\"<\nᐿ✅\\,\r\\\0<�ॹ;ᐿ\"\nॹ\\ॹ,;,<\"<πᐿᐿ\0\na\\	aa\r�✅ππ\n<<,\0	,<\"\0\r\"<�	✅✅\n✅\n✅\n,a��\nπ;\"\r\n\"ᐿaॹ,;ᐿπᐿ✅ᐿ✅✅�\\,a;✅\n\"ॹπ\"\"✅\nᐿ\\\0\"a<,ॹ<ᐿ;,\0,π\r\r✅\"✅,\"a\"✅ᐿ�\\\n;πॹπ	,\0\n\"✅\0ॹ	;ᐿ\n,\r	<\0✅	\nॹ✅ॹॹॹa\r	�\"ॹ\\\n;π\0π,✅πॹ✅\r✅�π	\0	\r\n\0	;✅<\\π,\nॹॹ�ॹ\0\"\r\"a\\�\0\n,\n�<✅\r\\;;��	ॹ<\nπ✅ᐿ\rॹ\\	\"πॹ<✅\"ᐿ,\n\na\0\nπ\nॹ\nॹ	ॹ\naπ\\π<ᐿ\n;\\;ᐿ	,✅<\n,a;\n\\\n\r�ॹ	\\✅<�a\n<ᐿॹ\r�\0\"<ॹ\\\"\n\\	ᐿa;aπ✅π\r\",;;✅\"	a\r<ᐿ✅<\0;ᐿ,\n\\\0\n\"a\r;<π\n<\",,a	\n\0<\0\rπ,\\π�;π�\\<\n\nπ\r\"a\\�\n\r\n\\✅,✅\\ॹ,\"a\rπ\n\r<\"\0<a;π\n\n\0�\\�\nॹ\\\\ᐿ;ॹ	\\\\\\�✅\r;<	\n\0✅ॹॹ✅�\r,✅\n\n\na✅\0aᐿ;;a\";π;\nॹ		,πa;\r✅;\r\\✅\nπᐿ\0a�\0\n<\"\n�\"✅a�\0\\�\0,\\ᐿ\n\n\n	\0,\nπ✅\n;�\rπ\n\ra\\\\\0ᐿ\\a�	\n\n\\;<\ra\\�✅\0;π<\n,\",	✅a�πᐿ\r<;\n,ॹॹ;\n;�✅\\;;\rᐿ,\nπ\n;\"\0\n,\\\"\0,\\,\n,ॹ�\n<\0\r<;�\\a\r\0<\0ᐿ<\n✅\rᐿ\r�\\\n✅<ᐿ;\\\n;\\\\\n✅\r;<aᐿ✅π\n,a\r<✅ॹ\r\\\0\r\\ॹ<	\"a\"ॹᐿॹaᐿॹ✅\n\n	\";\\\n✅ॹ<<	π<\n\"aa\0\"ᐿᐿᐿ\"\0aa\\ॹ,�\0ᐿ,	\\\na	\n,ॹ�\0\rᐿ;	ᐿ\r\0✅\r\\�\\a\n\0ᐿ✅�<ॹ\0ॹa\"ॹ<\nπ\rπ	;,�,\0\n	ᐿ\"\\;<\\<✅\\\";ॹॹᐿॹ,π\\\r\n\\<\r\\aa<aa✅,ᐿᐿa;;\\;�\r\\ᐿᐿ\n\"�a,\0a\"\nॹ,✅\nπᐿ<✅	\nᐿॹ✅,\0\\\\\"\0,<✅\"\r\\\"\nπ\nπ\n	\"✅\0\n\",,✅ᐿॹ\"\n\n\rॹ\n<\"�\n\0\\<,a\nॹ<ᐿ�\nॹᐿ\n	<\"\n,\\ॹ✅\"\ra\r<ॹπ	\0\r\",<\nπ\n✅π<\0<\n\r\"\0�ॹ✅,\r�	\"ॹ�π;\"\0✅\\<πॹπॹॹ<	a	\n\n\"aπ✅\0\n��<\n\0a\nᐿॹ\n�\0\r\n,a\\	',_binary '�	\�*c\�\�S-�,\��\�Z\���\\ŕ\�Db��\�Q#\\\�(\�=�Z�Hա}\�I�0I��\�и\r#iH�F~\�<�\�Fb�\��0H5\�D\�:\�Ae\�\�<\�\���\�\�\�r������,\�􏉮D\� \��{\�\�ZQ�M���\�\�\�|s{|�r�\�WĢ�\��\�п\�\���w3�@7ꇘN%\n\�@jq��&y\�K0ȼ_s�\�6\�\�}$dd/�\�\�.U�\�r'),(27,'\nπa<ᐿॹ\\ॹ<π\n\"�<a\n\n;		π\n<\"ᐿ✅ॹॹ\n\n,<<\r;\";,ᐿ<\nᐿ\rπ\r�\n,�;,ॹπ\\	�\\\n<;\"\nॹ,,	<ᐿ\0a<	✅;\r;✅\"�πaπ\0ᐿ✅\r\\<\"ᐿ\r✅π\r\0\\�\naπ\rॹ�\\ॹ;π\\✅	<ᐿ,\0;\n\r✅\"ᐿ\n\r✅,\\ᐿ\n\0✅ᐿ\0\\a<\0\\aᐿ;\r;,\r✅\r✅	ᐿ;ππ,\r<\\\n;�\",π;	\n<�<π<\0\n\0\n\n✅\r\r,\\✅ᐿ\nᐿ\0\"a\"<\nπ✅\0\";\"\r,;\nπaaᐿaॹ��,�\0�ॹπ\nᐿ\n\\\0�\r<a\0\n\0,\"�✅✅ॹπ\nᐿ;,�<ॹ\r\n�\"\na	<,	\0\"<✅,;✅;<✅;,;\\\nᐿ\n✅✅\r\\✅ᐿ<\\\n\n\0�\n\n\n	ॹ\0\"\\✅\r	ॹ\\\n<πॹ\r\n\"π;\0\\ᐿ�\0	,a	;	;<\nπ\\a\0ॹ\0✅		\0\n\"	\\\rπ\n\0\"\\\\<��\"�✅	ॹॹπ\n\r<\"\n\",ᐿ\naॹ\",\n<\\✅;\n<✅\\;\"ॹ✅\r,ᐿπ\n<\n�\0\r	ॹ	\n,✅\";a\n\"\"�,<\"\0ᐿ<\"\n<\n;\r\n,✅;a<\\\nᐿ\"\0\\\n\\,,,�;ॹ\n\n\n\0π<ॹॹ✅\n\\aᐿ\"a\r<\\	\ra\n�;ॹaπᐿॹ\r\nᐿ\0✅	\\ॹ\r	\n✅π�	ᐿ	✅ᐿ<aπ	,\n\n;;;✅�	,,ᐿ,✅,\\a,;\"	ᐿ	π<a\"✅\\;<\nᐿ\n\0,	π	ππaᐿ\rॹ\n\n	ᐿ✅,,π	✅�πππ,ᐿ,,✅�\r\\ॹπ;\n,\r✅,π\"<;,�\0�\\\0<\\a<\\aa\";;�<\rᐿa\\<✅\r\n\nπ\rπ\nπ\"\n✅<,a�\"π,\r,�\"\"\0\0✅	\n�\r\n\n�\r\"<\n\r\\�\"\\;\"\r\0\0✅\0<πa,<,✅π;\n	\\\nππ;\\\rπॹ\n\\π,\0ॹ✅\ra\"✅π�\n;\na<\"\n�\n�\nπ,\n	ᐿ\0\"�,✅\0\"\nᐿ�\r\"\\\n✅π	ᐿ	<\r✅<a✅,ᐿ	aπ\"\\\0\\\0\0aॹ�\\\"ᐿ;,\"πᐿ\naॹ\n;;<\0;<\\,<π\n\0ᐿᐿ\rπ\"	\"a\nπ\r\"a\n\n\",ᐿ\0✅π\"✅<✅π\"π<�ᐿa\\ᐿ\0<\"<ॹa\n\"aaॹ\r\r\r\rᐿ;,ᐿ✅<\0\"\nπ<✅ॹπ\r✅\0\0	,\n\n\n\nᐿ;\\π\\,\0\r\n\",;\n	\n\\\0�\"\"\\;\nπ,,;,,✅�\\\"\r,ॹ\\aa✅\\	�ॹ,\\\0ᐿ,;a\"\";π\r\n\nॹ✅\0π\\✅aॹᐿ\n\n;✅;\n\n\\<\0\0✅\0ॹᐿ✅	\n\\<\n✅�\0\"\n✅<πॹ\"\n\rπ\n,,ॹ✅\\\nॹ;	ᐿ\\\\\n\nॹ✅\n�\\ॹॹ<\\ॹ✅✅�\n,\0\"\0�\r	ॹ\0<<\na<\0;a,			aa;�\"�aᐿ✅\"<ॹ\\\0�\";ॹᐿ\"\r\n	\0<<<	\r\\\"<,;',_binary '�\�\\\�\�\�nP\�\�\�\"B1K�l+�/l���D��J\�͠\�̃\�\�?ET蛂\0\�\Z�Zu�\�w^��H�\����{�\"\�Q���\�Ëb��-vmO\��\�\�0�u�{B\�mp���?��e\�ؿ2LN��\�$I\�\�R3�\�XA\�\�`Pm:\�e���i��\�Ӹ�2�y\�\n\�]L)\�\�`x�|\��\Z(/* 3`J\�ٰ\�AdN!-\�'),(28,'�\"\\	\0,\\\"ॹ		\na✅,ॹ	\0;\\\\\nπ\nπ\\\nᐿ✅a�;ᐿ<✅ॹ		;π	\0\r\\;✅<\\\"✅\n,\n\\\r\0ॹ	\"ᐿ\0π,,;,,�a\"✅\\ᐿ;\n\\\"\r\0π�\n\n\"✅ॹπ\rॹ✅;aᐿ\0;\r\"a\\\"\0\\,;<\\\na\\,�✅\0<<a\r\n;\rᐿ,π\0ॹ\"\\\nॹ\\✅,\n;πa\0\r�πᐿ✅a�<,,	ᐿ\0\rπ\0\\;\nᐿ\r�\\ॹπ\n\\,a�\0\n\n<✅	✅;ᐿ\r✅;\\	ॹ<;,ᐿ✅π✅ᐿ\"\\��a�\rॹ;a\0	\0,ॹ\"✅π;	\n\0\r<<π<\n	a	<<a\n\0�,	π,a\n�;ॹπ\n\n\0,\\π<�\0✅\0<	;a\r<\\;\n;ॹ<<\0✅ॹ✅\0π<<	✅	π\0\\;a\\πॹ�	\naa\ra\\<\n	\n\"✅	\r\";\\;\0✅✅\"aa�ॹπ\\\r;<\n\naॹॹॹ✅\n\0�ॹ\r<πa�\\\"\n�\r<\n,\0a�,�;,\\,\"aπ\n<ॹ✅�;\r�\nᐿ	<aπॹ\ra\"✅	<�\\\n\n\"�;�;ॹ,a<\n✅	,;	\n\0\0,π<<,ᐿ,\"✅<�✅\n\n\\π\nॹ\n,\0	\\;\\<\nᐿ\"\"\r\n\r\n\r�\n;,<\"�<,\n\r\"	\n\\ॹॹπa\"✅,\"πॹ,�\n✅,a\",�\0<\rᐿॹ\"\"	\n✅\0π\r;\0\\	;π\nᐿ<,\n,�aᐿa\0�ᐿ	π✅,ᐿ�ᐿaᐿa;\\a\n;\nπ\",;\\a\n\n�\0<<\"π\n,a\\ॹᐿ	ᐿॹᐿ,;�π\0ᐿ\n		ॹa✅ᐿᐿॹᐿ,,ᐿ\\\n;\"<\n\n\0;\0	\n�;\\\n\r\\<\rॹॹ;ᐿ\\ᐿᐿॹπ✅✅π;ᐿ\\ᐿ\n�	a;\"\0ॹ\0\",\n,<π;;;\0<;✅,\0\n	\"ᐿ\0ॹ�\\πॹ✅a	\0\"<✅πᐿ	ᐿπᐿ,ᐿ\\;\na\n\"<ॹ\"\r\nπ	\\\n\rπ\r\0\0\0\n�\rᐿ<✅,<✅\n;<\n\"<ॹ<�ॹ\\\n,\nᐿ,;✅<\n\\,	\0\0\0ᐿ\0\"\0	,\"π,		<π\",✅a<\0ᐿ�\\\na✅πa;\0\";✅\"\\;\nᐿ\n\n	\"ॹ\\\0	a<\r\nॹ\\\n✅;�\\\r<✅ᐿ\r\r\rᐿ\nॹ\0\raॹ✅\rᐿॹ<,\r,,ᐿaπ\n,ॹπ\\π,	\0\n,\0✅π\"aᐿ\r;ᐿ,;	\\\"\nᐿ<\"πa\"\n<\\ᐿ\na\n\n�	\rπ\\\"\\ᐿπ\0ᐿ\nπ\n�\n	\r;�a\r,;<\n<\n\n	✅✅\"\"�\rᐿ✅,\\;,\nᐿ\0\r\na\"\nᐿ\n\n✅\\\nॹ	ᐿᐿ;✅π✅\r,<π\";ॹ\r\"<<ॹa\0π\n;,a<✅a;a,✅,\nπॹ	✅\n,���\n,\rᐿ✅ॹ	,a\"\"\n\r\0✅\n<aᐿ\rᐿ	✅	✅	\"\ra	\nπ�ᐿ✅<\r;ᐿॹॹ;\"\n\"\\�\0ᐿ✅<\n\"\r\r✅\"ᐿॹ;a\r�<�ॹ�✅π�ॹ✅,	\0,✅\"\n\nॹ\\a✅',_binary '\��MEB�\"�\��\�Km\Z�\�\�K���\nyn�D\�_\�t�\�\�<�\�a\�\0@\�Vb	8\\fݳ�r��ٜ�(<?�qf\�|ú&\�^/Qi\�//F��\�\0\�N\�\�L�\rWtwĲaZ{\�\�\n��`&\�V�\�5	.o\�2&h\�Mk\�$\"UN,�_,0S\0ݒ\�\�ղĔ\�P�q�ְ\�\�r<U/*�o�)\�\�]���x�O\�\�ρ\��\���;6>'),(29,'ᐿᐿᐿॹॹ	a	ᐿ,✅,π\r✅\\\"\"a✅\n<ॹ,ᐿ�\rॹᐿ\0	ॹ\"<�ॹ<\r\0,\r\rॹ✅πᐿ�	<✅ॹ	a\n\n\r\r<;\0\0	✅;<\r<ॹ\n<\0,\0\nॹ\n\naॹ✅�\n\0ππॹ,ॹ��\0	�\n\0a�ॹॹa;ᐿ✅;\0\0ॹ\nॹπᐿ\"\",\\\\<\\aπᐿ;π\\,�;�;✅a\"\n✅ॹa,	π\r;;ᐿ✅		✅\n\n�;	✅a��ॹ\"\\\\π\n\n✅π;	ᐿ\\✅π\",\0\n\\	,\n\nॹa;\r,�\na\r��<a	\0			,�ॹॹ\r<�\"	ॹ\\�;\nπ<;ॹ✅<\0;π\n\"\0ॹ\\a;<π\\ॹᐿ✅	π\0\"\r\\\\\\<\0a;ᐿ\n�π,;\n\0<\";\nॹ�ॹ	\raᐿ\na\r<\n✅✅	\",\0\"�a<a\n\"ॹॹ,a\\,\nॹॹ<\"a\r,�ᐿ\nπ\\\0\\\r,�\r<aπ	�\\✅ॹ\n�a,�ॹ,\n�π✅πaॹᐿ\0ॹ✅π\\\r�\r;ᐿ;\r\r\n\0π\\,	aॹॹ,\r;✅\n	ॹ;	a\n\\\"<\"\n	\\πॹπᐿ\\\rᐿ\n,a\";ॹ\rᐿ\r\n,	ॹ	\\✅,\\\\ᐿ\",\n\n,\r✅πᐿ\r\n\n\n\";\n✅;\\	\";✅\r\\�\n�\nॹπ\n\nπ\nᐿᐿ�,π\0ᐿॹa;a�a✅\r<ᐿ�\n\"π\nπ,	\n�<;\r\\;π\rᐿ;\n\\ॹ\"ॹ✅\\\nπππ\r\"✅ॹπ;;\0\n�a�\n\n\0<\nπ\0\"\\aπᐿ�✅\\	�a,\r\0ॹ�\r\0\rᐿa\0,\nᐿ\nॹ\n\0,�\nॹ\nππ✅\0�ᐿ✅\n\"ᐿ	\nπ\nॹ,\"	\"ᐿ\n\"\n\r;\\\"\n��\n;	π�\n\r\"\n\"\0a�;ᐿ,\n	\0\n\n;\\ॹπᐿa\na\"\r\\\0a��\0πa	;\nᐿπ	\"\\ॹ\"✅�π\\π\\�\\\n\nॹ\";ॹॹ\n	<\\\r\n<\\ᐿॹॹ;\n\n\n\r\n;✅ᐿ\"\0,	\n\n�ॹ	\r,<<ᐿ\\ᐿ\"�<\"�ᐿᐿa\0✅\"a\rᐿ\\ᐿ✅✅aᐿ\"\\�\0	ॹ;\nᐿ�\n,\\�ᐿ	\0a\r,\n<���ॹॹ\n\r✅π,�\0ᐿ\\ᐿπॹ,<;\\\\π\nॹᐿa�✅a	ᐿ;ᐿ\n	ᐿ\n	\\ᐿ\"\0<;\",,�\r\rॹ\0✅π\n;�ॹ\";\n\r\"�\"\r\\ᐿᐿ�<✅ᐿ✅\"\0\\\n✅ᐿπॹ\r\nॹ\n�<\"\nᐿ\n\"\n,�<\0\r\rπ<\r;<ॹᐿπ\n\"<ॹ\n\r\"\\;;\n✅,ᐿ	πॹ\nπ\na\r\n\r\r✅	\"aa\r\0a\r\"✅π\"ᐿ\r	\0<\\\r�\\\r\"ᐿ\n\n\r\n\"<�ᐿ	π<aॹ\\<,\n<;;\"π<π\nᐿ,\n	ॹ\na<ᐿπ\\<✅ᐿ	\r\0✅a\"✅,	�\n\nॹ\n\0;ॹπ\\;\"\n\n,\n\rπa\r;<a�;ᐿ\n;		\"<π�\\�<\\\nπ\\,\\\nॹ\\\n✅\n,\n\n;<✅\"ॹ�aॹ�<\0a\0\nπॹπ\n\n\n\\\nॹ,\"\n\0\0\n;�<��ᐿπ',_binary 'Py�-���O\���c<�ՏA�n`�T\���\�\ZlB��$�8B\Z�Wؔ�Y;\�\���E`=Q�x\���л�(�\�,(�{\�!\'ufE\�\�cX\�v\�4<{)��@O��]Z\�J�E%�\�Q�_G/\�!��S�X\�7:\��?,^9bj38��G\��:7\'vRt�NmpY�)v��\�\�\�uq\�[\�\�ؑ\�BW\0���\"�ڱ9᜽\��'),(30,'\0ᐿ;�ᐿ\na\0\\;ᐿ\n\\\na;\nॹ,<<\na<ππ\nॹ\\�ॹ\r\\\r\0\"\0π<<\n,\0\"\n<\0✅\\\n\n\n\nᐿ\r\"ᐿ<\r\\a	\"\0<ॹ\nᐿ�\nᐿ;a,	�ॹ\n	ॹ�\"<aπ\n�\0ॹॹ,a\nᐿ;\na✅\nॹ\r<\r\0\"\n;✅π;ᐿa,\n\nπॹ\\\nᐿ\0<\n\"\0\\,ᐿ<�ᐿ;\n\"\",ᐿ\"\0;aπॹ�<;\",✅\n\n,\\ᐿ\n\na,	�\nॹ\\a<�a\naa�\na<\"ᐿ		ॹ\r\"\na\n\\�,ᐿ;a\0\"�\"<\nॹ\n\r\\\\\\;✅,,\\ᐿ	\nॹ	\r\0ᐿ\\πॹπ\"	\r;<<✅		ᐿ,�,ᐿ\0	\na�ॹa�;ॹ\n,\r,\"\\	,\n��\n\n\\\\\"\n\n\"\r\n\0\"\rᐿ	ᐿ<a	\nπ<✅✅\\\n	\\,,π\n��,a\n;;�✅\r;\n	\nπaᐿ\n\na✅\r<aᐿa�<,\r✅	<,;✅\\�ॹa\\ॹ\n<ππ\n\"ᐿ<\\ॹ\n<✅ॹ\n\n\\ॹ\"<	,ᐿ\\;;✅ᐿ\";;\\\0,ॹॹ\"\n\\\0���✅;	✅;�π	\n\nॹ\nπᐿ�\n\na\nπ�ᐿ\\ॹ\\\0;	\0π�\0\0\nᐿ\n\rॹॹ\n,ᐿ\0	\n\"	✅\n;\rπ\nππ\\\0\0�a\na✅\"ᐿॹ�ᐿ\n<\\✅\"π;\n\n\n	\"<π	,<\",\r\0π,ᐿ\"\0	�	\nπ�ᐿ\nᐿ,ᐿ\0�;π\r\na�\"\\<ॹᐿ;a,	\0a,\0a���\0ॹ,;,ॹ✅		π\"	\\\0<\rᐿ;π\\\r	<\r,ॹ\0ᐿ\r\r\n\0aa✅\0ππ	,;ᐿ;\n	\n✅\\,✅�<a<�\n✅;;<✅\n✅\r\r\n\0\n,;\r;✅;�a\"ᐿ;ॹॹ\\	\"ᐿa<a✅;<a�	\"\n�\\\"a\nᐿ;✅;,✅\\<\n;π✅✅;ᐿ\"✅π✅	\"\0,\\	aπ\\✅\n�ॹa\n\n;✅\0\n\0\"π,	�\n\\\0�\\✅ᐿ<	<\0\n\\\0\r;	\r\\\0ᐿॹ�ॹ�✅\nᐿπ	a�\nπ\\ॹa\r<		<\0	\"ᐿa\n<�ᐿ\0\n✅ॹ\na\0\n\rπ\\,\0\nᐿ✅�,\\✅;a\"	\n;\nॹ\rᐿ�a	;aᐿ\n\\\"π✅\0ॹaॹॹ,<a,		ᐿ	\0\0\\π\na	\n\nπᐿa\r;\0,;;;\r�aॹᐿ,a�ॹaॹ<,\n\r\0aπ\r\0\r✅✅\\✅<;	\n�\\	\"\\\n\\\\ᐿॹ,ᐿ\0✅�<ॹπ;	\"\"	\"\n;\"�\\ॹ;	\0�;\0ॹ\\π\0<π\r\n\\�✅aaॹ;\n<;\n\0;<;�\r\n\\\rॹ\\\n\0✅\n\n;	<π\raa;\n\r�ᐿ\rπ�\n;\r;<\"ॹ✅<	\0\\<✅�,\nπ✅	ππ,\0\"\n\0\0\n\0\0;✅�\"	;	ᐿa\n	\n\nπ�\\a\"ॹaa�	\n\0\r\nॹ\0,<<\"ॹ\\��ॹ\\,\\ॹπ✅;πॹ\r�\\ᐿπ\n\0�\"�✅;�π\n<\n�\"\0ॹ<\0\n;	,�a<\\\nᐿ<ॹ\\',_binary '���]�wp/\�X)\�FO\�\�&�\�^\nO\�%u�}\�\�%�e�*k�Zvê����,6�6w\�/yx8�L\�\0\����٫\�~H_tp��m�\�\�#M� q(4\�\\\r�\�~l \�\�\���v�\�;F_@�Ɇ�\�[t\� ������\�5��\\!\�^�Q\�7HóK�)�\�\�}�\�GV\�\�|�\�ѧ\���\n^�\�!\�\�B\�	��\Z');
/*!40000 ALTER TABLE `simple` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `third`
--

DROP TABLE IF EXISTS `third`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
 SET character_set_client = utf8mb4 ;
CREATE TABLE `third` (
  `i` int(11) NOT NULL AUTO_INCREMENT,
  `a` int(11) DEFAULT NULL,
  `b` int(11) DEFAULT NULL,
  `C` int(11) DEFAULT NULL,
  PRIMARY KEY (`i`),
  KEY `a` (`a`,`b`),
  KEY `C` (`C`),
  CONSTRAINT `third_ibfk_1` FOREIGN KEY (`a`, `b`) REFERENCES `second` (`i`, `k`),
  CONSTRAINT `third_ibfk_2` FOREIGN KEY (`C`) REFERENCES `third` (`i`) ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `third`
--

LOCK TABLES `third` WRITE;
/*!40000 ALTER TABLE `third` DISABLE KEYS */;
/*!40000 ALTER TABLE `third` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2018-11-19 20:27:42
